"""Paginated GraphQL search iterator.

Yields pages of repository nodes and exposes rate-limit metadata via
a shared state dict that the spider can read for progress display.
"""

from __future__ import annotations

import asyncio
import itertools
from collections.abc import AsyncIterator
from typing import Any

from loguru import logger
from rich.console import Console

from github_scout.client.github_client import GitHubClient
from github_scout.config.graphql_queries import SEARCH_REPOS_QUERY
from github_scout.config.settings import Settings

__all__: list[str] = ["paginate_search"]

console = Console()

# Maximum consecutive page-level failures before aborting pagination.
_MAX_PAGE_FAILURES = 3
# Base delay (seconds) between retries after a page-level failure.
_PAGE_RETRY_DELAY = 30


async def paginate_search(
    client: GitHubClient,
    settings: Settings,
    query: str | None = None,
    rate_state: dict[str, Any] | None = None,
) -> AsyncIterator[list[dict[str, Any]]]:
    """Yield pages of repository nodes from GitHub's GraphQL search.

    Automatically respects ``settings.max_pages`` (when set) and stops when
    ``hasNextPage`` is ``false``.  If ``max_pages`` is ``None`` the iterator
    continues until all results have been fetched.

    If a page request fails even after the client-level retries, the
    paginator waits and retries the *same* cursor up to
    ``_MAX_PAGE_FAILURES`` consecutive times before giving up.

    Args:
        client: An authenticated ``GitHubClient`` instance.
        settings: Application settings.
        query: Override search query; falls back to
            ``settings.default_query``.
        rate_state: Optional shared dict updated with the latest
            rate-limit data from each response so the caller can
            display progress (keys: ``gql_remaining``, ``gql_limit``,
            ``gql_cost``, ``total_matches``).

    Yields:
        A list of raw repository node dicts per page.
    """
    search_query = query or settings.default_query
    cursor: str | None = None
    max_label = str(settings.max_pages) if settings.max_pages else "all"
    consecutive_failures = 0
    state = rate_state if rate_state is not None else {}

    for page_num in itertools.count(1):
        # Honour the page cap when set
        if settings.max_pages is not None and page_num > settings.max_pages:
            logger.info("Reached max_pages limit ({}). Stopping.", settings.max_pages)
            break

        variables: dict[str, Any] = {"q": search_query}
        if cursor is not None:
            variables["after"] = cursor

        try:
            response = await client.graphql(SEARCH_REPOS_QUERY, variables)
        except Exception as exc:
            consecutive_failures += 1
            wait = _PAGE_RETRY_DELAY * consecutive_failures
            console.print(
                f"  [bold red]>> Page {page_num} request failed[/] "
                f"({consecutive_failures}/{_MAX_PAGE_FAILURES}): {exc}\n"
                f"  Waiting {wait}s before retrying same cursor..."
            )
            logger.error(
                "Page {} request failed ({}/{}): {}. "
                "Waiting {}s before retrying same cursor...",
                page_num, consecutive_failures, _MAX_PAGE_FAILURES, exc, wait,
            )
            if consecutive_failures >= _MAX_PAGE_FAILURES:
                logger.error(
                    "Aborting pagination after {} consecutive page failures.",
                    _MAX_PAGE_FAILURES,
                )
                break
            await asyncio.sleep(wait)
            continue  # Retry the same page_num / cursor

        # Reset on success
        consecutive_failures = 0

        data = response.get("data", {})
        search_data = data.get("search", {})

        # Extract rate-limit info from GraphQL response
        rate_info = data.get("rateLimit", {})
        state["gql_remaining"] = rate_info.get("remaining")
        state["gql_limit"] = rate_info.get("limit")
        state["gql_cost"] = rate_info.get("cost")

        nodes: list[dict[str, Any]] = search_data.get("nodes", [])
        page_info = search_data.get("pageInfo", {})
        total_count = search_data.get("repositoryCount", 0)
        state["total_matches"] = total_count

        logger.info(
            "Page {}/{} - got {} repos (total matches: {}) "
            "[GQL quota: {}/{}]",
            page_num,
            max_label,
            len(nodes),
            total_count,
            rate_info.get("remaining", "?"),
            rate_info.get("limit", "?"),
        )

        if nodes:
            yield nodes

        if not page_info.get("hasNextPage", False):
            logger.info("No more pages. Stopping pagination.")
            break

        cursor = page_info.get("endCursor")

"""Paginated GraphQL search iterator."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from loguru import logger

from github_scout.client.github_client import GitHubClient
from github_scout.config.graphql_queries import SEARCH_REPOS_QUERY
from github_scout.config.settings import Settings

__all__: list[str] = ["paginate_search"]


async def paginate_search(
    client: GitHubClient,
    settings: Settings,
    query: str | None = None,
) -> AsyncIterator[list[dict[str, Any]]]:
    """Yield pages of repository nodes from GitHub's GraphQL search.

    Automatically respects ``settings.max_pages`` and stops when
    ``hasNextPage`` is ``false``.

    Args:
        client: An authenticated ``GitHubClient`` instance.
        settings: Application settings.
        query: Override search query; falls back to
            ``settings.default_query``.

    Yields:
        A list of raw repository node dicts per page.
    """
    search_query = query or settings.default_query
    cursor: str | None = None

    for page_num in range(1, settings.max_pages + 1):
        variables: dict[str, Any] = {"q": search_query}
        if cursor is not None:
            variables["after"] = cursor

        response = await client.graphql(SEARCH_REPOS_QUERY, variables)
        data = response.get("data", {})
        search_data = data.get("search", {})

        nodes: list[dict[str, Any]] = search_data.get("nodes", [])
        page_info = search_data.get("pageInfo", {})
        total_count = search_data.get("repositoryCount", 0)

        logger.info(
            "Page {}/{} — got {} repos (total matches: {})",
            page_num,
            settings.max_pages,
            len(nodes),
            total_count,
        )

        if nodes:
            yield nodes

        if not page_info.get("hasNextPage", False):
            logger.info("No more pages. Stopping pagination.")
            break

        cursor = page_info.get("endCursor")

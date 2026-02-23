"""Crawl orchestrator - ties paginator, enricher, and persistence together.

Provides a live Rich progress display showing:
  - Pages fetched / total estimated
  - Repos found, new, updated, errors
  - GraphQL & REST rate-limit quota remaining
  - Elapsed time
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from time import monotonic
from typing import Any

from loguru import logger
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from github_scout.client.github_client import GitHubClient
from github_scout.client.paginator import paginate_search
from github_scout.client.rate_limiter import last_rest_rate
from github_scout.config.settings import Settings
from github_scout.crawler.enricher import enrich_repos
from github_scout.database.connection import get_connection
from github_scout.database.dao import (
    insert_crawl_run,
    insert_snapshot,
    update_crawl_run,
    upsert_repository,
)
from github_scout.database.schema import create_tables
from github_scout.models.crawl_run import CrawlRunModel
from github_scout.models.repository import RepositoryModel

__all__: list[str] = ["run_crawl"]

console = Console()


def _format_elapsed(seconds: float) -> str:
    """Return a human-readable elapsed time string."""
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h {m:02d}m {s:02d}s"
    if m:
        return f"{m}m {s:02d}s"
    return f"{s}s"


def _build_status_table(
    *,
    page_num: int,
    total_matches: int,
    repos_found: int,
    repos_new: int,
    repos_updated: int,
    errors: int,
    gql_remaining: int | None,
    gql_limit: int | None,
    gql_cost: int | None,
    rest_remaining: int | None,
    rest_limit: int | None,
    elapsed: float,
    query: str,
    status_msg: str,
) -> Panel:
    """Build a Rich Panel with the current crawl progress."""
    table = Table.grid(padding=(0, 2))
    table.add_column("Label", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")
    table.add_column("Label2", style="cyan", no_wrap=True)
    table.add_column("Value2", style="white")

    # Row 1: Pages and time
    pages_est = max(1, (total_matches + 99) // 100) if total_matches else "?"
    table.add_row(
        "Page:", f"{page_num} / ~{pages_est}",
        "Elapsed:", _format_elapsed(elapsed),
    )

    # Row 2: Repos
    table.add_row(
        "Repos found:", f"[bold green]{repos_found}[/]",
        "Total matches:", f"{total_matches:,}" if total_matches else "?",
    )

    # Row 3: New / Updated
    table.add_row(
        "New:", f"[green]{repos_new}[/]",
        "Updated:", f"[yellow]{repos_updated}[/]",
    )

    # Row 4: Errors and quota
    err_style = "[red]" if errors > 0 else "[dim]"
    if gql_remaining is not None and gql_limit is not None:
        gql_str = f"{gql_remaining:,}/{gql_limit:,}"
    else:
        gql_str = "?"
    gql_color = "red" if (gql_remaining or 5000) < 200 else (
        "yellow" if (gql_remaining or 5000) < 500 else "green"
    )
    table.add_row(
        "Errors:", f"{err_style}{errors}[/]",
        "GraphQL quota:", f"[{gql_color}]{gql_str}[/] (cost: {gql_cost or '?'})",
    )

    # Row 5: REST quota
    if rest_remaining is not None and rest_limit is not None:
        rest_str = f"{rest_remaining:,}/{rest_limit:,}"
    else:
        rest_str = "?"
    rest_color = "red" if (rest_remaining or 5000) < 200 else (
        "yellow" if (rest_remaining or 5000) < 1000 else "green"
    )
    table.add_row(
        "Status:", f"[dim]{status_msg}[/]",
        "REST quota:", f"[{rest_color}]{rest_str}[/]",
    )

    short_query = (query[:60] + "...") if len(query) > 63 else query
    return Panel(
        table,
        title=f"[bold]Crawling GitHub[/]  [dim]{short_query}[/]",
        border_style="green",
        padding=(1, 2),
    )


async def run_crawl(
    settings: Settings,
    query: str | None = None,
    max_pages: int | None = None,
) -> CrawlRunModel:
    """Execute a full crawl cycle: paginate -> enrich -> persist.

    Args:
        settings: Application configuration.
        query: Override search query.
        max_pages: Override maximum pages to fetch.

    Returns:
        The completed ``CrawlRunModel`` with statistics.
    """
    if max_pages is not None:
        settings = settings.model_copy(update={"max_pages": max_pages})

    run = CrawlRunModel(
        run_id=uuid.uuid4().hex[:12],
        query_string=query or settings.default_query,
        started_at=datetime.now(tz=timezone.utc),
        status="running",
    )

    client = GitHubClient(settings)

    # Shared state dict for rate-limit tracking between paginator & spider
    rate_state: dict[str, Any] = {}
    page_num = 0
    status_msg = "Initializing..."
    start_time = monotonic()
    search_query = query or settings.default_query

    with get_connection(settings.db_path) as conn:
        create_tables(conn)
        insert_crawl_run(conn, run)

        with Live(console=console, refresh_per_second=4) as live:

            def _refresh() -> None:
                live.update(
                    _build_status_table(
                        page_num=page_num,
                        total_matches=rate_state.get("total_matches", 0),
                        repos_found=run.repos_found,
                        repos_new=run.repos_new,
                        repos_updated=run.repos_updated,
                        errors=run.errors_count,
                        gql_remaining=rate_state.get("gql_remaining"),
                        gql_limit=rate_state.get("gql_limit"),
                        gql_cost=rate_state.get("gql_cost"),
                        rest_remaining=last_rest_rate.get("remaining"),
                        rest_limit=last_rest_rate.get("limit"),
                        elapsed=monotonic() - start_time,
                        query=search_query,
                        status_msg=status_msg,
                    )
                )

            _refresh()

            try:
                consecutive_failures = 0
                max_consecutive_failures = 3

                async for page_nodes in paginate_search(
                    client, settings, query, rate_state=rate_state,
                ):
                    try:
                        page_num += 1
                        status_msg = f"Processing page {page_num}..."
                        _refresh()

                        # Parse GraphQL nodes into models
                        repos: list[RepositoryModel] = []
                        for node in page_nodes:
                            try:
                                repos.append(RepositoryModel.from_graphql(node))
                            except Exception as exc:
                                logger.warning("Skipping malformed node: {}", exc)
                                run.errors_count += 1

                        status_msg = f"Enriching {len(repos)} repos (REST)..."
                        _refresh()

                        # Enrich with REST data
                        await enrich_repos(client, settings, repos)

                        status_msg = f"Persisting {len(repos)} repos..."
                        _refresh()

                        # Persist
                        for repo in repos:
                            try:
                                is_new = upsert_repository(conn, repo)
                                insert_snapshot(conn, repo)
                                if is_new:
                                    run.repos_new += 1
                                else:
                                    run.repos_updated += 1
                                run.repos_found += 1
                            except Exception as exc:
                                logger.error(
                                    "Failed to persist {}: {}", repo.full_name, exc,
                                )
                                run.errors_count += 1

                        # Reset consecutive failure counter on success
                        consecutive_failures = 0
                        status_msg = "Waiting for next page..."
                        _refresh()

                    except Exception as page_exc:
                        consecutive_failures += 1
                        run.errors_count += 1
                        status_msg = (
                            f"Page error ({consecutive_failures}/"
                            f"{max_consecutive_failures})"
                        )
                        _refresh()
                        logger.error(
                            "Page processing failed ({}/{}): {}",
                            consecutive_failures,
                            max_consecutive_failures,
                            page_exc,
                        )
                        if consecutive_failures >= max_consecutive_failures:
                            logger.error(
                                "Aborting crawl after {} consecutive page failures.",
                                max_consecutive_failures,
                            )
                            break

                run.status = "completed"
                status_msg = "Completed!"
                _refresh()

            except Exception as exc:
                logger.error("Crawl failed: {}", exc)
                run.status = "failed"
                run.errors_count += 1
                status_msg = f"FAILED: {exc}"
                _refresh()

            finally:
                run.finished_at = datetime.now(tz=timezone.utc)
                update_crawl_run(conn, run)
                await client.close()

    elapsed = monotonic() - start_time
    logger.success(
        "Crawl {} finished in {} - found={}, new={}, updated={}, errors={}",
        run.run_id,
        _format_elapsed(elapsed),
        run.repos_found,
        run.repos_new,
        run.repos_updated,
        run.errors_count,
    )
    return run

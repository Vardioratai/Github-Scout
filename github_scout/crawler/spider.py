"""Crawl orchestrator - ties paginator, enricher, and persistence together.

Provides a live Rich progress display showing:
  - Current date slice and page within it
  - Repos found, new, refreshed, skipped, errors
  - GraphQL & REST rate-limit quota remaining
  - Elapsed time

When the GitHub Search API returns more than 1,000 results for a query
(its hard cap), the spider automatically partitions the query into
date-range slices, each staying under 1,000, so that every matching
repository is retrieved.

Smart re-crawl strategy:
  CASE A — NEW:          repo not in DB → full enrichment + insert + snapshot
  CASE B — REFRESH:      repo in DB but stale → full re-enrichment + upsert + snapshot
  CASE C — SKIP-ENRICH:  repo in DB and fresh → lightweight update + conditional snapshot
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
from github_scout.client.paginator import paginate_search, probe_query_count
from github_scout.client.rate_limiter import last_rest_rate
from github_scout.config.settings import Settings
from github_scout.crawler.enricher import enrich_repos
from github_scout.crawler.query_slicer import generate_query_slices
from github_scout.database.connection import get_connection
from github_scout.database.dao import (
    get_repo_freshness,
    insert_crawl_run,
    insert_snapshot,
    lightweight_update_repo,
    should_take_snapshot,
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
    slice_num: int,
    total_slices: int,
    page_num: int,
    total_matches: int,
    repos_found: int,
    repos_new: int,
    repos_updated: int,
    repos_refreshed: int,
    repos_skipped_fresh: int,
    snapshots_taken: int,
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

    # Row 1: Slice, page and time
    slice_str = f"{slice_num}/{total_slices}" if total_slices > 1 else "-"
    pages_est = max(1, (total_matches + 99) // 100) if total_matches else "?"
    table.add_row(
        "Slice:", slice_str,
        "Page:", f"{page_num} / ~{pages_est}",
    )

    # Row 2: Repos and time
    table.add_row(
        "Repos found:", f"[bold green]{repos_found}[/]",
        "Elapsed:", _format_elapsed(elapsed),
    )

    # Row 3: New / Refreshed
    table.add_row(
        "🆕 New:", f"[green]{repos_new}[/]",
        "🔄 Refreshed:", f"[yellow]{repos_refreshed}[/]",
    )

    # Row 4: Skipped / Snapshots
    table.add_row(
        "⏩ Skipped:", f"[dim]{repos_skipped_fresh}[/]",
        "📸 Snapshots:", f"[cyan]{snapshots_taken}[/]",
    )

    # Row 5: Errors and GraphQL quota
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

    # Row 6: Status and REST quota
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


def _build_summary_panel(run: CrawlRunModel, elapsed: float) -> Panel:
    """Build the final crawl summary panel."""
    lines = [
        f"  🆕 New repos:          {run.repos_new}",
        f"  🔄 Refreshed (stale):  {run.repos_refreshed}",
        f"  ⏩ Skipped (fresh):    {run.repos_skipped_fresh}",
        f"  📸 Snapshots taken:    {run.snapshots_taken}",
        f"  ❌ Errors:             {run.errors_count}",
        f"  ⏱  Duration:           {_format_elapsed(elapsed)}",
    ]
    return Panel(
        "\n".join(lines),
        title="[bold]Crawl Summary[/]",
        border_style="green",
        padding=(1, 2),
    )


async def run_crawl(
    settings: Settings,
    query: str | None = None,
    max_pages: int | None = None,
) -> CrawlRunModel:
    """Execute a full crawl cycle: paginate -> enrich -> persist.

    When the query matches more than 1,000 repositories (GitHub's search
    API hard cap), the query is automatically sliced into date-range
    partitions so that every result is fetched.

    Uses a three-tier smart overwrite strategy:
      - CASE A (NEW): Full enrichment + insert + snapshot
      - CASE B (REFRESH): Full re-enrichment + upsert + snapshot
      - CASE C (SKIP-ENRICH): Lightweight update + conditional snapshot

    Args:
        settings: Application configuration.
        query: Override search query.
        max_pages: Override maximum pages to fetch.

    Returns:
        The completed ``CrawlRunModel`` with statistics.
    """
    if max_pages is not None:
        settings = settings.model_copy(update={"max_pages": max_pages})

    search_query = query or settings.default_query
    force_refresh = settings.force_refresh
    refresh_ttl = settings.refresh_ttl_hours
    snapshot_ttl = settings.snapshot_ttl_hours

    run = CrawlRunModel(
        run_id=uuid.uuid4().hex[:12],
        query_string=search_query,
        started_at=datetime.now(tz=timezone.utc),
        status="running",
    )

    client = GitHubClient(settings)

    # Progress state
    rate_state: dict[str, Any] = {}
    page_num = 0
    slice_num = 0
    total_slices = 1
    status_msg = "Probing query..."
    start_time = monotonic()

    with get_connection(settings.db_path) as conn:
        create_tables(conn)
        insert_crawl_run(conn, run)

        with Live(console=console, refresh_per_second=4) as live:

            def _refresh() -> None:
                live.update(
                    _build_status_table(
                        slice_num=slice_num,
                        total_slices=total_slices,
                        page_num=page_num,
                        total_matches=rate_state.get("total_matches", 0),
                        repos_found=run.repos_found,
                        repos_new=run.repos_new,
                        repos_updated=run.repos_updated,
                        repos_refreshed=run.repos_refreshed,
                        repos_skipped_fresh=run.repos_skipped_fresh,
                        snapshots_taken=run.snapshots_taken,
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
                # ----- Step 1: Probe total result count --------------------
                total_count = await probe_query_count(client, search_query)
                status_msg = f"Total results: {total_count:,}"
                _refresh()

                # ----- Step 2: Generate query slices if needed -------------
                sub_queries = generate_query_slices(search_query, total_count)
                total_slices = len(sub_queries)

                if total_slices > 1:
                    console.print(
                        f"  [bold cyan]>> Query has {total_count:,} results "
                        f"(exceeds 1,000 limit). "
                        f"Split into {total_slices} date slices.[/]"
                    )

                # ----- Step 3: Iterate slices ------------------------------
                consecutive_failures = 0
                max_consecutive_failures = 3

                for idx, sub_q in enumerate(sub_queries, 1):
                    slice_num = idx
                    page_num = 0
                    status_msg = (
                        f"Slice {slice_num}/{total_slices}: starting..."
                    )
                    _refresh()

                    if total_slices > 1:
                        logger.info(
                            "Slice {}/{}: {}", slice_num, total_slices, sub_q,
                        )

                    async for page_nodes in paginate_search(
                        client, settings, sub_q, rate_state=rate_state,
                    ):
                        try:
                            page_num += 1
                            status_msg = (
                                f"Slice {slice_num}/{total_slices} "
                                f"- page {page_num}..."
                            )
                            _refresh()

                            # Parse GraphQL nodes into models
                            repos: list[RepositoryModel] = []
                            for node in page_nodes:
                                try:
                                    repos.append(
                                        RepositoryModel.from_graphql(node),
                                    )
                                except Exception as exc:
                                    logger.warning(
                                        "Skipping malformed node: {}", exc,
                                    )
                                    run.errors_count += 1

                            # ── Classify repos by freshness ──────────────
                            now_utc = datetime.now(tz=timezone.utc)
                            repos_to_enrich: list[RepositoryModel] = []
                            repos_fresh: list[RepositoryModel] = []

                            for repo in repos:
                                freshness = get_repo_freshness(conn, repo.id)
                                if freshness is None:
                                    # CASE A — NEW
                                    repos_to_enrich.append(repo)
                                elif force_refresh:
                                    # Force refresh overrides TTL
                                    repos_to_enrich.append(repo)
                                else:
                                    updated_in_db_at = freshness[0]
                                    if updated_in_db_at is not None:
                                        age_hours = (
                                            now_utc - updated_in_db_at.replace(
                                                tzinfo=timezone.utc,
                                            )
                                        ).total_seconds() / 3600
                                    else:
                                        age_hours = float("inf")

                                    if age_hours >= refresh_ttl:
                                        # CASE B — STALE
                                        repos_to_enrich.append(repo)
                                    else:
                                        # CASE C — FRESH
                                        repos_fresh.append(repo)

                            # ── Enrich only repos that need it ───────────
                            if repos_to_enrich:
                                status_msg = (
                                    f"Enriching {len(repos_to_enrich)} repos "
                                    f"(REST)..."
                                )
                                _refresh()
                                await enrich_repos(
                                    client, settings, repos_to_enrich,
                                )

                            status_msg = (
                                f"Persisting {len(repos)} repos..."
                            )
                            _refresh()

                            # ── Persist: full upsert for enriched repos ──
                            for repo in repos_to_enrich:
                                try:
                                    freshness = get_repo_freshness(
                                        conn, repo.id,
                                    )
                                    is_new = freshness is None
                                    upsert_repository(conn, repo)
                                    insert_snapshot(conn, repo)
                                    run.snapshots_taken += 1

                                    if is_new:
                                        run.repos_new += 1
                                        logger.info(
                                            "[NEW] {}", repo.full_name,
                                        )
                                    else:
                                        run.repos_refreshed += 1
                                        logger.info(
                                            "[REFRESH] {} — stale",
                                            repo.full_name,
                                        )
                                    run.repos_found += 1
                                except Exception as exc:
                                    logger.error(
                                        "Failed to persist {}: {}",
                                        repo.full_name, exc,
                                    )
                                    run.errors_count += 1

                            # ── Persist: lightweight update for fresh ─────
                            for repo in repos_fresh:
                                try:
                                    lightweight_update_repo(conn, repo)
                                    run.repos_skipped_fresh += 1
                                    run.repos_found += 1

                                    # Conditional snapshot
                                    if should_take_snapshot(
                                        conn, repo.id, snapshot_ttl,
                                    ):
                                        insert_snapshot(conn, repo)
                                        run.snapshots_taken += 1

                                    logger.debug(
                                        "[SKIP-ENRICH] {} — fresh",
                                        repo.full_name,
                                    )
                                except Exception as exc:
                                    logger.error(
                                        "Failed to update {}: {}",
                                        repo.full_name, exc,
                                    )
                                    run.errors_count += 1

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
                                    "Aborting crawl after {} consecutive "
                                    "page failures.",
                                    max_consecutive_failures,
                                )
                                break

                    # Log slice summary
                    if total_slices > 1:
                        logger.info(
                            "Slice {}/{} done. Running total: {} repos.",
                            slice_num, total_slices, run.repos_found,
                        )

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

    # Print the final summary panel
    console.print(_build_summary_panel(run, elapsed))

    logger.success(
        "Crawl {} finished in {} — new={}, refreshed={}, "
        "skipped={}, snapshots={}, errors={}",
        run.run_id,
        _format_elapsed(elapsed),
        run.repos_new,
        run.repos_refreshed,
        run.repos_skipped_fresh,
        run.snapshots_taken,
        run.errors_count,
    )
    return run

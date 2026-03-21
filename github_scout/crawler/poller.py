"""Poller for existing repositories using ETag conditional requests."""

from __future__ import annotations

import asyncio
from loguru import logger
from rich.console import Console
from rich.panel import Panel

from github_scout.client.github_client import GitHubClient
from github_scout.config.settings import Settings
from github_scout.crawler.enricher import enrich_repos
from github_scout.database.connection import get_connection
from github_scout.database.dao import upsert_repository, insert_snapshot
from github_scout.database.schema import create_tables
from github_scout.models.repository import RepositoryModel

__all__: list[str] = ["run_poll"]

console = Console()


async def run_poll(settings: Settings, limit: int) -> None:
    """Poll known repositories for updates using REST API and ETag conditional requests."""
    client = GitHubClient(settings)

    with get_connection(settings.db_path) as conn:
        create_tables(conn)

        # Fetch up to `limit` repos ordered by updated_in_db_at ASC 
        # to prioritize repos that haven't been checked in a while.
        rows = conn.execute(
            """
            SELECT id, full_name, owner_login, name, etag 
            FROM repositories 
            ORDER BY updated_in_db_at ASC 
            LIMIT $1
            """,
            [limit],
        ).fetchall()

        if not rows:
            logger.info("No repositories found in DB to poll.")
            await client.close()
            return

        polled = 0
        not_modified = 0
        updated = 0
        errors = 0

        for row in rows:
            repo_id, full_name, owner, name, etag = row
            polled += 1
            
            try:
                resp = await client.rest_get(f"/repos/{owner}/{name}", etag=etag)
                if resp.status_code == 304:
                    not_modified += 1
                    # Update updated_in_db_at to avoid polling it again too soon
                    conn.execute(
                        "UPDATE repositories SET updated_in_db_at = current_timestamp WHERE id = $1",
                        [repo_id],
                    )
                    logger.debug("304 Not Modified: {}", full_name)
                elif resp.status_code == 200:
                    updated += 1
                    data = resp.json()
                    new_etag = resp.headers.get("etag")
                    logger.info("200 OK: {}. Enriched and updated.", full_name)

                    # Build partial repository from REST payload
                    repo = RepositoryModel.from_rest(data, etag=new_etag)
                    
                    # Full enrichment of README, releases, and contributors
                    await enrich_repos(client, settings, [repo])
                    
                    # Persist
                    upsert_repository(conn, repo)
                    insert_snapshot(conn, repo)
                else:
                    logger.warning("Unexpected status code {} for {}", resp.status_code, full_name)

            except Exception as exc:
                errors += 1
                logger.error("Failed polling {}: {}", full_name, exc)

        await client.close()

    lines = [
        f"  Total checked:    {polled}",
        f"  ⏩ 304 Fresh:     {not_modified}",
        f"  🔄 200 Updated:   {updated}",
        f"  ❌ Errors:        {errors}",
    ]
    console.print(
        Panel(
            "\\n".join(lines),
            title="[bold]Polling Summary[/]",
            border_style="green",
            padding=(1, 2),
        )
    )

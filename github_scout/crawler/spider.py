"""Crawl orchestrator — ties paginator, enricher, and persistence together."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from loguru import logger

from github_scout.client.github_client import GitHubClient
from github_scout.client.paginator import paginate_search
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


async def run_crawl(
    settings: Settings,
    query: str | None = None,
    max_pages: int | None = None,
) -> CrawlRunModel:
    """Execute a full crawl cycle: paginate → enrich → persist.

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

    with get_connection(settings.db_path) as conn:
        create_tables(conn)
        insert_crawl_run(conn, run)

        try:
            async for page_nodes in paginate_search(client, settings, query):
                # Parse GraphQL nodes into models
                repos: list[RepositoryModel] = []
                for node in page_nodes:
                    try:
                        repos.append(RepositoryModel.from_graphql(node))
                    except Exception as exc:
                        logger.warning("Skipping malformed node: {}", exc)
                        run.errors_count += 1

                # Enrich with REST data
                await enrich_repos(client, settings, repos)

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
                        logger.error("Failed to persist {}: {}", repo.full_name, exc)
                        run.errors_count += 1

            run.status = "completed"
        except Exception as exc:
            logger.error("Crawl failed: {}", exc)
            run.status = "failed"
            run.errors_count += 1
        finally:
            run.finished_at = datetime.now(tz=timezone.utc)
            update_crawl_run(conn, run)
            await client.close()

    logger.success(
        "Crawl {} finished — found={}, new={}, updated={}, errors={}",
        run.run_id,
        run.repos_found,
        run.repos_new,
        run.repos_updated,
        run.errors_count,
    )
    return run

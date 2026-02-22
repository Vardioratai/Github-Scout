"""Data-access objects — UPSERT, snapshot, and crawl-run persistence."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
from loguru import logger

from github_scout.models.crawl_run import CrawlRunModel
from github_scout.models.repository import RepositoryModel

__all__: list[str] = [
    "upsert_repository",
    "insert_snapshot",
    "insert_crawl_run",
    "update_crawl_run",
    "repo_exists",
]


def upsert_repository(conn: duckdb.DuckDBPyConnection, repo: RepositoryModel) -> bool:
    """Insert or replace a repository row (idempotent upsert).

    On replace, ``scraped_at`` is preserved from the original row while
    ``updated_in_db_at`` is refreshed.

    Args:
        conn: An open DuckDB connection.
        repo: Validated repository model.

    Returns:
        ``True`` if the repo was newly inserted, ``False`` if updated.
    """
    is_new = not repo_exists(conn, repo.id)

    now = datetime.now(tz=timezone.utc).isoformat()

    conn.execute(
        """
        INSERT OR REPLACE INTO repositories (
            id, name, full_name, owner_login, owner_type,
            description, url, homepage_url, primary_language,
            topics, license_spdx, is_archived, is_fork, is_template,
            stars, forks, watchers, open_issues,
            created_at, updated_at, pushed_at, disk_usage_kb,
            readme_length_chars, readme_h2_sections,
            readme_has_badges, readme_has_demo_gif, readme_has_install,
            contributors_count, releases_count,
            latest_release_tag, latest_release_at,
            scraped_at, updated_in_db_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            $6, $7, $8, $9,
            $10, $11, $12, $13, $14,
            $15, $16, $17, $18,
            $19, $20, $21, $22,
            $23, $24,
            $25, $26, $27,
            $28, $29,
            $30, $31,
            COALESCE(
                (SELECT scraped_at FROM repositories WHERE id = $1),
                $32::TIMESTAMPTZ
            ),
            $32::TIMESTAMPTZ
        )
        """,
        [
            repo.id,
            repo.name,
            repo.full_name,
            repo.owner_login,
            repo.owner_type,
            repo.description,
            repo.url,
            repo.homepage_url,
            repo.primary_language,
            repo.topics,
            repo.license_spdx,
            repo.is_archived,
            repo.is_fork,
            repo.is_template,
            repo.stars,
            repo.forks,
            repo.watchers,
            repo.open_issues,
            repo.created_at.isoformat() if repo.created_at else None,
            repo.updated_at.isoformat() if repo.updated_at else None,
            repo.pushed_at.isoformat() if repo.pushed_at else None,
            repo.disk_usage_kb,
            repo.readme_length_chars,
            repo.readme_h2_sections,
            repo.readme_has_badges,
            repo.readme_has_demo_gif,
            repo.readme_has_install,
            repo.contributors_count,
            repo.releases_count,
            repo.latest_release_tag,
            repo.latest_release_at.isoformat() if repo.latest_release_at else None,
            now,
        ],
    )
    return is_new


def insert_snapshot(
    conn: duckdb.DuckDBPyConnection,
    repo: RepositoryModel,
) -> None:
    """Record a point-in-time snapshot for momentum tracking.

    Always inserts a **new** row so historical deltas can be computed.

    Args:
        conn: An open DuckDB connection.
        repo: Validated repository model.
    """
    conn.execute(
        """
        INSERT INTO repo_snapshots (repo_id, snapshot_at, stars, forks, open_issues)
        VALUES ($1, current_timestamp, $2, $3, $4)
        """,
        [repo.id, repo.stars, repo.forks, repo.open_issues],
    )


def repo_exists(conn: duckdb.DuckDBPyConnection, repo_id: str) -> bool:
    """Check whether a repository already exists in the database.

    Args:
        conn: An open DuckDB connection.
        repo_id: The GitHub node ID.

    Returns:
        ``True`` if a row exists.
    """
    result = conn.execute(
        "SELECT 1 FROM repositories WHERE id = $1", [repo_id]
    ).fetchone()
    return result is not None


def insert_crawl_run(
    conn: duckdb.DuckDBPyConnection,
    run: CrawlRunModel,
) -> None:
    """Persist a new crawl-run record.

    Args:
        conn: An open DuckDB connection.
        run: Crawl-run metadata.
    """
    conn.execute(
        """
        INSERT INTO crawl_runs (
            run_id, query_string, started_at, finished_at,
            repos_found, repos_new, repos_updated, errors_count, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        [
            run.run_id,
            run.query_string,
            run.started_at.isoformat() if run.started_at else None,
            run.finished_at.isoformat() if run.finished_at else None,
            run.repos_found,
            run.repos_new,
            run.repos_updated,
            run.errors_count,
            run.status,
        ],
    )
    logger.info("Crawl run {} inserted (status={}).", run.run_id, run.status)


def update_crawl_run(
    conn: duckdb.DuckDBPyConnection,
    run: CrawlRunModel,
) -> None:
    """Update an existing crawl-run record.

    Args:
        conn: An open DuckDB connection.
        run: Crawl-run metadata with updated fields.
    """
    conn.execute(
        """
        UPDATE crawl_runs SET
            finished_at   = $2,
            repos_found   = $3,
            repos_new     = $4,
            repos_updated = $5,
            errors_count  = $6,
            status        = $7
        WHERE run_id = $1
        """,
        [
            run.run_id,
            run.finished_at.isoformat() if run.finished_at else None,
            run.repos_found,
            run.repos_new,
            run.repos_updated,
            run.errors_count,
            run.status,
        ],
    )

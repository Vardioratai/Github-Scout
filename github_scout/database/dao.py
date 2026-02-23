"""Data-access objects — UPSERT, snapshot, and crawl-run persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

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
    "count_repos_matching",
    "delete_repos",
    "delete_orphan_snapshots",
    "count_orphan_snapshots",
]


# ------------------------------------------------------------------
# Filter-based cleanup helpers
# ------------------------------------------------------------------


def _build_where_clause(
    filters: dict[str, Any],
) -> tuple[str, list[Any]]:
    """Build a dynamic WHERE clause from *filters*.

    Supported keys (all optional):
        * ``before``  – ISO-date string  → ``scraped_at < ?``
        * ``score_below`` – float        → ``potential_score < ? OR potential_score IS NULL``
        * ``archived``    – bool          → ``is_archived = true``
        * ``forks``       – bool          → ``is_fork = true``
        * ``language``    – str           → ``lower(primary_language) = lower(?)``

    Returns:
        A ``(clause, params)`` tuple.  *clause* is an empty string when no
        filters are active (caller should guard against this).
    """
    conditions: list[str] = []
    params: list[Any] = []

    if filters.get("before"):
        conditions.append(f"scraped_at < ${len(params) + 1}::TIMESTAMPTZ")
        params.append(filters["before"])

    if filters.get("score_below") is not None:
        conditions.append(
            f"(potential_score < ${len(params) + 1} OR potential_score IS NULL)"
        )
        params.append(filters["score_below"])

    if filters.get("archived"):
        conditions.append("is_archived = true")

    if filters.get("forks"):
        conditions.append("is_fork = true")

    if filters.get("language"):
        conditions.append(f"lower(primary_language) = lower(${len(params) + 1})")
        params.append(filters["language"])

    clause = " AND ".join(conditions) if conditions else ""
    return clause, params


def count_repos_matching(
    conn: duckdb.DuckDBPyConnection,
    filters: dict[str, Any],
) -> int:
    """Return how many repository rows match *filters*.

    Used by the CLI dry-run mode to preview deletions without mutating data.
    """
    clause, params = _build_where_clause(filters)
    if not clause:
        return 0
    sql = f"SELECT COUNT(*) FROM repositories WHERE {clause}"
    row = conn.execute(sql, params).fetchone()
    return int(row[0]) if row else 0


def count_orphan_snapshots(conn: duckdb.DuckDBPyConnection) -> int:
    """Count repo_snapshots rows whose repo_id has no matching repository."""
    row = conn.execute(
        "SELECT COUNT(*) FROM repo_snapshots "
        "WHERE repo_id NOT IN (SELECT id FROM repositories)"
    ).fetchone()
    return int(row[0]) if row else 0


def delete_repos(
    conn: duckdb.DuckDBPyConnection,
    filters: dict[str, Any],
) -> tuple[int, int]:
    """Delete repository rows matching *filters* and cascade to snapshots.

    Args:
        conn: An open DuckDB connection.
        filters: Dictionary produced by the CLI (same keys as
            :func:`_build_where_clause`).  If the ``all_data`` key is truthy
            the three main tables are truncated entirely.

    Returns:
        ``(deleted_repos, deleted_snapshots)`` counts.
    """
    # ── full purge ────────────────────────────────────────────────
    if filters.get("all_data"):
        repo_count = conn.execute("SELECT COUNT(*) FROM repositories").fetchone()
        snap_count = conn.execute("SELECT COUNT(*) FROM repo_snapshots").fetchone()
        n_repos = int(repo_count[0]) if repo_count else 0
        n_snaps = int(snap_count[0]) if snap_count else 0

        conn.execute("DELETE FROM repositories")
        conn.execute("DELETE FROM repo_snapshots")
        conn.execute("DELETE FROM crawl_runs")
        logger.warning(
            "Full purge: deleted {} repos, {} snapshots, and all crawl_runs.",
            n_repos,
            n_snaps,
        )
        return n_repos, n_snaps

    # ── filtered delete ───────────────────────────────────────────
    clause, params = _build_where_clause(filters)
    if not clause:
        return 0, 0

    # Count before deleting
    n_repos = count_repos_matching(conn, filters)

    # Delete repos
    conn.execute(f"DELETE FROM repositories WHERE {clause}", params)

    # Cascade: remove orphan snapshots
    n_snaps_row = conn.execute(
        "SELECT COUNT(*) FROM repo_snapshots "
        "WHERE repo_id NOT IN (SELECT id FROM repositories)"
    ).fetchone()
    n_snaps = int(n_snaps_row[0]) if n_snaps_row else 0

    conn.execute(
        "DELETE FROM repo_snapshots "
        "WHERE repo_id NOT IN (SELECT id FROM repositories)"
    )

    logger.warning(
        "Filtered delete: removed {} repos and {} orphan snapshots.", n_repos, n_snaps
    )
    return n_repos, n_snaps


def delete_orphan_snapshots(conn: duckdb.DuckDBPyConnection) -> int:
    """Delete repo_snapshots with no matching repository row.

    Returns:
        Number of deleted snapshot rows.
    """
    n = count_orphan_snapshots(conn)
    if n:
        conn.execute(
            "DELETE FROM repo_snapshots "
            "WHERE repo_id NOT IN (SELECT id FROM repositories)"
        )
        logger.warning("Deleted {} orphan snapshots.", n)
    return n


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

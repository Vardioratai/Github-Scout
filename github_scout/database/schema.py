"""DuckDB schema initialisation — DDL statements."""

from __future__ import annotations

import duckdb

__all__: list[str] = ["create_tables"]

REPOSITORIES_DDL: str = """
CREATE TABLE IF NOT EXISTS repositories (
    id                    VARCHAR PRIMARY KEY,
    name                  VARCHAR NOT NULL,
    full_name             VARCHAR NOT NULL,
    owner_login           VARCHAR,
    owner_type            VARCHAR,
    description           VARCHAR,
    url                   VARCHAR,
    homepage_url          VARCHAR,
    primary_language      VARCHAR,
    topics                VARCHAR[],
    license_spdx          VARCHAR,
    is_archived           BOOLEAN DEFAULT false,
    is_fork               BOOLEAN DEFAULT false,
    is_template           BOOLEAN DEFAULT false,
    stars                 INTEGER DEFAULT 0,
    forks                 INTEGER DEFAULT 0,
    watchers              INTEGER DEFAULT 0,
    open_issues           INTEGER DEFAULT 0,
    closed_issues         INTEGER DEFAULT 0,
    created_at            TIMESTAMPTZ,
    updated_at            TIMESTAMPTZ,
    pushed_at             TIMESTAMPTZ,
    disk_usage_kb         INTEGER,
    readme_length_chars   INTEGER,
    readme_h2_sections    INTEGER,
    readme_has_badges     BOOLEAN DEFAULT false,
    readme_has_demo_gif   BOOLEAN DEFAULT false,
    readme_has_install    BOOLEAN DEFAULT false,
    contributors_count    INTEGER,
    releases_count        INTEGER,
    latest_release_tag    VARCHAR,
    latest_release_at     TIMESTAMPTZ,
    star_velocity         DOUBLE,
    momentum_7d           DOUBLE,
    activity_score        DOUBLE,
    readme_quality        DOUBLE,
    potential_score       DOUBLE,
    HG_score              DOUBLE,
    age_tier              VARCHAR,
    maturity_tier         VARCHAR,
    scraped_at            TIMESTAMPTZ DEFAULT current_timestamp,
    updated_in_db_at      TIMESTAMPTZ DEFAULT current_timestamp
);
"""

REPO_SNAPSHOTS_DDL: str = """
CREATE TABLE IF NOT EXISTS repo_snapshots (
    repo_id       VARCHAR NOT NULL,
    snapshot_at   TIMESTAMPTZ DEFAULT current_timestamp,
    stars         INTEGER,
    forks         INTEGER,
    open_issues   INTEGER,
    PRIMARY KEY (repo_id, snapshot_at)
);
"""

CRAWL_RUNS_DDL: str = """
CREATE TABLE IF NOT EXISTS crawl_runs (
    run_id              VARCHAR PRIMARY KEY,
    query_string        VARCHAR,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ,
    repos_found         INTEGER DEFAULT 0,
    repos_new           INTEGER DEFAULT 0,
    repos_updated       INTEGER DEFAULT 0,
    repos_refreshed     INTEGER DEFAULT 0,
    repos_skipped_fresh INTEGER DEFAULT 0,
    snapshots_taken     INTEGER DEFAULT 0,
    errors_count        INTEGER DEFAULT 0,
    status              VARCHAR
);
"""


# Migration helpers — add columns that may be missing in older databases.
_REPOSITORIES_MIGRATIONS: list[str] = [
    "ALTER TABLE repositories ADD COLUMN IF NOT EXISTS age_tier VARCHAR",
    "ALTER TABLE repositories ADD COLUMN IF NOT EXISTS maturity_tier VARCHAR",
    "ALTER TABLE repositories ADD COLUMN IF NOT EXISTS closed_issues INTEGER DEFAULT 0",
    "ALTER TABLE repositories ADD COLUMN IF NOT EXISTS HG_score DOUBLE",
]

_CRAWL_RUNS_MIGRATIONS: list[str] = [
    "ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS repos_refreshed INTEGER DEFAULT 0",
    "ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS repos_skipped_fresh INTEGER DEFAULT 0",
    "ALTER TABLE crawl_runs ADD COLUMN IF NOT EXISTS snapshots_taken INTEGER DEFAULT 0",
]


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Execute all DDL statements to ensure schema exists.

    Also applies lightweight migrations for columns added in newer versions.

    Args:
        conn: An open DuckDB connection.
    """
    conn.execute(REPOSITORIES_DDL)
    conn.execute(REPO_SNAPSHOTS_DDL)
    conn.execute(CRAWL_RUNS_DDL)

    # Apply migrations for existing tables
    for stmt in _REPOSITORIES_MIGRATIONS:
        conn.execute(stmt)
    for stmt in _CRAWL_RUNS_MIGRATIONS:
        conn.execute(stmt)

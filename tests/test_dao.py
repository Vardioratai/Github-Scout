"""Tests for the data-access layer — upsert idempotency, snapshots, and new fields."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from github_scout.database.dao import insert_snapshot, upsert_repository
from github_scout.database.schema import create_tables
from github_scout.models.repository import RepositoryModel


def _get_test_conn() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with schema."""
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    return conn


def _make_repo(
    repo_id: str = "R_abc123",
    open_issues: int = 5,
    closed_issues: int = 15,
) -> RepositoryModel:
    """Build a minimal test RepositoryModel."""
    return RepositoryModel(
        id=repo_id,
        name="test-repo",
        full_name="owner/test-repo",
        owner_login="owner",
        owner_type="User",
        description="A test project",
        url="https://github.com/owner/test-repo",
        primary_language="Python",
        stars=100,
        forks=20,
        open_issues=open_issues,
        closed_issues=closed_issues,
        created_at=datetime(2026, 1, 10, tzinfo=timezone.utc),
        updated_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
    )


# ------------------------------------------------------------------
# Test: upsert same repo_id twice → COUNT(*) = 1 in repositories
# ------------------------------------------------------------------


def test_upsert_same_repo_produces_single_row() -> None:
    """Upserting the same ``repo_id`` twice must yield exactly one row."""
    conn = _get_test_conn()

    repo = _make_repo()
    upsert_repository(conn, repo)

    # Upsert again with updated stars
    repo_updated = repo.model_copy(update={"stars": 200})
    upsert_repository(conn, repo_updated)

    (count,) = conn.execute(
        "SELECT COUNT(*) FROM repositories WHERE id = $1", [repo.id]
    ).fetchone()  # type: ignore[misc]

    assert count == 1, f"Expected exactly 1 row, got {count}"

    # Verify the update took effect
    (stars,) = conn.execute(
        "SELECT stars FROM repositories WHERE id = $1", [repo.id]
    ).fetchone()  # type: ignore[misc]
    assert stars == 200

    conn.close()


# ------------------------------------------------------------------
# Test: upsert same repo_id twice → COUNT(*) = 2 in repo_snapshots
# ------------------------------------------------------------------


def test_upsert_produces_two_snapshots() -> None:
    """Each upsert must insert a **new** snapshot row for momentum tracking."""
    conn = _get_test_conn()

    repo = _make_repo()

    upsert_repository(conn, repo)
    insert_snapshot(conn, repo)

    # Second upsert + snapshot
    repo_updated = repo.model_copy(update={"stars": 200})
    upsert_repository(conn, repo_updated)
    insert_snapshot(conn, repo_updated)

    (count,) = conn.execute(
        "SELECT COUNT(*) FROM repo_snapshots WHERE repo_id = $1", [repo.id]
    ).fetchone()  # type: ignore[misc]

    assert count == 2, f"Expected 2 snapshots, got {count}"

    conn.close()


# ------------------------------------------------------------------
# Test: get_repo_freshness returns None for unknown repo
# ------------------------------------------------------------------


def test_get_repo_freshness_unknown() -> None:
    """get_repo_freshness returns None for a repo not in the database."""
    from github_scout.database.dao import get_repo_freshness

    conn = _get_test_conn()
    assert get_repo_freshness(conn, "R_unknown") is None
    conn.close()


# ------------------------------------------------------------------
# Test: get_repo_freshness returns timestamps for known repo
# ------------------------------------------------------------------


def test_get_repo_freshness_known() -> None:
    """get_repo_freshness returns (updated_in_db_at, scraped_at) after upsert."""
    from github_scout.database.dao import get_repo_freshness

    conn = _get_test_conn()
    repo = _make_repo()
    upsert_repository(conn, repo)

    result = get_repo_freshness(conn, repo.id)
    assert result is not None
    updated_in_db_at, scraped_at = result
    assert updated_in_db_at is not None
    assert scraped_at is not None
    conn.close()


# ------------------------------------------------------------------
# Test: closed_issues persisted on upsert
# ------------------------------------------------------------------


def test_upsert_persists_closed_issues() -> None:
    """Upserting a repo with closed_issues should save the value to the DB."""
    conn = _get_test_conn()

    repo = _make_repo(closed_issues=42)
    upsert_repository(conn, repo)

    (closed,) = conn.execute(
        "SELECT closed_issues FROM repositories WHERE id = $1", [repo.id]
    ).fetchone()  # type: ignore[misc]

    assert closed == 42, f"Expected closed_issues=42, got {closed}"
    conn.close()


# ------------------------------------------------------------------
# Test: closed_issues updated on upsert
# ------------------------------------------------------------------


def test_upsert_updates_closed_issues() -> None:
    """Re-upserting a repo with a new closed_issues count should overwrite the old value."""
    conn = _get_test_conn()

    repo = _make_repo(closed_issues=10)
    upsert_repository(conn, repo)

    repo_updated = repo.model_copy(update={"closed_issues": 99})
    upsert_repository(conn, repo_updated)

    (closed,) = conn.execute(
        "SELECT closed_issues FROM repositories WHERE id = $1", [repo.id]
    ).fetchone()  # type: ignore[misc]

    assert closed == 99, f"Expected closed_issues=99 after update, got {closed}"
    conn.close()


# ------------------------------------------------------------------
# Test: lightweight_update_repo only changes volatile fields
# ------------------------------------------------------------------


def test_lightweight_update_preserves_enrichment() -> None:
    """lightweight_update must NOT overwrite enrichment fields like readme_length_chars."""
    from github_scout.database.dao import lightweight_update_repo

    conn = _get_test_conn()
    repo = _make_repo()
    # Simulate enriched data
    repo.readme_length_chars = 5000
    repo.contributors_count = 42
    upsert_repository(conn, repo)

    # Create a fresh model with only GraphQL data (no enrichment)
    updated = repo.model_copy(
        update={"stars": 999, "forks": 50, "open_issues": 10, "closed_issues": 25}
    )
    lightweight_update_repo(conn, updated)

    row = conn.execute(
        "SELECT stars, forks, open_issues, closed_issues, readme_length_chars, contributors_count "
        "FROM repositories WHERE id = $1",
        [repo.id],
    ).fetchone()
    assert row is not None

    stars, forks, open_issues, closed_issues, readme_len, contribs = row
    # Volatile fields should be updated
    assert stars == 999
    assert forks == 50
    assert open_issues == 10
    assert closed_issues == 25
    # Enrichment fields should be preserved
    assert readme_len == 5000
    assert contribs == 42
    conn.close()


# ------------------------------------------------------------------
# Test: lightweight_update_repo persists closed_issues update
# ------------------------------------------------------------------


def test_lightweight_update_persists_closed_issues() -> None:
    """lightweight_update must persist the updated closed_issues count."""
    from github_scout.database.dao import lightweight_update_repo

    conn = _get_test_conn()
    repo = _make_repo(open_issues=5, closed_issues=10)
    upsert_repository(conn, repo)

    updated = repo.model_copy(update={"open_issues": 3, "closed_issues": 77})
    lightweight_update_repo(conn, updated)

    row = conn.execute(
        "SELECT open_issues, closed_issues FROM repositories WHERE id = $1",
        [repo.id],
    ).fetchone()
    assert row is not None
    assert row[0] == 3
    assert row[1] == 77
    conn.close()


# ------------------------------------------------------------------
# Test: should_take_snapshot with no snapshots → True
# ------------------------------------------------------------------


def test_should_take_snapshot_no_existing() -> None:
    """should_take_snapshot returns True when no snapshot exists."""
    from github_scout.database.dao import should_take_snapshot

    conn = _get_test_conn()
    assert should_take_snapshot(conn, "R_never_snapped", ttl_hours=6) is True
    conn.close()


# ------------------------------------------------------------------
# Test: should_take_snapshot returns False when recent snapshot exists
# ------------------------------------------------------------------


def test_should_take_snapshot_recent() -> None:
    """should_take_snapshot returns False immediately after a snapshot."""
    from github_scout.database.dao import should_take_snapshot

    conn = _get_test_conn()
    repo = _make_repo()
    upsert_repository(conn, repo)
    insert_snapshot(conn, repo)

    # Just snapped — should be False with any reasonable TTL
    assert should_take_snapshot(conn, repo.id, ttl_hours=6) is False
    conn.close()


# ------------------------------------------------------------------
# Test: schema migration adds closed_issues and HG_score columns
# ------------------------------------------------------------------


def test_schema_migration_adds_new_columns() -> None:
    """create_tables must add closed_issues and HG_score columns via migration."""
    conn = duckdb.connect(":memory:")

    # Simulate a pre-migration schema without the new columns
    conn.execute("""
        CREATE TABLE repositories (
            id            VARCHAR PRIMARY KEY,
            name          VARCHAR NOT NULL,
            full_name     VARCHAR NOT NULL,
            stars         INTEGER DEFAULT 0,
            forks         INTEGER DEFAULT 0,
            open_issues   INTEGER DEFAULT 0,
            age_tier      VARCHAR,
            maturity_tier VARCHAR,
            scraped_at    TIMESTAMPTZ DEFAULT current_timestamp,
            updated_in_db_at TIMESTAMPTZ DEFAULT current_timestamp
        )
    """)

    # Running create_tables should add missing columns via migrations
    create_tables(conn)

    cols = {
        row[0]
        for row in conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'repositories'"
        ).fetchall()
    }

    assert "closed_issues" in cols, "closed_issues column should be created by migration"
    assert "HG_score" in cols, "HG_score column should be created by migration"
    conn.close()

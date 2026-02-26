"""Tests for the data-access layer — upsert idempotency and snapshots."""

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


def _make_repo(repo_id: str = "R_abc123") -> RepositoryModel:
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
        open_issues=5,
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
        update={"stars": 999, "forks": 50, "open_issues": 10}
    )
    lightweight_update_repo(conn, updated)

    row = conn.execute(
        "SELECT stars, forks, open_issues, readme_length_chars, contributors_count "
        "FROM repositories WHERE id = $1",
        [repo.id],
    ).fetchone()
    assert row is not None

    stars, forks, open_issues, readme_len, contribs = row
    # Volatile fields should be updated
    assert stars == 999
    assert forks == 50
    assert open_issues == 10
    # Enrichment fields should be preserved
    assert readme_len == 5000
    assert contribs == 42
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

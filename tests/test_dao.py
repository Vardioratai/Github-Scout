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

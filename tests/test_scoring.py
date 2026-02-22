"""Tests for the scoring pipeline — range and null checks."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

from github_scout.database.dao import insert_snapshot, upsert_repository
from github_scout.database.schema import create_tables
from github_scout.models.repository import RepositoryModel
from github_scout.scoring.scorer import _score_pipeline


def _get_test_conn() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with schema."""
    conn = duckdb.connect(":memory:")
    create_tables(conn)
    return conn


def _make_repo(
    repo_id: str = "R_test1",
    stars: int = 500,
    forks: int = 50,
    open_issues: int = 10,
) -> RepositoryModel:
    """Build a test RepositoryModel with enrichment fields."""
    return RepositoryModel(
        id=repo_id,
        name="test-repo",
        full_name="owner/test-repo",
        owner_login="owner",
        owner_type="User",
        description="A test project",
        url="https://github.com/owner/test-repo",
        primary_language="Python",
        stars=stars,
        forks=forks,
        open_issues=open_issues,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
        pushed_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
        readme_length_chars=2000,
        readme_h2_sections=5,
        readme_has_badges=True,
        readme_has_demo_gif=True,
        readme_has_install=True,
        contributors_count=20,
        releases_count=5,
    )


# ------------------------------------------------------------------
# Test: potential_score always in [0.0, 100.0]
# ------------------------------------------------------------------


def test_potential_score_range() -> None:
    """All potential_score values must be within [0.0, 100.0]."""
    conn = _get_test_conn()

    # Insert several repos with varying metrics
    for i in range(5):
        repo = _make_repo(
            repo_id=f"R_{i}",
            stars=100 * (i + 1),
            forks=10 * (i + 1),
            open_issues=i,
        )
        upsert_repository(conn, repo)
        insert_snapshot(conn, repo)

    scored = _score_pipeline(conn)
    assert scored == 5

    rows = conn.execute(
        "SELECT potential_score FROM repositories WHERE potential_score IS NOT NULL"
    ).fetchall()

    assert len(rows) == 5, "All 5 repos should have scores"
    for (score,) in rows:
        assert 0.0 <= score <= 100.0, f"Score {score} is out of range [0, 100]"

    conn.close()


# ------------------------------------------------------------------
# Test: no nulls in scoring output columns
# ------------------------------------------------------------------


def test_no_nulls_in_scored_columns() -> None:
    """After scoring, key output columns must not be null."""
    conn = _get_test_conn()

    repo = _make_repo()
    upsert_repository(conn, repo)
    insert_snapshot(conn, repo)

    _score_pipeline(conn)

    row = conn.execute(
        """
        SELECT star_velocity, activity_score, readme_quality, potential_score
        FROM repositories
        WHERE id = 'R_test1'
        """
    ).fetchone()

    assert row is not None
    for i, col_name in enumerate(
        ["star_velocity", "activity_score", "readme_quality", "potential_score"]
    ):
        assert row[i] is not None, f"{col_name} should not be null after scoring"

    conn.close()

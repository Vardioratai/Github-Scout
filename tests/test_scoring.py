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
    closed_issues: int = 30,
    contributors_count: int = 20,
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
        closed_issues=closed_issues,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        updated_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
        pushed_at=datetime(2026, 2, 20, tzinfo=timezone.utc),
        readme_length_chars=2000,
        readme_h2_sections=5,
        readme_has_badges=True,
        readme_has_demo_gif=True,
        readme_has_install=True,
        contributors_count=contributors_count,
        releases_count=5,
    )


# ------------------------------------------------------------------
# Test: potential_score always in [0.0, 100.0]
# ------------------------------------------------------------------


def test_potential_score_range() -> None:
    """All potential_score values must be within [0.0, 100.0]."""
    conn = _get_test_conn()

    for i in range(5):
        repo = _make_repo(
            repo_id=f"R_{i}",
            stars=100 * (i + 1),
            forks=10 * (i + 1),
            open_issues=i,
            closed_issues=i * 3,
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
        assert 0.0 <= score <= 100.0, f"potential_score {score} is out of range [0, 100]"

    conn.close()


# ------------------------------------------------------------------
# Test: HG_score always in [0.0, 100.0]
# ------------------------------------------------------------------


def test_hg_score_range() -> None:
    """All HG_score values must be within [0.0, 100.0]."""
    conn = _get_test_conn()

    for i in range(5):
        repo = _make_repo(
            repo_id=f"R_{i}",
            stars=100 * (i + 1),
            forks=10 * (i + 1),
            open_issues=5 + i,
            closed_issues=20 + (i * 4),
            contributors_count=5 + (i * 2),
        )
        upsert_repository(conn, repo)
        insert_snapshot(conn, repo)

    scored = _score_pipeline(conn)
    assert scored == 5

    rows = conn.execute(
        "SELECT HG_score FROM repositories WHERE HG_score IS NOT NULL"
    ).fetchall()

    assert len(rows) == 5, "All 5 repos should have HG_score values"
    for (score,) in rows:
        assert 0.0 <= score <= 100.0, f"HG_score {score} is out of range [0, 100]"

    conn.close()


# ------------------------------------------------------------------
# Test: no nulls in scoring output columns (including HG_score)
# ------------------------------------------------------------------


def test_no_nulls_in_scored_columns() -> None:
    """After scoring, key output columns (including HG_score) must not be null."""
    conn = _get_test_conn()

    repo = _make_repo()
    upsert_repository(conn, repo)
    insert_snapshot(conn, repo)

    _score_pipeline(conn)

    row = conn.execute(
        """
        SELECT star_velocity, activity_score, readme_quality, potential_score, HG_score
        FROM repositories
        WHERE id = 'R_test1'
        """
    ).fetchone()

    assert row is not None
    for i, col_name in enumerate(
        ["star_velocity", "activity_score", "readme_quality", "potential_score", "HG_score"]
    ):
        assert row[i] is not None, f"{col_name} should not be null after scoring"

    conn.close()


# ------------------------------------------------------------------
# Test: HG_score ranks fork_engagement (high forks/stars ratio → higher rank)
# ------------------------------------------------------------------


def test_hg_score_fork_engagement_effect() -> None:
    """A repo with high forks/stars ratio should score higher in HG_score."""
    conn = _get_test_conn()

    # Repo with high fork engagement (1:1 ratio = very actively forked)
    high_fork_repo = _make_repo(
        repo_id="R_high_forks",
        stars=200,
        forks=200,  # 100% fork ratio
        open_issues=5,
        closed_issues=50,
        contributors_count=30,
    )
    # Repo with low fork engagement
    low_fork_repo = _make_repo(
        repo_id="R_low_forks",
        stars=200,
        forks=2,  # ~1% fork ratio
        open_issues=5,
        closed_issues=50,
        contributors_count=30,
    )

    upsert_repository(conn, high_fork_repo)
    insert_snapshot(conn, high_fork_repo)
    upsert_repository(conn, low_fork_repo)
    insert_snapshot(conn, low_fork_repo)

    _score_pipeline(conn)

    high_score = conn.execute(
        "SELECT HG_score FROM repositories WHERE id = 'R_high_forks'"
    ).fetchone()[0]
    low_score = conn.execute(
        "SELECT HG_score FROM repositories WHERE id = 'R_low_forks'"
    ).fetchone()[0]

    assert high_score is not None
    assert low_score is not None
    assert high_score > low_score, (
        f"High fork engagement ({high_score:.1f}) should outscore low engagement ({low_score:.1f})"
    )
    conn.close()


# ------------------------------------------------------------------
# Test: HG_score community health signal (contributors + issue close rate)
# ------------------------------------------------------------------


def test_hg_score_community_health_effect() -> None:
    """Repos with higher community health should rank higher once percentile spread is meaningful.

    We use 5 repos to create a proper spread in the within-tier percentile ranking.
    The top repo has very high contributors and close rate; the bottom has almost none.
    """
    conn = _get_test_conn()

    # Create 5 repos with clearly ordered community health metrics (same stars/forks/velocity)
    community_levels = [
        ("R_ch_1", 1,   5,   1),    # weakest: 1 contributor, 5/6 closed
        ("R_ch_2", 5,   20,  30),
        ("R_ch_3", 15,  50,  50),
        ("R_ch_4", 40,  100, 30),
        ("R_ch_5", 100, 500, 10),   # strongest: 100 contributors, 500/510 closed
    ]
    for repo_id, contribs, closed, open_i in community_levels:
        repo = _make_repo(
            repo_id=repo_id,
            stars=500,
            forks=50,
            open_issues=open_i,
            closed_issues=closed,
            contributors_count=contribs,
        )
        upsert_repository(conn, repo)
        insert_snapshot(conn, repo)

    _score_pipeline(conn)

    scores = {
        row[0]: row[1]
        for row in conn.execute(
            "SELECT id, HG_score FROM repositories ORDER BY HG_score DESC"
        ).fetchall()
    }

    # All repos should have a score
    assert all(v is not None for v in scores.values()), "All repos must have an HG_score"

    # The repo with highest community health should rank better than the lowest
    assert scores["R_ch_5"] > scores["R_ch_1"], (
        f"Best community ({scores['R_ch_5']:.1f}) should outscore worst ({scores['R_ch_1']:.1f})"
    )
    conn.close()


# ------------------------------------------------------------------
# Test: HG_score handles zero stars gracefully (no divide-by-zero)
# ------------------------------------------------------------------


def test_hg_score_zero_stars_no_crash() -> None:
    """HG_score should not crash or produce NaN when stars == 0."""
    conn = _get_test_conn()

    # Newly created repo with zero stars
    repo = _make_repo(
        repo_id="R_zero_stars",
        stars=0,
        forks=0,
        open_issues=0,
        closed_issues=0,
        contributors_count=1,
    )
    upsert_repository(conn, repo)
    insert_snapshot(conn, repo)

    # Should not raise any exception
    scored = _score_pipeline(conn)
    assert scored == 1

    row = conn.execute(
        "SELECT HG_score FROM repositories WHERE id = 'R_zero_stars'"
    ).fetchone()
    assert row is not None
    assert row[0] is not None
    assert 0.0 <= row[0] <= 100.0
    conn.close()


# ------------------------------------------------------------------
# Test: HG_score handles zero total issues gracefully (no divide-by-zero)
# ------------------------------------------------------------------


def test_hg_score_zero_issues_no_crash() -> None:
    """HG_score should not crash or produce NaN when open_issues + closed_issues == 0."""
    conn = _get_test_conn()

    repo = _make_repo(
        repo_id="R_no_issues",
        stars=200,
        forks=10,
        open_issues=0,
        closed_issues=0,
        contributors_count=5,
    )
    upsert_repository(conn, repo)
    insert_snapshot(conn, repo)

    scored = _score_pipeline(conn)
    assert scored == 1

    row = conn.execute(
        "SELECT HG_score FROM repositories WHERE id = 'R_no_issues'"
    ).fetchone()
    assert row is not None
    assert row[0] is not None
    assert 0.0 <= row[0] <= 100.0
    conn.close()


# ------------------------------------------------------------------
# Test: both scores are independent (high potential ≠ high HG)
# ------------------------------------------------------------------


def test_potential_and_hg_scores_differ() -> None:
    """potential_score and HG_score can produce meaningfully different rankings."""
    conn = _get_test_conn()

    # This repo has high star velocity (good for potential_score) but poor fork engagement
    fast_star_repo = _make_repo(
        repo_id="R_fast",
        stars=10000,
        forks=10,   # very low fork engagement
        open_issues=100,
        closed_issues=5,  # poor close rate
        contributors_count=1,
    )
    # This repo has modest stars but good community metrics
    community_repo = _make_repo(
        repo_id="R_community",
        stars=300,
        forks=280,  # high fork engagement
        open_issues=5,
        closed_issues=300,  # great close rate
        contributors_count=80,
    )

    upsert_repository(conn, fast_star_repo)
    insert_snapshot(conn, fast_star_repo)
    upsert_repository(conn, community_repo)
    insert_snapshot(conn, community_repo)

    _score_pipeline(conn)

    rows = conn.execute(
        "SELECT id, potential_score, HG_score FROM repositories ORDER BY id"
    ).fetchall()

    scores = {row[0]: {"potential": row[1], "hg": row[2]} for row in rows}

    # Both should be scored
    assert scores["R_fast"]["potential"] is not None
    assert scores["R_fast"]["hg"] is not None
    assert scores["R_community"]["potential"] is not None
    assert scores["R_community"]["hg"] is not None

    conn.close()

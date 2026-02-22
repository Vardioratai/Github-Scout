"""Feature-engineering helpers for the Polars scoring pipeline."""

from __future__ import annotations

import duckdb
import polars as pl

__all__: list[str] = ["minmax_norm", "load_momentum_7d"]


def minmax_norm(col: str) -> pl.Expr:
    """Min-max normalise a column to the range ``[0, 1]``.

    Args:
        col: Column name to normalise.

    Returns:
        A Polars expression.
    """
    return (pl.col(col) - pl.col(col).min()) / (
        (pl.col(col).max() - pl.col(col).min()).clip(lower_bound=1e-9)
    )


def load_momentum_7d(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Compute 7-day star momentum from snapshots.

    Returns a DataFrame with columns ``repo_id`` and ``momentum_7d``.

    Args:
        conn: An open DuckDB connection.

    Returns:
        Polars DataFrame with one row per ``repo_id``.
    """
    query = """
    WITH current_stars AS (
        SELECT repo_id, stars
        FROM repo_snapshots
        WHERE snapshot_at = (
            SELECT MAX(snapshot_at) FROM repo_snapshots s2
            WHERE s2.repo_id = repo_snapshots.repo_id
        )
    ),
    stars_7d_ago AS (
        SELECT repo_id, stars
        FROM repo_snapshots
        WHERE snapshot_at >= current_timestamp - INTERVAL '7 days'
        AND snapshot_at = (
            SELECT MIN(snapshot_at) FROM repo_snapshots s3
            WHERE s3.repo_id = repo_snapshots.repo_id
            AND s3.snapshot_at >= current_timestamp - INTERVAL '7 days'
        )
    )
    SELECT
        c.repo_id,
        CASE WHEN s.stars IS NOT NULL AND s.stars > 0
             THEN (c.stars - s.stars)::DOUBLE / s.stars
             ELSE 0.0
        END AS momentum_7d
    FROM current_stars c
    LEFT JOIN stars_7d_ago s ON c.repo_id = s.repo_id
    """
    result = conn.execute(query).fetchall()
    if not result:
        return pl.DataFrame({"repo_id": [], "momentum_7d": []}).cast(
            {"repo_id": pl.Utf8, "momentum_7d": pl.Float64}
        )
    return pl.DataFrame(
        {"repo_id": [r[0] for r in result], "momentum_7d": [r[1] for r in result]}
    )

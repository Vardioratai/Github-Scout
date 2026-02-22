"""Composite potential-score computation using Polars."""

from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import polars as pl
from loguru import logger

from github_scout.database.connection import get_connection
from github_scout.database.schema import create_tables
from github_scout.scoring.features import load_momentum_7d, minmax_norm

__all__: list[str] = ["compute_scores"]


def compute_scores(db_path: str) -> int:
    """Run the full scoring pipeline and write results back to DuckDB.

    Args:
        db_path: Path to the DuckDB database file.

    Returns:
        Number of repositories scored.
    """
    with get_connection(db_path) as conn:
        create_tables(conn)
        return _score_pipeline(conn)


def _score_pipeline(conn: duckdb.DuckDBPyConnection) -> int:
    """Execute the Polars scoring pipeline.

    Args:
        conn: An open DuckDB connection.

    Returns:
        Number of repositories scored.
    """
    # Load repos into Polars
    rows = conn.execute("SELECT * FROM repositories").fetchall()
    if not rows:
        logger.info("No repositories to score.")
        return 0

    columns = [desc[0] for desc in conn.description]  # type: ignore[union-attr]
    df = pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    )

    # Load momentum data
    momentum_df = load_momentum_7d(conn)

    # Join momentum
    if not momentum_df.is_empty():
        df = df.join(
            momentum_df.rename({"repo_id": "id"}),
            on="id",
            how="left",
            suffix="_snap",
        )
        # Use snapshot momentum if available, otherwise keep existing
        if "momentum_7d_snap" in df.columns:
            df = df.with_columns(
                pl.coalesce(["momentum_7d_snap", "momentum_7d"]).alias("momentum_7d")
            ).drop("momentum_7d_snap")

    now = datetime.now(tz=timezone.utc)

    # 1. Feature engineering
    df = df.with_columns(
        pl.col("created_at").cast(pl.Datetime("us", "UTC"), strict=False).alias("created_at_dt")
    )

    df = df.with_columns(
        pl.when(pl.col("created_at_dt").is_not_null())
        .then(
            (pl.lit(now) - pl.col("created_at_dt")).dt.total_days().clip(lower_bound=1)
        )
        .otherwise(pl.lit(1.0))
        .alias("days_since_creation")
    )

    df = df.with_columns(
        [
            (
                pl.col("stars").cast(pl.Float64)
                / pl.col("days_since_creation").clip(lower_bound=1)
            ).alias("star_velocity"),
            pl.when(pl.col("days_since_creation") <= 90)
            .then(1.0)
            .otherwise((-pl.col("days_since_creation") / 90.0).exp())
            .alias("recency_decay"),
            (
                pl.col("forks").cast(pl.Float64)
                + pl.col("open_issues").cast(pl.Float64)
                + pl.col("contributors_count").fill_null(0).cast(pl.Float64)
            )
            .log1p()
            .alias("raw_activity"),
            (
                (pl.col("readme_length_chars").fill_null(0) > 500)
                .cast(pl.Float64)
                * 0.2
                + (pl.col("readme_h2_sections").fill_null(0) > 3)
                .cast(pl.Float64)
                * 0.2
                + pl.col("readme_has_badges")
                .fill_null(False)
                .cast(pl.Float64)
                * 0.2
                + pl.col("readme_has_demo_gif")
                .fill_null(False)
                .cast(pl.Float64)
                * 0.2
                + pl.col("readme_has_install")
                .fill_null(False)
                .cast(pl.Float64)
                * 0.2
            ).alias("readme_quality"),
        ]
    )

    # 2. Ensure momentum_7d exists
    if "momentum_7d" not in df.columns:
        df = df.with_columns(pl.lit(0.0).alias("momentum_7d"))
    else:
        df = df.with_columns(pl.col("momentum_7d").fill_null(0.0))

    # 3. Final composite score  (0-100)
    df = df.with_columns(
        [
            (
                100.0
                * (
                    0.35 * minmax_norm("star_velocity")
                    + 0.20 * pl.col("recency_decay")
                    + 0.20 * minmax_norm("raw_activity")
                    + 0.15 * minmax_norm("momentum_7d").fill_null(0.0)
                    + 0.10 * pl.col("readme_quality")
                )
            )
            .clip(lower_bound=0.0, upper_bound=100.0)
            .alias("potential_score"),
            # Also store the intermediate activity_score
            minmax_norm("raw_activity").alias("activity_score"),
        ]
    )

    # 4. Write scores back to DuckDB
    scored_count = 0
    for row in df.iter_rows(named=True):
        conn.execute(
            """
            UPDATE repositories SET
                star_velocity   = $2,
                momentum_7d     = $3,
                activity_score  = $4,
                readme_quality  = $5,
                potential_score = $6,
                updated_in_db_at = current_timestamp
            WHERE id = $1
            """,
            [
                row["id"],
                row.get("star_velocity"),
                row.get("momentum_7d"),
                row.get("activity_score"),
                row.get("readme_quality"),
                row.get("potential_score"),
            ],
        )
        scored_count += 1

    logger.success("Scored {} repositories.", scored_count)
    return scored_count

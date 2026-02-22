"""Typer CLI application — entry point for ``github-scout``."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Optional

import typer
from loguru import logger
from rich.console import Console
from rich.table import Table

from github_scout.analytics.queries import (
    LANGUAGE_DISTRIBUTION,
    SCORE_DISTRIBUTION,
    STAR_VELOCITY_PERCENTILES,
    TOP_POTENTIAL_REPOS,
    TOPIC_HEATMAP,
    TRENDING_7D,
)
from github_scout.config.settings import Settings
from github_scout.database.connection import get_connection
from github_scout.database.schema import create_tables
from github_scout.scoring.scorer import compute_scores

__all__: list[str] = ["app"]

app = typer.Typer(
    name="github-scout",
    help="GitHub repository intelligence spider with analytical scoring.",
    add_completion=False,
)
console = Console()


def _get_settings() -> Settings:
    """Load settings, logging validation errors clearly."""
    try:
        return Settings()  # type: ignore[call-arg]
    except Exception as exc:
        console.print(f"[red bold]Configuration error:[/] {exc}")
        raise typer.Exit(1) from exc


def _configure_logging(level: str) -> None:
    """Configure loguru to the requested level."""
    logger.remove()
    logger.add(sys.stderr, level=level.upper())


# ------------------------------------------------------------------
# crawl
# ------------------------------------------------------------------


@app.command()
def crawl(
    query: Optional[str] = typer.Option(None, "--query", "-q", help="Search query override"),
    max_pages: Optional[int] = typer.Option(None, "--max-pages", "-p", help="Max pages to fetch"),
) -> None:
    """Crawl GitHub for repositories matching the search query."""
    settings = _get_settings()
    _configure_logging(settings.log_level)

    from github_scout.crawler.spider import run_crawl

    with console.status("[bold green]Crawling GitHub…"):
        run_model = asyncio.run(run_crawl(settings, query=query, max_pages=max_pages))

    table = Table(title="Crawl Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Run ID", run_model.run_id)
    table.add_row("Status", run_model.status)
    table.add_row("Repos found", str(run_model.repos_found))
    table.add_row("New repos", str(run_model.repos_new))
    table.add_row("Updated repos", str(run_model.repos_updated))
    table.add_row("Errors", str(run_model.errors_count))
    console.print(table)


# ------------------------------------------------------------------
# score
# ------------------------------------------------------------------


@app.command()
def score() -> None:
    """Compute potential scores for all repositories."""
    settings = _get_settings()
    _configure_logging(settings.log_level)

    with console.status("[bold green]Scoring…"):
        scored = compute_scores(str(settings.db_path))

    console.print(f"[green]Scored {scored} repositories.[/]")


# ------------------------------------------------------------------
# top
# ------------------------------------------------------------------


@app.command()
def top(
    limit: int = typer.Option(20, "--limit", "-n", help="Number of repos to show"),
) -> None:
    """Show the top repositories by potential score."""
    settings = _get_settings()
    _configure_logging(settings.log_level)

    with get_connection(settings.db_path) as conn:
        create_tables(conn)
        rows = conn.execute(TOP_POTENTIAL_REPOS).fetchall()

    if not rows:
        console.print("[yellow]No scored repositories found. Run 'crawl' then 'score' first.[/]")
        return

    table = Table(title=f"Top {limit} Repositories by Potential Score")
    table.add_column("Repo", style="cyan", no_wrap=True)
    table.add_column("Lang", style="magenta")
    table.add_column("⭐", justify="right")
    table.add_column("Score", justify="right", style="green bold")
    table.add_column("Velocity", justify="right")
    table.add_column("README", justify="right")

    for row in rows[:limit]:
        table.add_row(
            str(row[0]),
            str(row[1] or "—"),
            str(row[2]),
            f"{row[9]:.1f}" if row[9] is not None else "—",
            f"{row[6]:.2f}" if row[6] is not None else "—",
            f"{row[8]:.2f}" if row[8] is not None else "—",
        )

    console.print(table)


# ------------------------------------------------------------------
# stats
# ------------------------------------------------------------------


@app.command()
def stats() -> None:
    """Display aggregate statistics and analytics."""
    settings = _get_settings()
    _configure_logging(settings.log_level)

    with get_connection(settings.db_path) as conn:
        create_tables(conn)

        # Language distribution
        lang_rows = conn.execute(LANGUAGE_DISTRIBUTION).fetchall()
        if lang_rows:
            t = Table(title="Language Distribution")
            t.add_column("Language", style="cyan")
            t.add_column("Count", justify="right")
            t.add_column("Avg ⭐", justify="right")
            t.add_column("Avg Score", justify="right", style="green")
            for r in lang_rows[:15]:
                t.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]))
            console.print(t)

        # Topic heatmap
        topic_rows = conn.execute(TOPIC_HEATMAP).fetchall()
        if topic_rows:
            t = Table(title="Topic Heatmap (top 20)")
            t.add_column("Topic", style="cyan")
            t.add_column("Count", justify="right")
            t.add_column("Avg Score", justify="right", style="green")
            for r in topic_rows[:20]:
                t.add_row(str(r[0]), str(r[1]), str(r[2]))
            console.print(t)

        # Star velocity percentiles
        pct_rows = conn.execute(STAR_VELOCITY_PERCENTILES).fetchall()
        if pct_rows and pct_rows[0][0] is not None:
            t = Table(title="Star Velocity Percentiles")
            t.add_column("Percentile", style="cyan")
            t.add_column("Value", justify="right", style="green")
            t.add_row("p50", f"{pct_rows[0][0]:.4f}")
            t.add_row("p90", f"{pct_rows[0][1]:.4f}")
            t.add_row("p99", f"{pct_rows[0][2]:.4f}")
            console.print(t)

        # Score distribution
        dist_rows = conn.execute(SCORE_DISTRIBUTION).fetchall()
        if dist_rows:
            t = Table(title="Score Distribution (histogram)")
            t.add_column("Bucket", style="cyan")
            t.add_column("Count", justify="right", style="green")
            for r in dist_rows:
                bucket_label = f"{r[0] * 10}–{(r[0] + 1) * 10}"
                t.add_row(bucket_label, str(r[1]))
            console.print(t)

        # Trending 7d
        trend_rows = conn.execute(TRENDING_7D).fetchall()
        if trend_rows:
            t = Table(title="Trending (7-day momentum)")
            t.add_column("Repo", style="cyan")
            t.add_column("Δ Stars", justify="right", style="green")
            t.add_column("Momentum", justify="right")
            for r in trend_rows[:10]:
                t.add_row(
                    str(r[0]),
                    str(r[3]),
                    f"{r[4]:.4f}" if r[4] is not None else "—",
                )
            console.print(t)


# ------------------------------------------------------------------
# export
# ------------------------------------------------------------------


@app.command()
def export(
    output: Path = typer.Option(
        Path("./github_scout_export.csv"),
        "--output",
        "-o",
        help="Output file path (.csv or .parquet)",
    ),
) -> None:
    """Export scored repositories to CSV or Parquet."""
    settings = _get_settings()
    _configure_logging(settings.log_level)

    import polars as pl

    with get_connection(settings.db_path) as conn:
        create_tables(conn)
        rows = conn.execute(
            "SELECT * FROM repositories ORDER BY potential_score DESC NULLS LAST"
        ).fetchall()
        if not rows:
            console.print("[yellow]No data to export.[/]")
            return
        columns = [desc[0] for desc in conn.description]  # type: ignore[union-attr]

    df = pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    )

    suffix = output.suffix.lower()
    if suffix == ".parquet":
        df.write_parquet(str(output))
    else:
        # Flatten list columns (e.g. topics) to comma-separated strings for CSV
        if "topics" in df.columns:
            df = df.with_columns(
                pl.col("topics").list.join(", ").alias("topics")
            )
        df.write_csv(str(output))

    console.print(f"[green]Exported {len(df)} repos to {output}[/]")


if __name__ == "__main__":
    app()

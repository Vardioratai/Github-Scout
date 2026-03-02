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
    MATURITY_TIER_SUMMARY,
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
    force_refresh: bool = typer.Option(
        False, "--force-refresh",
        help="Re-fetch and re-enrich all repos regardless of TTL",
    ),
    refresh_ttl: Optional[int] = typer.Option(
        None, "--refresh-ttl",
        help="Hours before a repo is considered stale (default: 24)",
    ),
) -> None:
    """Crawl GitHub for repositories matching the search query."""
    settings = _get_settings()
    _configure_logging(settings.log_level)

    # Apply CLI overrides to settings
    overrides: dict = {}
    if force_refresh:
        overrides["force_refresh"] = True
    if refresh_ttl is not None:
        overrides["refresh_ttl_hours"] = refresh_ttl
    if overrides:
        settings = settings.model_copy(update=overrides)

    from github_scout.crawler.spider import run_crawl

    asyncio.run(run_crawl(settings, query=query, max_pages=max_pages))


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
    
    if scored > 0:
        with get_connection(settings.db_path) as conn:
            tier_rows = conn.execute(MATURITY_TIER_SUMMARY).fetchall()
            if tier_rows:
                console.print()
                table = Table(title="Maturity Tier Summary (Current Dataset)")
                table.add_column("Tier", style="cyan")
                table.add_column("Count", justify="right")
                table.add_column("Avg Score", justify="right", style="green")
                table.add_column("Avg HG Score", justify="right", style="green bold")
                table.add_column("Avg Star Velocity", justify="right")
                table.add_column("Avg Stars", justify="right")
                for r in tier_rows:
                    table.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4]), str(r[5]))
                console.print(table)


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

    # Group by tier
    tiers = {"Scale": [], "Traction": [], "Seed": []}
    for row in rows:
        tier = str(row[13])  # maturity_tier is the 14th column (index 13)
        if tier in tiers:
            tiers[tier].append(row)
        else:
            if "Unknown" not in tiers:
                tiers["Unknown"] = []
            tiers["Unknown"].append(row)

    for tier_name, tier_rows in tiers.items():
        if not tier_rows:
            continue
            
        table = Table(title=f"Top Repositories by Potential Score ({tier_name} Tier)")
        table.add_column("Repo", style="cyan", no_wrap=True)
        table.add_column("Lang", style="magenta")
        table.add_column("Stars", justify="right")
        table.add_column("Score", justify="right", style="green")
        table.add_column("HG Score", justify="right", style="green bold")
        table.add_column("Velocity", justify="right")
        table.add_column("README", justify="right")

        for row in tier_rows[:limit]:
            table.add_row(
                str(row[0]),
                str(row[1] or "—"),
                str(row[2]),
                f"{row[9]:.1f}" if row[9] is not None else "—",
                f"{row[10]:.1f}" if row[10] is not None else "—",
                f"{row[6]:.2f}" if row[6] is not None else "—",
                f"{row[8]:.2f}" if row[8] is not None else "—",
            )

        console.print(table)
        console.print()


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

        # Maturity tier summary
        tier_rows = conn.execute(MATURITY_TIER_SUMMARY).fetchall()
        if tier_rows:
            table = Table(title="Maturity Tier Analysis")
            table.add_column("Tier", style="cyan")
            table.add_column("Count", justify="right")
            table.add_column("Avg Score", justify="right", style="green")
            table.add_column("Avg HG Score", justify="right", style="green bold")
            table.add_column("Avg Star Velocity", justify="right")
            table.add_column("Avg Stars", justify="right")
            for r in tier_rows:
                table.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4]), str(r[5]))
            console.print(table)
            console.print()

        # Language distribution
        lang_rows = conn.execute(LANGUAGE_DISTRIBUTION).fetchall()
        if lang_rows:
            t = Table(title="Language Distribution")
            t.add_column("Language", style="cyan")
            t.add_column("Count", justify="right")
            t.add_column("Avg Stars", justify="right")
            t.add_column("Avg Score", justify="right", style="green")
            t.add_column("Avg HG Score", justify="right", style="green bold")
            for r in lang_rows[:15]:
                t.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]), str(r[4]))
            console.print(t)

        # Topic heatmap
        topic_rows = conn.execute(TOPIC_HEATMAP).fetchall()
        if topic_rows:
            t = Table(title="Topic Heatmap (top 20)")
            t.add_column("Topic", style="cyan")
            t.add_column("Count", justify="right")
            t.add_column("Avg Score", justify="right", style="green")
            t.add_column("Avg HG Score", justify="right", style="green bold")
            for r in topic_rows[:20]:
                t.add_row(str(r[0]), str(r[1]), str(r[2]), str(r[3]))
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
            t = Table(title="Score Distribution by Maturity Tier")
            t.add_column("Bucket", style="cyan")
            t.add_column("Total", justify="right", style="green")
            t.add_column("Seed", justify="right")
            t.add_column("Traction", justify="right")
            t.add_column("Scale", justify="right")
            for r in dist_rows:
                bucket_label = f"{r[0] * 10}–{(r[0] + 1) * 10}"
                t.add_row(bucket_label, str(r[1]), str(r[2]), str(r[3]), str(r[4]))
            console.print(t)

        # Trending 7d - Split by tier
        trend_rows = conn.execute(TRENDING_7D).fetchall()
        if trend_rows:
            # Group rows by tier
            tiers = {"Scale": [], "Traction": [], "Seed": []}
            for r in trend_rows:
                tier = str(r[1])
                if tier in tiers:
                    tiers[tier].append(r)
                else:
                    # E.g. Unknown
                    if "Unknown" not in tiers:
                        tiers["Unknown"] = []
                    tiers["Unknown"].append(r)

            for tier_name, rows in tiers.items():
                if not rows:
                    continue
                t = Table(title=f"Trending 7d ({tier_name} Tier)")
                t.add_column("Repo", style="cyan")
                t.add_column("Δ Stars", justify="right", style="green")
                t.add_column("Momentum", justify="right")
                
                # Show top 5 for each tier instead of 10 globally to keep it concise
                for r in rows[:5]:
                    t.add_row(
                        str(r[0]),
                        str(r[4]),
                        f"{r[5]:.4f}" if r[5] is not None else "—",
                    )
                console.print(t)
                console.print()


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


# ------------------------------------------------------------------
# clean
# ------------------------------------------------------------------


@app.command()
def clean(
    all_data: bool = typer.Option(
        False, "--all",
        help="Truncate repositories, repo_snapshots, and reset crawl_runs",
    ),
    before: Optional[str] = typer.Option(
        None, "--before",
        help="Delete repos where scraped_at < YYYY-MM-DD (ISO format)",
    ),
    score_below: Optional[float] = typer.Option(
        None, "--score-below",
        help="Delete repos with potential_score < threshold",
    ),
    archived: bool = typer.Option(
        False, "--archived",
        help="Delete all repos where is_archived = true",
    ),
    forks: bool = typer.Option(
        False, "--forks",
        help="Delete all repos where is_fork = true",
    ),
    language: Optional[str] = typer.Option(
        None, "--language",
        help="Delete repos by primary_language (case-insensitive)",
    ),
    orphan_snapshots: bool = typer.Option(
        False, "--orphan-snapshots",
        help="Delete repo_snapshots with no matching repository",
    ),
    dry_run: bool = typer.Option(
        True, "--dry-run/--execute",
        help="Default is dry-run. Use --execute to apply changes.",
    ),
    yes: bool = typer.Option(
        False, "--yes", "-y",
        help="Skip confirmation prompt (for automation)",
    ),
) -> None:
    """Clean the DuckDB database by applying one or more filter conditions.

    Dry-run mode is ON by default.  Pass ``--execute`` to apply.
    Conditions are combined with AND.
    """
    from rich.panel import Panel

    from github_scout.database.dao import (
        count_orphan_snapshots,
        count_repos_matching,
        delete_orphan_snapshots,
        delete_repos,
    )

    settings = _get_settings()
    _configure_logging(settings.log_level)

    # --- validate that at least one filter is active -----------------
    has_filter = any([
        all_data, before, score_below is not None, archived,
        forks, language, orphan_snapshots,
    ])
    if not has_filter:
        console.print(
            "[yellow]No filter specified. "
            "Use --help to see available options.[/]"
        )
        raise typer.Exit(0)

    # --- build filter dict -------------------------------------------
    filters: dict = {}
    if all_data:
        filters["all_data"] = True
    if before:
        filters["before"] = before
    if score_below is not None:
        filters["score_below"] = score_below
    if archived:
        filters["archived"] = True
    if forks:
        filters["forks"] = True
    if language:
        filters["language"] = language

    with get_connection(settings.db_path) as conn:
        create_tables(conn)

        # ── compute preview counts ──────────────────────────────────
        if all_data:
            row = conn.execute("SELECT COUNT(*) FROM repositories").fetchone()
            n_repos = int(row[0]) if row else 0
            row = conn.execute("SELECT COUNT(*) FROM repo_snapshots").fetchone()
            n_snaps = int(row[0]) if row else 0
        elif filters:
            n_repos = count_repos_matching(conn, filters)
            # estimate snapshots that would become orphaned
            n_snaps = conn.execute(
                "SELECT COUNT(*) FROM repo_snapshots rs "
                "WHERE rs.repo_id IN ("
                "  SELECT id FROM repositories WHERE "
                + (
                    " AND ".join(
                        c
                        for c in [
                            f"scraped_at < '{before}'::TIMESTAMPTZ" if before else "",
                            f"(potential_score < {score_below} OR potential_score IS NULL)"
                            if score_below is not None
                            else "",
                            "is_archived = true" if archived else "",
                            "is_fork = true" if forks else "",
                            f"lower(primary_language) = lower('{language}')"
                            if language
                            else "",
                        ]
                        if c
                    )
                )
                + ")"
            ).fetchone()
            n_snaps = int(n_snaps[0]) if n_snaps else 0
        else:
            n_repos = 0
            n_snaps = 0

        n_orphan = count_orphan_snapshots(conn) if orphan_snapshots else 0

        # ── dry-run: show preview panel ──────────────────────────────
        if dry_run:
            lines = ["[bold]Dry-run preview - no data will be modified.[/]\n"]
            if filters:
                lines.append(f"  Repositories to delete : [cyan]{n_repos}[/]")
                lines.append(f"  Cascaded snapshots     : [cyan]{n_snaps}[/]")
            if orphan_snapshots:
                lines.append(f"  Orphan snapshots       : [cyan]{n_orphan}[/]")
            lines.append("\nRe-run with [green]--execute[/] to apply.")
            console.print(Panel("\n".join(lines), title="Clean Preview", border_style="blue"))
            return

        # ── execute mode ─────────────────────────────────────────────
        total_affected = n_repos + n_snaps + n_orphan
        if total_affected == 0:
            console.print("[yellow]Nothing to delete - no rows match the filters.[/]")
            return

        if not yes:
            console.print(
                f"\n[red bold]WARNING[/red bold]: This will delete "
                f"[cyan]{n_repos}[/] repos, [cyan]{n_snaps}[/] cascaded snapshots"
                + (f", and [cyan]{n_orphan}[/] orphan snapshots" if orphan_snapshots else "")
                + "."
            )
            confirmation = typer.prompt("Type 'confirm' to proceed")
            if confirmation != "confirm":
                console.print("[yellow]Aborted.[/]")
                raise typer.Exit(0)

        # Perform deletions
        deleted_repos = 0
        deleted_snaps = 0
        if filters:
            deleted_repos, deleted_snaps = delete_repos(conn, filters)
        if orphan_snapshots:
            deleted_snaps += delete_orphan_snapshots(conn)

        console.print(
            f"[green]Done.[/] Deleted [cyan]{deleted_repos}[/] repos "
            f"and [cyan]{deleted_snaps}[/] snapshots."
        )


if __name__ == "__main__":
    app()

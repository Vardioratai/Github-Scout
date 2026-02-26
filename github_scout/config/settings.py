"""Application settings loaded from environment / .env file."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

__all__: list[str] = ["Settings"]


class Settings(BaseSettings):
    """Central configuration for GitHub Scout.

    All values can be overridden via environment variables or a ``.env`` file
    located in the working directory.
    """

    github_token: str = Field(..., description="GitHub PAT (required)")
    db_path: Path = Field(
        default=Path("./github_scout.duckdb"),
        description="Path to DuckDB database file",
    )
    log_level: str = Field(default="INFO", description="Loguru log level")
    max_concurrent_enrichments: int = Field(
        default=10,
        description="Max concurrent REST enrichment requests",
    )
    graphql_endpoint: str = Field(
        default="https://api.github.com/graphql",
        description="GitHub GraphQL API endpoint",
    )
    rest_base_url: str = Field(
        default="https://api.github.com",
        description="GitHub REST API base URL",
    )
    default_query: str = Field(
        default=(
            'language:python language:"Jupyter Notebook" '
            "stars:>100 created:>2026-01-01"
        ),
        description="Default GitHub search query",
    )
    crawl_page_size: int = Field(
        default=100,
        description="Results per GraphQL search page (max 100)",
    )
    max_pages: int | None = Field(
        default=None,
        description="Maximum number of pages to paginate (None = unlimited)",
    )
    refresh_ttl_hours: int = Field(
        default=24,
        description="Hours before a repo is considered stale for re-enrichment",
    )
    snapshot_ttl_hours: int = Field(
        default=6,
        description="Minimum hours between repo snapshots (avoids snapshot spam)",
    )
    force_refresh: bool = Field(
        default=False,
        description="Force full re-enrichment on all repos regardless of TTL",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        secrets_dir=None,
    )

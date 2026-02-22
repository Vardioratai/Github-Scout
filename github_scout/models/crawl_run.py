"""Pydantic model for crawl-run metadata."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

__all__: list[str] = ["CrawlRunModel"]


class CrawlRunModel(BaseModel):
    """Tracks metadata for a single crawl execution."""

    run_id: str
    query_string: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    repos_found: int = 0
    repos_new: int = 0
    repos_updated: int = 0
    errors_count: int = 0
    status: str = Field(default="running", pattern=r"^(running|completed|failed)$")

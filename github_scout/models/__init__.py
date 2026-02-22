"""Pydantic models sub-package."""

from github_scout.models.crawl_run import CrawlRunModel
from github_scout.models.repository import RepositoryModel

__all__: list[str] = ["RepositoryModel", "CrawlRunModel"]

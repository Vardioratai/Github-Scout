"""REST-based enrichment for repository metadata.

Fetches README content, contributor counts, and release info using the
GitHub REST v3 API, with a semaphore to cap concurrency.
"""

from __future__ import annotations

import asyncio
import base64
import re
from typing import Any

from loguru import logger

from github_scout.client.github_client import GitHubClient
from github_scout.config.settings import Settings
from github_scout.models.repository import RepositoryModel

__all__: list[str] = ["enrich_repos"]


async def enrich_repos(
    client: GitHubClient,
    settings: Settings,
    repos: list[RepositoryModel],
) -> list[RepositoryModel]:
    """Enrich a batch of repositories with REST API data.

    Uses ``asyncio.Semaphore`` to limit concurrent requests to
    ``settings.max_concurrent_enrichments``.

    Args:
        client: Authenticated ``GitHubClient``.
        settings: Application settings.
        repos: Repositories to enrich.

    Returns:
        The same list of repositories, mutated in-place with enrichment
        fields populated.
    """
    sem = asyncio.Semaphore(settings.max_concurrent_enrichments)

    async def _enrich_one(repo: RepositoryModel) -> None:
        async with sem:
            owner = repo.owner_login or ""
            name = repo.name
            await _enrich_readme(client, repo, owner, name)
            await _enrich_contributors(client, repo, owner, name)
            await _enrich_releases(client, repo, owner, name)

    tasks = [asyncio.create_task(_enrich_one(r)) for r in repos]
    await asyncio.gather(*tasks, return_exceptions=True)
    return repos


# ------------------------------------------------------------------
# README enrichment
# ------------------------------------------------------------------

async def _enrich_readme(
    client: GitHubClient,
    repo: RepositoryModel,
    owner: str,
    name: str,
) -> None:
    """Fetch and analyse the README for a repository.

    Args:
        client: Authenticated HTTP client.
        repo: Repository model to mutate.
        owner: Repository owner login.
        name: Repository name.
    """
    try:
        resp = await client.rest_get(f"/repos/{owner}/{name}/readme")
        data: dict[str, Any] = resp.json()
        content_b64 = data.get("content", "")
        text = base64.b64decode(content_b64).decode("utf-8", errors="replace")

        text_lower = text.lower()
        repo.readme_length_chars = len(text)
        repo.readme_h2_sections = len(re.findall(r"^## ", text, re.MULTILINE))
        repo.readme_has_badges = "[![" in text
        repo.readme_has_demo_gif = ".gif" in text_lower or "demo" in text_lower
        repo.readme_has_install = (
            "pip install" in text_lower or "conda install" in text_lower
        )
    except Exception:
        logger.debug("README fetch failed for {}/{}.", owner, name)


# ------------------------------------------------------------------
# Contributors enrichment
# ------------------------------------------------------------------

async def _enrich_contributors(
    client: GitHubClient,
    repo: RepositoryModel,
    owner: str,
    name: str,
) -> None:
    """Estimate total contributors from the ``Link`` header.

    Args:
        client: Authenticated HTTP client.
        repo: Repository model to mutate.
        owner: Repository owner login.
        name: Repository name.
    """
    try:
        resp = await client.rest_get(
            f"/repos/{owner}/{name}/contributors",
            params={"per_page": "1"},
        )
        repo.contributors_count = _parse_last_page(resp.headers.get("link", ""))
        if repo.contributors_count == 0:
            # If no Link header, count the items directly
            data = resp.json()
            if isinstance(data, list):
                repo.contributors_count = len(data)
    except Exception:
        logger.debug("Contributors fetch failed for {}/{}.", owner, name)


# ------------------------------------------------------------------
# Releases enrichment
# ------------------------------------------------------------------

async def _enrich_releases(
    client: GitHubClient,
    repo: RepositoryModel,
    owner: str,
    name: str,
) -> None:
    """Fetch the latest release and estimate total release count.

    Args:
        client: Authenticated HTTP client.
        repo: Repository model to mutate.
        owner: Repository owner login.
        name: Repository name.
    """
    try:
        resp = await client.rest_get(
            f"/repos/{owner}/{name}/releases",
            params={"per_page": "1"},
        )
        releases = resp.json()
        if isinstance(releases, list) and releases:
            latest = releases[0]
            repo.latest_release_tag = latest.get("tag_name")
            published = latest.get("published_at")
            if published:
                from datetime import datetime

                repo.latest_release_at = datetime.fromisoformat(
                    published.replace("Z", "+00:00")
                )
        repo.releases_count = _parse_last_page(resp.headers.get("link", ""))
        if repo.releases_count == 0 and isinstance(releases, list):
            repo.releases_count = len(releases)
    except Exception:
        logger.debug("Releases fetch failed for {}/{}.", owner, name)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _parse_last_page(link_header: str) -> int:
    """Extract the last page number from a GitHub ``Link`` header.

    Args:
        link_header: Raw value of the ``Link`` response header.

    Returns:
        The last page number, or ``0`` if it cannot be parsed.
    """
    match = re.search(r'[&?]page=(\d+)[^>]*>;\s*rel="last"', link_header)
    return int(match.group(1)) if match else 0

"""Tests for the HTTP client layer — pagination, retry logic, and model parsing."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx

from github_scout.client.github_client import GitHubClient
from github_scout.client.paginator import paginate_search
from github_scout.config.settings import Settings


def _make_settings(**overrides: Any) -> Settings:
    """Create a test Settings instance."""
    defaults = {
        "github_token": "ghp_test_token_fake",
        "db_path": ":memory:",
        "graphql_endpoint": "https://api.github.com/graphql",
        "rest_base_url": "https://api.github.com",
        "max_pages": 10,
        "crawl_page_size": 100,
    }
    defaults.update(overrides)
    return Settings(**defaults)  # type: ignore[arg-type]


def _graphql_response(
    nodes: list[dict],
    has_next: bool = False,
    end_cursor: str | None = "cursor123",
) -> dict:
    """Build a mock GraphQL search response."""
    return {
        "data": {
            "search": {
                "repositoryCount": len(nodes),
                "pageInfo": {
                    "endCursor": end_cursor,
                    "hasNextPage": has_next,
                },
                "nodes": nodes,
            },
            "rateLimit": {"remaining": 4999, "resetAt": "2099-01-01T00:00:00Z", "cost": 1},
        }
    }


def _fake_repo_node(name: str = "test-repo") -> dict:
    """Build a minimal valid repo node matching the updated GraphQL fragment (including closedIssues)."""
    return {
        "id": f"id_{name}",
        "name": name,
        "nameWithOwner": f"owner/{name}",
        "owner": {"login": "owner", "__typename": "User"},
        "description": "A test repository",
        "url": f"https://github.com/owner/{name}",
        "homepageUrl": None,
        "primaryLanguage": {"name": "Python"},
        "repositoryTopics": {"nodes": [{"topic": {"name": "testing"}}]},
        "licenseInfo": {"spdxId": "MIT"},
        "isArchived": False,
        "isFork": False,
        "isTemplate": False,
        "stargazerCount": 200,
        "forkCount": 30,
        "watchers": {"totalCount": 10},
        "issues": {"totalCount": 5},
        "closedIssues": {"totalCount": 40},
        "createdAt": "2026-01-15T00:00:00Z",
        "updatedAt": "2026-02-20T00:00:00Z",
        "pushedAt": "2026-02-20T00:00:00Z",
        "diskUsage": 1024,
        "defaultBranchRef": None,
    }


# ------------------------------------------------------------------
# Test: paginator stops when hasNextPage is false
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_paginator_stops_on_last_page() -> None:
    """Paginator must stop when ``hasNextPage`` is ``false``."""
    settings = _make_settings(max_pages=5)

    # Page 1 has data, page ends
    respx.post("https://api.github.com/graphql").mock(
        return_value=httpx.Response(
            200,
            json=_graphql_response(
                nodes=[_fake_repo_node("repo-1")],
                has_next=False,
            ),
        )
    )

    client = GitHubClient(settings)
    pages: list[list[dict]] = []
    async for page in paginate_search(client, settings):
        pages.append(page)
    await client.close()

    assert len(pages) == 1, "Should stop after first page when hasNextPage=false"
    assert len(pages[0]) == 1


# ------------------------------------------------------------------
# Test: 429 triggers retry
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_429_triggers_retry() -> None:
    """A 429 response must be retried (via tenacity)."""
    settings = _make_settings()

    call_count = 0

    def _side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(429, text="rate limited")
        return httpx.Response(
            200,
            json=_graphql_response(
                nodes=[_fake_repo_node()],
                has_next=False,
            ),
        )

    respx.post("https://api.github.com/graphql").mock(side_effect=_side_effect)

    client = GitHubClient(settings)
    pages: list[list[dict]] = []
    async for page in paginate_search(client, settings):
        pages.append(page)
    await client.close()

    assert call_count >= 3, "Should have retried at least twice before succeeding"
    assert len(pages) == 1


# ------------------------------------------------------------------
# Test: RepositoryModel.from_graphql parses closedIssues correctly
# ------------------------------------------------------------------


def test_model_parses_closed_issues() -> None:
    """RepositoryModel.from_graphql must correctly parse the closedIssues field."""
    from github_scout.models.repository import RepositoryModel

    node = _fake_repo_node("my-repo")
    model = RepositoryModel.from_graphql(node)

    assert model.closed_issues == 40, (
        f"Expected closed_issues=40, got {model.closed_issues}"
    )
    assert model.open_issues == 5, (
        f"Expected open_issues=5, got {model.open_issues}"
    )


# ------------------------------------------------------------------
# Test: RepositoryModel.from_graphql handles missing closedIssues gracefully
# ------------------------------------------------------------------


def test_model_handles_missing_closed_issues() -> None:
    """RepositoryModel.from_graphql must default closed_issues to 0 if not present."""
    from github_scout.models.repository import RepositoryModel

    node = _fake_repo_node("old-repo")
    # Simulate legacy GraphQL response without closedIssues key
    del node["closedIssues"]

    model = RepositoryModel.from_graphql(node)
    assert model.closed_issues == 0, (
        f"Expected closed_issues=0 when field missing, got {model.closed_issues}"
    )


# ------------------------------------------------------------------
# Test: RepositoryModel defaults
# ------------------------------------------------------------------


def test_model_defaults() -> None:
    """RepositoryModel must have correct defaults for open_issues and closed_issues."""
    from github_scout.models.repository import RepositoryModel

    model = RepositoryModel(id="R_x", name="x", full_name="owner/x")
    assert model.open_issues == 0
    assert model.closed_issues == 0

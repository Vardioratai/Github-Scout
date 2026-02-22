"""Shared async HTTP client for both GraphQL and REST calls."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from github_scout.client.rate_limiter import check_graphql_rate_limit, check_rate_limit
from github_scout.config.settings import Settings

__all__: list[str] = ["GitHubClient"]


class GitHubClient:
    """Wrapper around ``httpx.AsyncClient`` for GitHub API calls.

    Provides methods for GraphQL queries and REST GET requests, with
    automatic rate-limit handling and tenacity-based retry on transient
    server errors.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = httpx.AsyncClient(
            http2=True,
            headers={
                "Authorization": f"Bearer {settings.github_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            },
            timeout=httpx.Timeout(30.0),
        )

    async def close(self) -> None:
        """Gracefully close the underlying HTTP client."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # GraphQL
    # ------------------------------------------------------------------

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),  # type: ignore[arg-type]
        stop=stop_after_attempt(5),
    )
    async def graphql(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query against the GitHub v4 API.

        Args:
            query: The full GraphQL query string.
            variables: Optional mapping of query variables.

        Returns:
            Parsed JSON response body.

        Raises:
            httpx.HTTPStatusError: On 4xx/5xx responses (retried via
                tenacity for transient errors).
        """
        resp = await self._client.post(
            self._settings.graphql_endpoint,
            json={"query": query, "variables": variables or {}},
        )
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()
        await check_graphql_rate_limit(data.get("data", {}))
        return data

    # ------------------------------------------------------------------
    # REST
    # ------------------------------------------------------------------

    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.TransportError)),
        before_sleep=before_sleep_log(logger, logging.WARNING),  # type: ignore[arg-type]
        stop=stop_after_attempt(5),
    )
    async def rest_get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> httpx.Response:
        """Perform an authenticated GET against the GitHub REST v3 API.

        Args:
            path: URL path relative to ``rest_base_url`` (e.g.
                ``/repos/owner/repo/readme``).
            params: Optional query-string parameters.

        Returns:
            The full ``httpx.Response``.

        Raises:
            httpx.HTTPStatusError: Propagated after retry exhaustion.
        """
        url = f"{self._settings.rest_base_url}{path}"
        resp = await self._client.get(url, params=params)
        await check_rate_limit(resp)
        resp.raise_for_status()
        return resp

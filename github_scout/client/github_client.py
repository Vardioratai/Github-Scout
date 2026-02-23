"""Shared async HTTP client for both GraphQL and REST calls.

Retry policy aligned with GitHub API rate-limit documentation:
  - Transient 5xx and 429 are retried with exponential back-off.
  - 403 with ``retry-after`` (secondary rate limit) is retried after the
    indicated delay.
  - Other 4xx errors are NOT retried (they indicate request problems).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from github_scout.client.rate_limiter import check_graphql_rate_limit, check_rate_limit
from github_scout.config.settings import Settings

__all__: list[str] = ["GitHubClient"]


def _is_retryable(exc: BaseException) -> bool:
    """Return True if the exception warrants a retry.

    Retried:
      - ``httpx.TransportError`` (network-level failures).
      - 429 Too Many Requests (primary rate limit exceeded).
      - 403 with ``retry-after`` header (secondary rate limit).
      - 5xx Server Errors (transient infra problems, e.g. 502).
    NOT retried:
      - 400, 401, 404, 422 and other 4xx (client bugs).
    """
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status == 429:
            return True
        if status == 403 and exc.response.headers.get("retry-after"):
            return True
        if status >= 500:
            return True
    return False


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
            timeout=httpx.Timeout(60.0, connect=15.0),
        )

    async def close(self) -> None:
        """Gracefully close the underlying HTTP client."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # GraphQL
    # ------------------------------------------------------------------

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=120),
        retry=retry_if_exception(_is_retryable),
        before_sleep=before_sleep_log(logger, logging.WARNING),  # type: ignore[arg-type]
        stop=stop_after_attempt(8),
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
            httpx.HTTPStatusError: On non-retryable 4xx or exhausted
                retries for transient errors.
        """
        resp = await self._client.post(
            self._settings.graphql_endpoint,
            json={"query": query, "variables": variables or {}},
        )
        # If we got a 403/429 with retry-after, handle sleep before raising
        if resp.status_code in (403, 429):
            await check_rate_limit(resp)
            resp.raise_for_status()
        resp.raise_for_status()
        data: dict[str, Any] = resp.json()

        # Check for GraphQL-level errors (can indicate rate limit even on 200)
        errors = data.get("errors", [])
        for err in errors:
            msg = err.get("message", "")
            if "rate limit" in msg.lower():
                logger.warning("GraphQL rate-limit error in body: {}", msg)
                # Force a sleep from the rateLimit block
                await check_graphql_rate_limit(data.get("data", {}))

        await check_graphql_rate_limit(data.get("data", {}))
        return data

    # ------------------------------------------------------------------
    # REST
    # ------------------------------------------------------------------

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=120),
        retry=retry_if_exception(_is_retryable),
        before_sleep=before_sleep_log(logger, logging.WARNING),  # type: ignore[arg-type]
        stop=stop_after_attempt(7),
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

"""Async rate-limit handling for the GitHub API."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
from loguru import logger

__all__: list[str] = ["check_rate_limit"]


async def check_rate_limit(response: httpx.Response) -> None:
    """Inspect response headers and sleep if approaching a rate limit.

    Handles both primary and secondary (abuse) rate limits.

    Args:
        response: The ``httpx.Response`` to inspect.
    """
    remaining = response.headers.get("x-ratelimit-remaining")
    reset_epoch = response.headers.get("x-ratelimit-reset")
    used = response.headers.get("x-ratelimit-used")

    if remaining is not None and int(remaining) < 100 and reset_epoch is not None:
        reset_dt = datetime.fromtimestamp(int(reset_epoch), tz=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        wait_seconds = max((reset_dt - now).total_seconds() + 5, 1.0)
        logger.warning(
            "Rate-limit low (remaining={}). Sleeping {:.0f}s until reset.",
            remaining,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
        return

    if used is not None and int(used) > 4000:
        logger.info("High usage (used={}). Throttling 1s.", used)
        await asyncio.sleep(1.0)


async def check_graphql_rate_limit(data: dict) -> None:
    """Inspect GraphQL ``rateLimit`` payload and sleep if close to limit.

    Args:
        data: The parsed JSON body from a GraphQL response containing
            a ``rateLimit`` key.
    """
    rate = data.get("rateLimit")
    if rate is None:
        return

    remaining = rate.get("remaining", 5000)
    reset_at = rate.get("resetAt")

    if remaining < 100 and reset_at:
        reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
        now = datetime.now(tz=timezone.utc)
        wait_seconds = max((reset_dt - now).total_seconds() + 5, 1.0)
        logger.warning(
            "GraphQL rate-limit low (remaining={}). Sleeping {:.0f}s.",
            remaining,
            wait_seconds,
        )
        await asyncio.sleep(wait_seconds)

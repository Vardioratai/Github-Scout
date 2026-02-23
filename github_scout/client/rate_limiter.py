"""Async rate-limit handling for the GitHub API.

Implements the rate-limit policies documented at:
  - REST:    https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api
  - GraphQL: https://docs.github.com/en/graphql/overview/rate-limits-and-node-limits-for-the-graphql-api

Key policies respected:
  - Primary limit (5 000 req/hour REST, 5 000 points/hour GraphQL for PATs)
  - Secondary limit (100 concurrent, 900 points/min REST, 2 000 points/min GraphQL)
  - ``retry-after`` header on secondary 403/429 responses
  - ``x-ratelimit-reset`` epoch for primary exhaustion
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import httpx
from loguru import logger
from rich.console import Console

__all__: list[str] = [
    "check_rate_limit",
    "check_graphql_rate_limit",
    "last_rest_rate",
]

console = Console()

# ---- Module-level state for external readers (e.g. spider progress) ---------
# Updated after every REST response so the spider can display quota.
last_rest_rate: dict[str, int | None] = {"remaining": None, "limit": None}

# ---- Configurable thresholds ------------------------------------------------
# When remaining drops below this, we preemptively pause until reset.
_REST_LOW_REMAINING = 200
_GRAPHQL_LOW_REMAINING = 200
# When usage is high relative to the limit, we throttle slightly.
_REST_THROTTLE_USED = 4_000
# Minimum sleep after any preemptive pause (seconds).
_MIN_SLEEP = 1.0


# ------------------------------------------------------------------
# REST rate-limit inspection
# ------------------------------------------------------------------


async def check_rate_limit(response: httpx.Response) -> None:
    """Inspect REST response headers and sleep if approaching a rate limit.

    Handles both:
      - **Primary rate limit**: ``x-ratelimit-remaining`` approaches zero.
      - **Secondary rate limit**: ``retry-after`` header present on 403/429.

    Args:
        response: The ``httpx.Response`` to inspect.
    """
    # ------ Secondary rate limit (retry-after) ----------------------------
    # GitHub sends ``retry-after`` on secondary 429/403 responses.
    retry_after = response.headers.get("retry-after")
    if retry_after is not None:
        wait = max(float(retry_after), _MIN_SLEEP)
        console.print(
            f"  [bold yellow]>> Secondary rate limit hit![/] "
            f"Waiting {wait:.0f}s (retry-after header)..."
        )
        logger.warning(
            "Secondary rate limit hit (retry-after={}s). Sleeping {:.0f}s.",
            retry_after, wait,
        )
        await asyncio.sleep(wait)
        return

    # ------ Primary rate limit --------------------------------------------
    remaining_str = response.headers.get("x-ratelimit-remaining")
    reset_epoch = response.headers.get("x-ratelimit-reset")
    limit_str = response.headers.get("x-ratelimit-limit")
    used_str = response.headers.get("x-ratelimit-used")
    resource = response.headers.get("x-ratelimit-resource", "core")

    if remaining_str is not None:
        remaining = int(remaining_str)
        limit = int(limit_str) if limit_str else 5_000
        used = int(used_str) if used_str else (limit - remaining)

        # Expose latest REST quota for progress display
        last_rest_rate["remaining"] = remaining
        last_rest_rate["limit"] = limit

        # Hard pause: remaining is dangerously low
        if remaining < _REST_LOW_REMAINING and reset_epoch is not None:
            reset_dt = datetime.fromtimestamp(int(reset_epoch), tz=timezone.utc)
            now = datetime.now(tz=timezone.utc)
            wait_seconds = max((reset_dt - now).total_seconds() + 5, _MIN_SLEEP)
            console.print(
                f"  [bold yellow]>> REST rate limit low![/] "
                f"[{resource}] {remaining}/{limit} remaining. "
                f"Sleeping {wait_seconds:.0f}s until reset at "
                f"{reset_dt.strftime('%H:%M:%S')} UTC..."
            )
            logger.warning(
                "REST rate limit low ({}/{}, resource={}). "
                "Sleeping {:.0f}s until reset.",
                remaining, limit, resource, wait_seconds,
            )
            await asyncio.sleep(wait_seconds)
            return

        # Soft throttle: high usage
        if used > _REST_THROTTLE_USED:
            logger.info(
                "REST usage high (used={}/{}, resource={}). Throttling 1s.",
                used, limit, resource,
            )
            await asyncio.sleep(1.0)


# ------------------------------------------------------------------
# GraphQL rate-limit inspection
# ------------------------------------------------------------------


async def check_graphql_rate_limit(data: dict) -> None:
    """Inspect GraphQL ``rateLimit`` payload and sleep if close to limit.

    The GraphQL API returns rate-limit info inside the response body:
        ``{"data": {"rateLimit": {"remaining": N, "resetAt": "...", "cost": N}}}``

    When remaining points are low (< ``_GRAPHQL_LOW_REMAINING``), we sleep
    until the reset time to avoid getting our requests rejected.

    Args:
        data: The parsed JSON body from a GraphQL response containing
            a ``rateLimit`` key.
    """
    rate = data.get("rateLimit")
    if rate is None:
        return

    remaining = rate.get("remaining", 5000)
    reset_at = rate.get("resetAt")
    cost = rate.get("cost", 1)
    limit = rate.get("limit", 5000)

    if remaining < _GRAPHQL_LOW_REMAINING and reset_at:
        reset_dt = datetime.fromisoformat(reset_at.replace("Z", "+00:00"))
        now = datetime.now(tz=timezone.utc)
        wait_seconds = max((reset_dt - now).total_seconds() + 5, _MIN_SLEEP)
        console.print(
            f"  [bold yellow]>> GraphQL rate limit low![/] "
            f"{remaining}/{limit} points remaining (last query cost: {cost}). "
            f"Sleeping {wait_seconds:.0f}s until reset at "
            f"{reset_dt.strftime('%H:%M:%S')} UTC..."
        )
        logger.warning(
            "GraphQL rate limit low (remaining={}/{}, cost={}). "
            "Sleeping {:.0f}s until reset.",
            remaining, limit, cost, wait_seconds,
        )
        await asyncio.sleep(wait_seconds)
    elif remaining < 500:
        # Not yet critical, but worth logging
        logger.info(
            "GraphQL quota: {}/{} remaining (cost={}).",
            remaining, limit, cost,
        )

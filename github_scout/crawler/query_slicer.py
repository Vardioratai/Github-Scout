"""Query slicer — splits searches into date ranges to bypass GitHub's 1,000-result limit.

GitHub's Search API returns at most 1,000 results per query regardless of
pagination.  When ``repositoryCount`` exceeds 1,000 we must partition the
query into smaller date windows (using the ``created:START..END`` qualifier),
each returning fewer than 1,000 results.

Strategy:
  1. Probe the original query to get ``repositoryCount``.
  2. If count <= 1,000 → return the original query (no slicing needed).
  3. Otherwise, extract or infer a ``created:`` date range, divide it into
     slices, and yield one sub-query per slice.
  4. If any slice still exceeds 1,000, it is recursively sub-divided.
"""

from __future__ import annotations

import re
from datetime import date, timedelta

from loguru import logger

__all__: list[str] = ["generate_query_slices"]

# GitHub search hard cap
_GITHUB_SEARCH_LIMIT = 1_000

# Regex patterns for ``created:`` qualifiers in the query string.
_CREATED_GT = re.compile(r"created:>\s*(\d{4}-\d{2}-\d{2})")
_CREATED_GTE = re.compile(r"created:>=\s*(\d{4}-\d{2}-\d{2})")
_CREATED_RANGE = re.compile(
    r"created:(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})"
)
# Matches any ``created:...`` token to strip it from the base query.
_CREATED_ANY = re.compile(r"created:\S+")


def _parse_date(s: str) -> date:
    return date.fromisoformat(s)


def _extract_date_range(query: str) -> tuple[str, date, date]:
    """Return (base_query_without_created, start_date, end_date).

    Supports:
      - ``created:>YYYY-MM-DD``   → start = day after, end = today
      - ``created:>=YYYY-MM-DD``  → start = that day, end = today
      - ``created:START..END``    → explicit range
      - No ``created:`` at all    → start = 2008-01-01, end = today
    """
    today = date.today()

    m = _CREATED_RANGE.search(query)
    if m:
        start, end = _parse_date(m.group(1)), _parse_date(m.group(2))
        base = _CREATED_ANY.sub("", query).strip()
        return base, start, end

    m = _CREATED_GTE.search(query)
    if m:
        start = _parse_date(m.group(1))
        base = _CREATED_ANY.sub("", query).strip()
        return base, start, today

    m = _CREATED_GT.search(query)
    if m:
        start = _parse_date(m.group(1)) + timedelta(days=1)
        base = _CREATED_ANY.sub("", query).strip()
        return base, start, today

    # No created qualifier — use a wide default range
    return query, date(2008, 1, 1), today


def _split_date_range(
    start: date,
    end: date,
    n_slices: int,
) -> list[tuple[date, date]]:
    """Divide [start, end] into *n_slices* roughly equal sub-ranges."""
    total_days = (end - start).days
    if total_days <= 0 or n_slices <= 1:
        return [(start, end)]

    chunk = max(total_days // n_slices, 1)
    slices: list[tuple[date, date]] = []
    cur = start
    while cur <= end:
        slice_end = min(cur + timedelta(days=chunk - 1), end)
        slices.append((cur, slice_end))
        cur = slice_end + timedelta(days=1)
    return slices


def generate_query_slices(
    query: str,
    total_count: int,
) -> list[str]:
    """Generate sub-queries that together cover all results.

    If ``total_count`` <= 1,000 the original query is returned as-is.
    Otherwise the date range is divided into slices sized to stay under
    the 1,000-result cap (with a safety margin).

    Args:
        query: The original GitHub search query string.
        total_count: The ``repositoryCount`` returned by the first probe.

    Returns:
        A list of query strings, each expected to return < 1,000 results.
    """
    if total_count <= _GITHUB_SEARCH_LIMIT:
        return [query]

    base_query, start, end = _extract_date_range(query)

    # Estimate how many slices are needed (with 30% safety margin)
    n_slices = max(2, int(total_count / (_GITHUB_SEARCH_LIMIT * 0.7)))
    slices = _split_date_range(start, end, n_slices)

    queries: list[str] = []
    for s, e in slices:
        sub_q = f"{base_query} created:{s.isoformat()}..{e.isoformat()}"
        # Normalise whitespace
        sub_q = " ".join(sub_q.split())
        queries.append(sub_q)

    logger.info(
        "Query exceeds 1,000 results ({:,}). Split into {} date slices "
        "({} to {}).",
        total_count, len(queries), start.isoformat(), end.isoformat(),
    )
    return queries

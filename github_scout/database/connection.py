"""DuckDB connection factory."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
from loguru import logger

__all__: list[str] = ["get_connection"]


@contextmanager
def get_connection(db_path: Path | str = "./github_scout.duckdb") -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Context-managed DuckDB connection.

    Args:
        db_path: Filesystem path to the DuckDB database file.  Use
            ``":memory:"`` for in-memory databases (e.g. tests).

    Yields:
        An open ``DuckDBPyConnection``.
    """
    db_path_str = str(db_path)
    logger.debug("Opening DuckDB connection: {}", db_path_str)
    conn = duckdb.connect(db_path_str)
    try:
        yield conn
    finally:
        conn.close()
        logger.debug("Closed DuckDB connection: {}", db_path_str)

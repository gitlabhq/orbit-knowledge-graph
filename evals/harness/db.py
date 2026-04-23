"""
Shared DuckDB connection management for the eval harness.

All state lives in a single DuckDB file (.eval-servers/eval.duckdb).
Connections are short-lived with retry on lock contention so multiple
processes (runner, CLI) can access the DB concurrently.

Schema lives in sql/ddl.sql, reusable macros in sql/helpers.sql.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb

WORKSPACE_DIR = ".eval-servers"
DB_FILENAME = "eval.duckdb"
MAX_RETRIES = 10
INITIAL_BACKOFF_MS = 100
MAX_BACKOFF_MS = 5000

_SQL_DIR = Path(__file__).parent / "sql"


def _read_sql(name: str) -> str:
    return (_SQL_DIR / name).read_text()


def _is_lock_error(e: Exception) -> bool:
    msg = str(e).lower()
    return "could not set lock" in msg or "lock on file" in msg


@contextmanager
def connect(db_path: Path, read_only: bool = False) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open a short-lived DuckDB connection with retry on lock contention."""
    max_attempts = MAX_RETRIES if not read_only else 5
    backoff_ms = INITIAL_BACKOFF_MS if not read_only else 50

    for attempt in range(max_attempts + 1):
        try:
            conn = duckdb.connect(str(db_path), read_only=read_only)
            try:
                yield conn
            finally:
                conn.close()
            return
        except Exception as e:
            if attempt < max_attempts and _is_lock_error(e):
                time.sleep(backoff_ms / 1000)
                if not read_only:
                    backoff_ms = min(backoff_ms * 2, MAX_BACKOFF_MS)
            else:
                raise


def default_db_path(workspace: str | Path = WORKSPACE_DIR) -> Path:
    ws = Path(workspace)
    ws.mkdir(parents=True, exist_ok=True)
    return ws / DB_FILENAME


def ensure_schema(db_path: Path) -> None:
    with connect(db_path) as conn:
        conn.execute(_read_sql("ddl.sql"))
        conn.execute(_read_sql("helpers.sql"))

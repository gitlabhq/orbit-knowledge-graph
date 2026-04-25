"""
DuckDB access layer for the eval harness.

Two modes:
- Server mode: all reads/writes go through db_server.py via HTTP.
  Used during runs (multiple arms + CLI reading concurrently).
- Direct mode: connects to the .duckdb file directly.
  Used in tests and offline CLI commands when no server is running.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import httpx

logger = logging.getLogger(__name__)

WORKSPACE_DIR = ".eval-servers"
DB_FILENAME = "eval.duckdb"
DB_SERVER_PORT = 5555

_SQL_DIR = Path(__file__).parent / "sql"


def _read_sql(name: str) -> str:
    return (_SQL_DIR / name).read_text()


def default_db_path(workspace: str | Path = WORKSPACE_DIR) -> Path:
    ws = Path(workspace)
    ws.mkdir(parents=True, exist_ok=True)
    return ws / DB_FILENAME


def default_db_url() -> str:
    return f"http://localhost:{DB_SERVER_PORT}"


# ---------------------------------------------------------------------------
# Server mode: HTTP client to db_server.py
# ---------------------------------------------------------------------------

class DbClient:
    """HTTP client for the DuckDB proxy server."""

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = base_url or default_db_url()
        self._http = httpx.Client(base_url=self.base_url, timeout=10.0)

    def write(self, sql: str, params: list | None = None) -> None:
        r = self._http.post("/write", json={"sql": sql, "params": params or []})
        r.raise_for_status()

    def write_batch(self, statements: list[dict[str, Any]]) -> None:
        r = self._http.post("/write_batch", json={"statements": statements})
        r.raise_for_status()

    def query(self, sql: str, params: list | None = None) -> list[list]:
        r = self._http.post("/query", json={"sql": sql, "params": params or []})
        r.raise_for_status()
        return r.json().get("rows", [])

    def query_one(self, sql: str, params: list | None = None) -> list | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def is_alive(self) -> bool:
        try:
            r = self._http.get("/health")
            return r.status_code == 200
        except Exception:
            return False

    def close(self) -> None:
        self._http.close()


# ---------------------------------------------------------------------------
# Direct mode: for tests and offline CLI
# ---------------------------------------------------------------------------

def ensure_schema(db_path: Path) -> None:
    """Apply DDL + helpers directly. Used in tests and direct mode."""
    import duckdb
    conn = duckdb.connect(str(db_path))
    conn.execute(_read_sql("ddl.sql"))
    conn.execute(_read_sql("helpers.sql"))
    conn.close()


@contextmanager
def direct_connect(db_path: Path, read_only: bool = False) -> Generator:
    """Direct DuckDB connection. Use only when db_server is not running."""
    import duckdb
    max_retries = 5
    backoff_ms = 100
    for attempt in range(max_retries + 1):
        try:
            conn = duckdb.connect(str(db_path), read_only=read_only)
            try:
                yield conn
            finally:
                conn.close()
            return
        except Exception as e:
            msg = str(e).lower()
            if attempt < max_retries and ("lock" in msg):
                time.sleep(backoff_ms / 1000)
                backoff_ms = min(backoff_ms * 2, 5000)
            else:
                raise

"""
DuckDB access layer. Server mode (HTTP to db_server.py) or direct mode (tests/offline CLI).
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Generator

import httpx

WORKSPACE_DIR = ".eval-servers"
DB_FILENAME = "eval.duckdb"


_SQL_DIR = Path(__file__).parent / "sql"


def default_db_path(workspace: str | Path = WORKSPACE_DIR) -> Path:
    ws = Path(workspace)
    ws.mkdir(parents=True, exist_ok=True)
    return ws / DB_FILENAME


class DbClient:
    """HTTP client for the DuckDB proxy server."""

    def __init__(self, base_url: str | None = None) -> None:
        self._http = httpx.Client(
            base_url=base_url or "http://localhost:0",
            timeout=10.0,
        )

    def write(self, sql: str, params: list | None = None) -> None:
        self._http.post("/write", json={"sql": sql, "params": params or []}).raise_for_status()

    def write_batch(self, statements: list[dict[str, Any]]) -> None:
        self._http.post("/write_batch", json={"statements": statements}).raise_for_status()

    def query(self, sql: str, params: list | None = None) -> list[list]:
        r = self._http.post("/query", json={"sql": sql, "params": params or []})
        r.raise_for_status()
        return r.json().get("rows", [])

    def query_one(self, sql: str, params: list | None = None) -> list | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def is_alive(self) -> bool:
        try:
            return self._http.get("/health").status_code == 200
        except Exception:
            return False

    def close(self) -> None:
        self._http.close()


class DirectClient:
    """DuckDB client that connects directly to the file. For tests and offline CLI."""

    def __init__(self, db_path: Path) -> None:
        self._path = db_path
        ensure_schema(db_path)

    def write(self, sql: str, params: list | None = None) -> None:
        with direct_connect(self._path) as c:
            c.execute(sql, params or [])

    def write_batch(self, statements: list) -> None:
        with direct_connect(self._path) as c:
            for s in statements:
                c.execute(s["sql"], s.get("params", []))

    def query(self, sql: str, params: list | None = None) -> list[list]:
        with direct_connect(self._path, read_only=True) as c:
            return [list(r) for r in c.execute(sql, params or []).fetchall()]

    def query_one(self, sql: str, params: list | None = None) -> list | None:
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def is_alive(self) -> bool:
        return True


def get_client() -> DbClient | DirectClient:
    """Return DbClient if server is running, DirectClient otherwise."""
    port_file = Path(WORKSPACE_DIR) / "db.port"
    if port_file.exists():
        port = port_file.read_text().strip()
        db = DbClient(base_url=f"http://localhost:{port}")
        if db.is_alive():
            return db
    return DirectClient(default_db_path())


def ensure_schema(db_path: Path) -> None:
    import duckdb
    conn = duckdb.connect(str(db_path))
    conn.execute((_SQL_DIR / "ddl.sql").read_text())
    conn.execute((_SQL_DIR / "helpers.sql").read_text())
    conn.close()


@contextmanager
def direct_connect(db_path: Path, read_only: bool = False) -> Generator:
    """Direct DuckDB connection for tests/offline CLI."""
    import duckdb
    backoff = 0.1
    for attempt in range(6):
        try:
            conn = duckdb.connect(str(db_path), read_only=read_only)
            try:
                yield conn
            finally:
                conn.close()
            return
        except Exception as e:
            if attempt < 5 and "lock" in str(e).lower():
                time.sleep(backoff)
                backoff = min(backoff * 2, 5)
            else:
                raise

"""
DuckDB proxy server. Single process owns the database file.
All writes and reads go through HTTP endpoints.

Batches inserts every FLUSH_INTERVAL_MS or FLUSH_BATCH_SIZE, whichever
comes first. Reads use a separate cursor (MVCC snapshot isolation).

Start:  uvicorn harness.db_server:app --port 5555
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from harness.db import default_db_path

logger = logging.getLogger(__name__)

FLUSH_INTERVAL_MS = 100
FLUSH_BATCH_SIZE = 200

_db: duckdb.DuckDBPyConnection | None = None
_write_queue: asyncio.Queue[tuple[str, list]] = asyncio.Queue()
_flush_task: asyncio.Task | None = None


def _init_db(db_path: Path) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(str(db_path))
    ddl = (Path(__file__).parent / "sql" / "ddl.sql").read_text()
    helpers = (Path(__file__).parent / "sql" / "helpers.sql").read_text()
    conn.execute(ddl)
    conn.execute(helpers)
    return conn


async def _flush_loop() -> None:
    """Drain the write queue in batches."""
    global _db
    while True:
        batch: list[tuple[str, list]] = []
        try:
            # Wait for first item
            item = await asyncio.wait_for(_write_queue.get(), timeout=FLUSH_INTERVAL_MS / 1000)
            batch.append(item)
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break

        # Drain up to FLUSH_BATCH_SIZE
        while len(batch) < FLUSH_BATCH_SIZE:
            try:
                batch.append(_write_queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if _db and batch:
            try:
                _db.begin()
                for sql, params in batch:
                    _db.execute(sql, params)
                _db.commit()
            except Exception as e:
                logger.error("flush error: %s", e)
                try:
                    _db.rollback()
                except Exception:
                    pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _db, _flush_task
    db_path_str = app.state.db_path if hasattr(app.state, "db_path") else None
    db_path = Path(db_path_str) if db_path_str else default_db_path()
    _db = _init_db(db_path)
    _flush_task = asyncio.create_task(_flush_loop())
    logger.info("db_server started: %s", db_path)
    yield
    _flush_task.cancel()
    try:
        await _flush_task
    except asyncio.CancelledError:
        pass
    if _db:
        _db.close()
    logger.info("db_server stopped")


app = FastAPI(lifespan=lifespan)


@app.post("/write")
async def write(request: Request) -> JSONResponse:
    """Execute a parameterized write statement. Batched internally."""
    body = await request.json()
    sql = body.get("sql")
    params = body.get("params", [])
    if not sql:
        raise HTTPException(400, "missing 'sql'")
    await _write_queue.put((sql, params))
    return JSONResponse({"queued": True})


@app.post("/write_batch")
async def write_batch(request: Request) -> JSONResponse:
    """Execute multiple parameterized write statements. Batched internally."""
    body = await request.json()
    statements = body.get("statements", [])
    for stmt in statements:
        sql = stmt.get("sql")
        params = stmt.get("params", [])
        if sql:
            await _write_queue.put((sql, params))
    return JSONResponse({"queued": len(statements)})


@app.post("/query")
async def query(request: Request) -> JSONResponse:
    """Execute a read query. Uses a separate cursor for MVCC isolation."""
    body = await request.json()
    sql = body.get("sql")
    params = body.get("params", [])
    if not sql:
        raise HTTPException(400, "missing 'sql'")
    if not _db:
        raise HTTPException(503, "db not ready")

    try:
        cursor = _db.cursor()
        result = cursor.execute(sql, params)
        columns = [desc[0] for desc in result.description] if result.description else []
        rows = result.fetchall()
        cursor.close()
        return JSONResponse({"columns": columns, "rows": _serialize_rows(rows)})
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "queue_size": _write_queue.qsize()})


def _serialize_rows(rows: list) -> list:
    """Convert DuckDB rows to JSON-safe lists."""
    result = []
    for row in rows:
        result.append([_serialize_value(v) for v in row])
    return result


def _serialize_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (int, float, bool, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_serialize_value(x) for x in v]
    return str(v)

"""
DuckDB proxy server. Single process owns the database file.

Batches inserts every 100ms or 200 items. Reads use a separate cursor (MVCC).
Start: uvicorn harness.db_server:app --port 5555
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import duckdb
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from harness.db import default_db_path, ensure_schema

logger = logging.getLogger(__name__)

_db: duckdb.DuckDBPyConnection | None = None
_write_queue: asyncio.Queue[tuple[str, list]] = asyncio.Queue()
_flush_task: asyncio.Task | None = None


async def _flush_loop() -> None:
    while True:
        batch: list[tuple[str, list]] = []
        try:
            batch.append(await asyncio.wait_for(_write_queue.get(), timeout=0.1))
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        while len(batch) < 200:
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
    db_path = Path(app.state.db_path) if hasattr(app.state, "db_path") else default_db_path()
    ensure_schema(db_path)
    _db = duckdb.connect(str(db_path))
    # Load macros into the connection
    _db.execute((Path(__file__).parent / "sql" / "helpers.sql").read_text())
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


app = FastAPI(lifespan=lifespan)


def _serialize(v: Any) -> Any:
    if v is None or isinstance(v, (int, float, bool, str)):
        return v
    if isinstance(v, (list, tuple)):
        return [_serialize(x) for x in v]
    return str(v)


@app.post("/write")
async def write(request: Request) -> JSONResponse:
    body = await request.json()
    if not body.get("sql"):
        raise HTTPException(400, "missing 'sql'")
    await _write_queue.put((body["sql"], body.get("params", [])))
    return JSONResponse({"queued": True})


@app.post("/write_batch")
async def write_batch(request: Request) -> JSONResponse:
    body = await request.json()
    stmts = body.get("statements", [])
    for s in stmts:
        if s.get("sql"):
            await _write_queue.put((s["sql"], s.get("params", [])))
    return JSONResponse({"queued": len(stmts)})


@app.post("/query")
async def query(request: Request) -> JSONResponse:
    body = await request.json()
    if not body.get("sql"):
        raise HTTPException(400, "missing 'sql'")
    if not _db:
        raise HTTPException(503, "db not ready")
    try:
        cursor = _db.cursor()
        result = cursor.execute(body["sql"], body.get("params", []))
        cols = [d[0] for d in result.description] if result.description else []
        rows = [[_serialize(v) for v in row] for row in result.fetchall()]
        cursor.close()
        return JSONResponse({"columns": cols, "rows": rows})
    except Exception as e:
        raise HTTPException(400, str(e))


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({"status": "ok", "queue_size": _write_queue.qsize()})

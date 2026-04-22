"""
Centralized logging for the eval harness.

Two outputs:
  1. Console: terse, human-readable, no httpx noise
  2. File: structured JSONL with timing, one line per event

Every event has a category, optional task/arm context, and duration_ms.
"""

from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator


_log_file: Path | None = None
_run_id: str | None = None


def setup(run_id: str, log_dir: str = ".eval-servers/logs") -> None:
    """Configure logging for a run. Call once at startup."""
    global _log_file, _run_id
    _run_id = run_id

    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    _log_file = log_path / f"run-{run_id}.jsonl"

    root = logging.getLogger()
    # Clear any handlers set by basicConfig or prior setup
    root.handlers.clear()
    root.setLevel(logging.DEBUG)

    # Console: terse, only harness.* at INFO+
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S"
    ))
    console.addFilter(_HarnessFilter())
    root.addHandler(console)

    # Suppress noisy loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("hpack").setLevel(logging.WARNING)


class _HarnessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith("harness")


def event(
    category: str,
    message: str,
    *,
    arm: str | None = None,
    task_id: str | None = None,
    duration_ms: int | None = None,
    data: dict[str, Any] | None = None,
    level: str = "info",
) -> None:
    """Write a structured event to both console and JSONL file."""
    logger = logging.getLogger(f"harness.{category}")

    # Console line
    parts = []
    if arm:
        parts.append(f"[{arm}]")
    if task_id:
        parts.append(f"{task_id}:")
    parts.append(message)
    if duration_ms is not None:
        parts.append(f"({_fmt_duration(duration_ms)})")
    console_msg = " ".join(parts)

    log_fn = getattr(logger, level, logger.info)
    log_fn(console_msg)

    # JSONL line
    if _log_file:
        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "run_id": _run_id,
            "category": category,
            "message": message,
        }
        if arm:
            record["arm"] = arm
        if task_id:
            record["task_id"] = task_id
        if duration_ms is not None:
            record["duration_ms"] = duration_ms
        if data:
            record["data"] = data
        with _log_file.open("a") as f:
            f.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")


@contextmanager
def timed(
    category: str,
    message: str,
    *,
    arm: str | None = None,
    task_id: str | None = None,
    data: dict[str, Any] | None = None,
) -> Generator[dict[str, Any], None, None]:
    """Context manager that logs an event with duration on exit.

    Yields a mutable dict -- put extra data in it and it'll be included.

        with log.timed("task", "executing", arm="orbit", task_id="t1") as ctx:
            result = do_work()
            ctx["status"] = result.status
    """
    ctx: dict[str, Any] = dict(data or {})
    start = time.monotonic()
    try:
        yield ctx
    finally:
        elapsed = int((time.monotonic() - start) * 1000)
        merged = dict(data or {})
        merged.update(ctx)
        event(category, message, arm=arm, task_id=task_id,
              duration_ms=elapsed, data=merged if merged else None)


def _fmt_duration(ms: int) -> str:
    if ms < 1000:
        return f"{ms}ms"
    s = ms / 1000
    if s < 60:
        return f"{s:.1f}s"
    m = int(s // 60)
    s = s % 60
    return f"{m}m{s:.0f}s"

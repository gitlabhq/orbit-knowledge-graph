"""
Result storage backed by DuckDB via the db server.

All reads go through SQL macros defined in helpers.sql.
All writes are batched by the db server.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from harness.db import DbClient, default_db_url
from harness.session import SessionSnapshot

logger = logging.getLogger(__name__)


class TaskStatus(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    AGENT_ERROR = "agent_error"
    INFRA_ERROR = "infra_error"


@dataclass
class SessionSummary:
    session_id: str
    steps: int = 0
    tool_calls: int = 0
    tokens: dict[str, int] = field(default_factory=dict)
    cost: float = 0.0
    duration_ms: int = 0


@dataclass
class TaskResult:
    task_id: str
    arm: str
    status: TaskStatus
    timestamp: str
    structured_output: dict[str, Any] | None = None
    error: str | None = None
    error_type: str | None = None
    session_summary: SessionSummary | None = None


def summarize_snapshot(snapshot: SessionSnapshot) -> SessionSummary:
    total_tool_calls = 0
    total_tokens: dict[str, int] = {"input": 0, "output": 0, "cache_read": 0}
    total_cost = 0.0
    steps = 0

    for msg in snapshot.messages:
        if msg.info.role == "assistant":
            steps += 1
            total_cost += msg.info.cost
            tokens = msg.info.tokens
            total_tokens["input"] += tokens.get("input", 0)
            total_tokens["output"] += tokens.get("output", 0)
            cache = tokens.get("cache", {})
            if isinstance(cache, dict):
                total_tokens["cache_read"] += cache.get("read", 0)
            else:
                total_tokens["cache_read"] += tokens.get("cache_read", 0)
            for part in msg.parts:
                if part.type in ("tool-invocation", "tool"):
                    total_tool_calls += 1

    return SessionSummary(
        session_id=snapshot.session.id,
        steps=steps,
        tool_calls=total_tool_calls,
        tokens=total_tokens,
        cost=total_cost,
        duration_ms=snapshot.timing.get("duration_ms", 0),
    )


class ResultStore:
    """Reads and writes eval results via the DuckDB proxy server."""

    def __init__(self, db: DbClient | None = None, run_id: str | None = None) -> None:
        self.db = db or DbClient()
        self.run_id = run_id or self.make_run_id()
        self._event_seqs: dict[str, int] = {}

    # -- writes ---------------------------------------------------------------

    def write_result(self, result: TaskResult) -> None:
        s = result.session_summary
        structured = (
            json.dumps(result.structured_output, default=str)
            if result.structured_output is not None
            else None
        )
        self.db.write(
            "INSERT OR REPLACE INTO task_results "
            "(run_id, task_id, arm, status, timestamp, structured_output, "
            " error, error_type, session_id, steps, tool_calls, "
            " tokens_input, tokens_output, tokens_cache_read, cost, duration_ms) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                self.run_id, result.task_id, result.arm, result.status.value,
                result.timestamp, structured, result.error, result.error_type,
                s.session_id if s else None,
                s.steps if s else 0, s.tool_calls if s else 0,
                s.tokens.get("input", 0) if s else 0,
                s.tokens.get("output", 0) if s else 0,
                s.tokens.get("cache_read", 0) if s else 0,
                s.cost if s else 0.0, s.duration_ms if s else 0,
            ],
        )

    def write_snapshot(self, arm: str, task_id: str, snapshot: SessionSnapshot) -> None:
        data = json.dumps(snapshot.to_dict(), default=str)
        self.db.write(
            "INSERT OR REPLACE INTO snapshots (run_id, arm, task_id, data) VALUES (?, ?, ?, ?)",
            [self.run_id, arm, task_id, data],
        )

    def write_live_event(self, arm: str, task_id: str, event: dict[str, Any]) -> None:
        key = f"{arm}:{task_id}"
        seq = self._event_seqs.get(key, 0)
        self._event_seqs[key] = seq + 1
        event_type = event.get("type", "unknown")
        timestamp = event.get("ts", "")
        data_json = json.dumps(event.get("data", {}), default=str)
        self.db.write(
            "INSERT OR REPLACE INTO live_events "
            "(run_id, arm, task_id, seq, event_type, timestamp, data) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [self.run_id, arm, task_id, seq, event_type, timestamp, data_json],
        )

    def write_scores(self, arm: str, task_scores: list[dict[str, Any]]) -> None:
        stmts = []
        for entry in task_scores:
            task_id = entry["task_id"]
            for evaluator, score_data in entry["scores"].items():
                stmts.append({
                    "sql": "INSERT OR REPLACE INTO scores "
                           "(run_id, arm, task_id, evaluator, score) VALUES (?, ?, ?, ?, ?)",
                    "params": [self.run_id, arm, task_id, evaluator,
                               json.dumps(score_data, default=str)],
                })
        if stmts:
            self.db.write_batch(stmts)

    def snapshot_config(self, config: Any, base_dir: Path | None = None) -> str:
        base = base_dir or Path(".")
        files: dict[str, str] = {}

        for arm in config.arms:
            _collect_file(files, base / arm.agent)
            for skill in arm.skills:
                _collect_file(files, base / skill / "SKILL.md")

        for pattern in config.run.tasks.paths:
            for path in sorted(base.glob(pattern)):
                _collect_file(files, path)

        fixtures = base / config.run.scoring.fixtures_path
        if fixtures.is_dir():
            for f in sorted(fixtures.rglob("*.json")):
                _collect_file(files, f)

        config_json = json.dumps(config.model_dump(), sort_keys=True, default=str)
        files_json = json.dumps(files, sort_keys=True)

        h = hashlib.sha256()
        h.update(config_json.encode())
        h.update(files_json.encode())
        config_hash = h.hexdigest()[:16]

        config_name = config.run.name
        config_version = config.run.version

        self.db.write(
            "INSERT OR REPLACE INTO run_configs "
            "(run_id, config_name, config_version, config_hash, config, files) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [self.run_id, config_name, config_version, config_hash, config_json, files_json],
        )
        return config_hash

    # -- reads (all via macros) -----------------------------------------------

    def read_results(self, arm: str) -> list[TaskResult]:
        rows = self.db.query("FROM results_for_arm(?, ?)", [self.run_id, arm])
        results = []
        for row in rows:
            (task_id, arm_name, status, ts, structured_json,
             error, error_type, session_id, steps, tool_calls,
             tok_in, tok_out, tok_cache, cost, duration_ms) = row
            structured = json.loads(structured_json) if structured_json else None
            summary = SessionSummary(
                session_id=session_id or "", steps=steps, tool_calls=tool_calls,
                tokens={"input": tok_in, "output": tok_out, "cache_read": tok_cache},
                cost=cost, duration_ms=duration_ms,
            ) if session_id else None
            results.append(TaskResult(
                task_id=task_id, arm=arm_name, status=TaskStatus(status),
                timestamp=ts, structured_output=structured,
                error=error, error_type=error_type, session_summary=summary,
            ))
        return results

    def completed_task_ids(self, arm: str) -> set[str]:
        rows = self.db.query("FROM completed_tasks(?, ?)", [self.run_id, arm])
        return {r[0] for r in rows}

    def read_snapshot(self, arm: str, task_id: str) -> dict[str, Any] | None:
        row = self.db.query_one("FROM snapshot(?, ?, ?)", [self.run_id, arm, task_id])
        if not row:
            return None
        return json.loads(row[0])

    def read_scores(self) -> dict[str, list[dict[str, Any]]]:
        rows = self.db.query("FROM scores_for_run(?)", [self.run_id])
        by_arm: dict[str, dict[str, dict[str, Any]]] = {}
        for arm, task_id, evaluator, score_json in rows:
            by_arm.setdefault(arm, {}).setdefault(task_id, {})[evaluator] = json.loads(score_json)
        result: dict[str, list[dict[str, Any]]] = {}
        for arm, tasks in by_arm.items():
            result[arm] = [{"task_id": tid, "scores": scores} for tid, scores in tasks.items()]
        return result

    def list_run_ids(self) -> list[str]:
        rows = self.db.query("FROM all_run_ids()")
        return [r[0] for r in rows]

    def read_config(self) -> dict[str, Any] | None:
        row = self.db.query_one("FROM run_config(?)", [self.run_id])
        if not row:
            return None
        return {
            "config_name": row[0], "config_version": row[1],
            "config_hash": row[2], "config": json.loads(row[3]),
            "files": json.loads(row[4]),
        }

    def find_runs_by_config(self, config_hash: str) -> list[str]:
        rows = self.db.query("FROM run_ids_by_config(?)", [config_hash])
        return [r[0] for r in rows]

    @staticmethod
    def make_run_id() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def list_run_ids(db: DbClient) -> list[str]:
    rows = db.query("FROM all_run_ids()")
    return [r[0] for r in rows]


def _collect_file(files: dict[str, str], path: Path) -> None:
    try:
        files[str(path)] = path.resolve().read_text()
    except (OSError, UnicodeDecodeError) as e:
        logger.debug("skipping file %s: %s", path, e)

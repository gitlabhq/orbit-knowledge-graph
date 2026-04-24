"""
Result storage backed by DuckDB.

All eval state (task results, session snapshots, scores) lives in the
shared DuckDB file managed by harness.db. No more JSONL or JSON files.
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

from harness.db import connect, default_db_path, ensure_schema

logger = logging.getLogger(__name__)
from harness.session import SessionSnapshot


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
    """Extract lightweight summary stats from a full snapshot."""
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
    """Reads and writes eval results, snapshots, and scores to DuckDB."""

    def __init__(self, db_path: Path | None = None, run_id: str | None = None) -> None:
        self.db_path = db_path or default_db_path()
        self.run_id = run_id or self.make_run_id()
        ensure_schema(self.db_path)
        self._event_seqs = self._load_event_seqs()

    def _load_event_seqs(self) -> dict[str, int]:
        """Seed event sequence counters from DB for resume correctness."""
        try:
            with connect(self.db_path, read_only=True) as db:
                rows = db.execute(
                    "SELECT arm || ':' || task_id, max(seq) + 1 "
                    "FROM live_events WHERE run_id = ? GROUP BY arm, task_id",
                    [self.run_id],
                ).fetchall()
            return {k: v for k, v in rows}
        except Exception:
            return {}

    def write_live_event(self, arm: str, task_id: str, event: dict[str, Any]) -> None:
        """Write a single SSE event to DuckDB as it arrives."""
        key = f"{arm}:{task_id}"
        seq = self._event_seqs.get(key, 0)
        self._event_seqs[key] = seq + 1

        event_type = event.get("type", "unknown")
        timestamp = event.get("ts", "")
        data_json = json.dumps(event.get("data", {}), default=str)

        with connect(self.db_path) as db:
            db.execute(
                "INSERT OR REPLACE INTO live_events "
                "(run_id, arm, task_id, seq, event_type, timestamp, data) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [self.run_id, arm, task_id, seq, event_type, timestamp, data_json],
            )

    def write_result(self, result: TaskResult) -> None:
        s = result.session_summary
        structured = (
            json.dumps(result.structured_output, default=str)
            if result.structured_output is not None
            else None
        )
        with connect(self.db_path) as db:
            db.execute(
                "INSERT OR REPLACE INTO task_results "
                "(run_id, task_id, arm, status, timestamp, structured_output, "
                " error, error_type, session_id, steps, tool_calls, "
                " tokens_input, tokens_output, tokens_cache_read, cost, duration_ms) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    self.run_id,
                    result.task_id,
                    result.arm,
                    result.status.value,
                    result.timestamp,
                    structured,
                    result.error,
                    result.error_type,
                    s.session_id if s else None,
                    s.steps if s else 0,
                    s.tool_calls if s else 0,
                    s.tokens.get("input", 0) if s else 0,
                    s.tokens.get("output", 0) if s else 0,
                    s.tokens.get("cache_read", 0) if s else 0,
                    s.cost if s else 0.0,
                    s.duration_ms if s else 0,
                ],
            )

    def write_snapshot(self, arm: str, task_id: str, snapshot: SessionSnapshot) -> None:
        data = json.dumps(snapshot.to_dict(), default=str)
        with connect(self.db_path) as db:
            db.execute(
                "INSERT OR REPLACE INTO snapshots (run_id, arm, task_id, data) VALUES (?, ?, ?, ?)",
                [self.run_id, arm, task_id, data],
            )

    def read_snapshot(self, arm: str, task_id: str) -> dict[str, Any] | None:
        with connect(self.db_path, read_only=True) as db:
            row = db.execute(
                "FROM snapshot(?, ?, ?)", [self.run_id, arm, task_id]
            ).fetchone()
        if not row:
            return None
        return json.loads(row[0])

    def read_results(self, arm: str) -> list[TaskResult]:
        with connect(self.db_path, read_only=True) as db:
            rows = db.execute(
                "FROM results_for_arm(?, ?)", [self.run_id, arm]
            ).fetchall()

        results = []
        for row in rows:
            (task_id, arm_name, status, ts, structured_json,
             error, error_type, session_id, steps, tool_calls,
             tok_in, tok_out, tok_cache, cost, duration_ms) = row

            structured = json.loads(structured_json) if structured_json else None
            summary = SessionSummary(
                session_id=session_id or "",
                steps=steps,
                tool_calls=tool_calls,
                tokens={"input": tok_in, "output": tok_out, "cache_read": tok_cache},
                cost=cost,
                duration_ms=duration_ms,
            ) if session_id else None

            results.append(TaskResult(
                task_id=task_id,
                arm=arm_name,
                status=TaskStatus(status),
                timestamp=ts,
                structured_output=structured,
                error=error,
                error_type=error_type,
                session_summary=summary,
            ))
        return results

    def completed_task_ids(self, arm: str) -> set[str]:
        with connect(self.db_path, read_only=True) as db:
            rows = db.execute(
                "FROM completed_tasks(?, ?)", [self.run_id, arm]
            ).fetchall()
        return {r[0] for r in rows}

    def write_scores(self, arm: str, task_scores: list[dict[str, Any]]) -> None:
        """Write per-task evaluator scores.

        task_scores: [{"task_id": ..., "scores": {"evaluator_name": {...}, ...}}, ...]
        """
        with connect(self.db_path) as db:
            for entry in task_scores:
                task_id = entry["task_id"]
                for evaluator, score_data in entry["scores"].items():
                    db.execute(
                        "INSERT OR REPLACE INTO scores "
                        "(run_id, arm, task_id, evaluator, score) VALUES (?, ?, ?, ?, ?)",
                        [self.run_id, arm, task_id, evaluator, json.dumps(score_data, default=str)],
                    )

    def read_scores(self) -> dict[str, list[dict[str, Any]]]:
        """Read all scores for the current run, grouped by arm.

        Returns: {"arm": [{"task_id": ..., "scores": {"evaluator": {...}}}, ...]}
        """
        with connect(self.db_path, read_only=True) as db:
            rows = db.execute(
                "FROM scores_for_run(?)", [self.run_id]
            ).fetchall()

        by_arm: dict[str, dict[str, dict[str, Any]]] = {}
        for arm, task_id, evaluator, score_json in rows:
            by_arm.setdefault(arm, {}).setdefault(task_id, {})[evaluator] = json.loads(score_json)

        result: dict[str, list[dict[str, Any]]] = {}
        for arm, tasks in by_arm.items():
            result[arm] = [{"task_id": tid, "scores": scores} for tid, scores in tasks.items()]
        return result

    def list_run_ids(self) -> list[str]:
        """List all run IDs that have task results, most recent first."""
        return list_run_ids(self.db_path)

    def snapshot_config(self, config: Any, base_dir: Path | None = None) -> str:
        """Snapshot the full eval config + all referenced files into DuckDB.

        Collects: eval.yaml (as parsed config), agent files, skill files,
        task YAMLs, and fixture files. Returns the config_hash.
        """
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

        # Hash config + files together for a stable fingerprint
        h = hashlib.sha256()
        h.update(config_json.encode())
        h.update(files_json.encode())
        config_hash = h.hexdigest()[:16]

        config_name = config.run.name
        config_version = config.run.version

        with connect(self.db_path) as db:
            db.execute(
                "INSERT OR REPLACE INTO run_configs "
                "(run_id, config_name, config_version, config_hash, config, files) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [self.run_id, config_name, config_version, config_hash, config_json, files_json],
            )

        return config_hash

    def read_config(self) -> dict[str, Any] | None:
        """Read the snapshotted config for the current run."""
        with connect(self.db_path, read_only=True) as db:
            row = db.execute(
                "FROM run_config(?)", [self.run_id]
            ).fetchone()
        if not row:
            return None
        return {
            "config_name": row[0],
            "config_version": row[1],
            "config_hash": row[2],
            "config": json.loads(row[3]),
            "files": json.loads(row[4]),
        }

    def find_runs_by_config(self, config_hash: str) -> list[str]:
        """Find all run IDs that used the same config hash."""
        with connect(self.db_path, read_only=True) as db:
            rows = db.execute(
                "FROM run_ids_by_config(?)", [config_hash]
            ).fetchall()
        return [r[0] for r in rows]

    @staticmethod
    def make_run_id() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def list_run_ids(db_path: Path) -> list[str]:
    """List all run IDs that have task results, most recent first."""
    with connect(db_path, read_only=True) as db:
        rows = db.execute("FROM all_run_ids()").fetchall()
    return [r[0] for r in rows]


def _collect_file(files: dict[str, str], path: Path) -> None:
    """Read a file into the files dict, keyed by relative path."""
    try:
        resolved = path.resolve()
        content = resolved.read_text()
        # Use the path as given (relative) for the key
        files[str(path)] = content
    except (OSError, UnicodeDecodeError) as e:
        logger.debug("skipping file %s: %s", path, e)

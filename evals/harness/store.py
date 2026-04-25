"""Result storage backed by DuckDB via the db server."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from harness.db import DbClient
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
    tokens = {"input": 0, "output": 0, "cache_read": 0}
    cost, steps, tool_calls = 0.0, 0, 0
    for msg in snapshot.messages:
        if msg.info.role != "assistant":
            continue
        steps += 1
        cost += msg.info.cost
        t = msg.info.tokens
        tokens["input"] += t.get("input", 0)
        tokens["output"] += t.get("output", 0)
        cache = t.get("cache", {})
        tokens["cache_read"] += cache.get("read", 0) if isinstance(cache, dict) else t.get("cache_read", 0)
        tool_calls += sum(1 for p in msg.parts if p.type in ("tool-invocation", "tool"))
    return SessionSummary(
        session_id=snapshot.session.id, steps=steps, tool_calls=tool_calls,
        tokens=tokens, cost=cost, duration_ms=snapshot.timing.get("duration_ms", 0),
    )


class ResultStore:
    def __init__(self, db: DbClient, run_id: str | None = None) -> None:
        self.db = db
        self.run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self._event_seqs: dict[str, int] = {}

    def write_result(self, r: TaskResult) -> None:
        s = r.session_summary
        self.db.write(
            "INSERT OR REPLACE INTO task_results "
            "(run_id,task_id,arm,status,timestamp,structured_output,"
            "error,error_type,session_id,steps,tool_calls,"
            "tokens_input,tokens_output,tokens_cache_read,cost,duration_ms) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [self.run_id, r.task_id, r.arm, r.status.value, r.timestamp,
             json.dumps(r.structured_output, default=str) if r.structured_output else None,
             r.error, r.error_type,
             s.session_id if s else None, s.steps if s else 0, s.tool_calls if s else 0,
             s.tokens.get("input", 0) if s else 0, s.tokens.get("output", 0) if s else 0,
             s.tokens.get("cache_read", 0) if s else 0, s.cost if s else 0.0, s.duration_ms if s else 0],
        )

    def write_snapshot(self, arm: str, task_id: str, snapshot: SessionSnapshot) -> None:
        self.db.write(
            "INSERT OR REPLACE INTO snapshots (run_id,arm,task_id,data) VALUES (?,?,?,?)",
            [self.run_id, arm, task_id, json.dumps(snapshot.to_dict(), default=str)],
        )

    def write_live_event(self, arm: str, task_id: str, event: dict[str, Any]) -> None:
        key = f"{arm}:{task_id}"
        seq = self._event_seqs.get(key, 0)
        self._event_seqs[key] = seq + 1
        self.db.write(
            "INSERT OR REPLACE INTO live_events "
            "(run_id,arm,task_id,seq,event_type,timestamp,data) VALUES (?,?,?,?,?,?,?)",
            [self.run_id, arm, task_id, seq,
             event.get("type", "unknown"), event.get("ts", ""),
             json.dumps(event.get("data", {}), default=str)],
        )

    def write_scores(self, arm: str, task_scores: list[dict[str, Any]]) -> None:
        stmts = [
            {"sql": "INSERT OR REPLACE INTO scores (run_id,arm,task_id,evaluator,score) VALUES (?,?,?,?,?)",
             "params": [self.run_id, arm, e["task_id"], ev, json.dumps(sc, default=str)]}
            for e in task_scores for ev, sc in e["scores"].items()
        ]
        if stmts:
            self.db.write_batch(stmts)

    def snapshot_config(self, config: Any, base_dir: Path | None = None,
                        image_hash: str | None = None) -> str:
        base = base_dir or Path(".")
        files: dict[str, str] = {}
        container = base / "container"
        for arm in config.arms:
            _collect_file(files, container / ".opencode" / "agents" / f"{arm.agent}.md")
            for skill in arm.skills:
                _collect_file(files, container / ".opencode" / "skills" / skill / "SKILL.md")
        for pattern in config.run.tasks.paths:
            for p in sorted(base.glob(pattern)):
                _collect_file(files, p)
        fixtures = base / config.run.scoring.fixtures_path
        if fixtures.is_dir():
            for f in sorted(fixtures.rglob("*.json")):
                _collect_file(files, f)

        config_json = json.dumps(config.model_dump(), sort_keys=True, default=str)
        files_json = json.dumps(files, sort_keys=True)
        config_hash = hashlib.sha256((config_json + files_json).encode()).hexdigest()[:16]

        self.db.write(
            "INSERT OR REPLACE INTO run_configs "
            "(run_id,config_name,config_version,config_hash,image_hash,config,files) "
            "VALUES (?,?,?,?,?,?,?)",
            [self.run_id, config.run.name, config.run.version, config_hash,
             image_hash, config_json, files_json],
        )
        return config_hash

    # reads (all via macros)

    def read_results(self, arm: str) -> list[TaskResult]:
        return [_row_to_result(r) for r in self.db.query("FROM results_for_arm(?,?)", [self.run_id, arm])]

    def completed_task_ids(self, arm: str) -> set[str]:
        return {r[0] for r in self.db.query("FROM completed_tasks(?,?)", [self.run_id, arm])}

    def read_snapshot(self, arm: str, task_id: str) -> dict[str, Any] | None:
        row = self.db.query_one("FROM snapshot(?,?,?)", [self.run_id, arm, task_id])
        return json.loads(row[0]) if row else None

    def read_scores(self) -> dict[str, list[dict[str, Any]]]:
        by_arm: dict[str, dict[str, dict]] = defaultdict(lambda: defaultdict(dict))
        for arm, tid, ev, sc in self.db.query("FROM scores_for_run(?)", [self.run_id]):
            by_arm[arm][tid][ev] = json.loads(sc)
        return {arm: [{"task_id": tid, "scores": s} for tid, s in tasks.items()]
                for arm, tasks in by_arm.items()}

    def list_run_ids(self) -> list[str]:
        return [r[0] for r in self.db.query("FROM all_run_ids()")]

    def read_config(self) -> dict[str, Any] | None:
        row = self.db.query_one("FROM run_config(?)", [self.run_id])
        if not row:
            return None
        return {"config_name": row[0], "config_version": row[1], "config_hash": row[2],
                "image_hash": row[3], "config": json.loads(row[4]), "files": json.loads(row[5])}

    def find_runs_by_config(self, config_hash: str) -> list[str]:
        return [r[0] for r in self.db.query("FROM run_ids_by_config(?)", [config_hash])]

    @staticmethod
    def make_run_id() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def list_run_ids(db: DbClient) -> list[str]:
    return [r[0] for r in db.query("FROM all_run_ids()")]


def _row_to_result(row) -> TaskResult:
    (tid, arm, status, ts, structured, error, etype,
     sid, steps, tools, tok_in, tok_out, tok_cache, cost, dur) = row
    summary = SessionSummary(
        session_id=sid or "", steps=steps, tool_calls=tools,
        tokens={"input": tok_in, "output": tok_out, "cache_read": tok_cache},
        cost=cost, duration_ms=dur,
    ) if sid else None
    return TaskResult(
        task_id=tid, arm=arm, status=TaskStatus(status), timestamp=ts,
        structured_output=json.loads(structured) if structured else None,
        error=error, error_type=etype, session_summary=summary,
    )


def _collect_file(files: dict[str, str], path: Path) -> None:
    try:
        files[str(path)] = path.resolve().read_text()
    except (OSError, UnicodeDecodeError):
        pass

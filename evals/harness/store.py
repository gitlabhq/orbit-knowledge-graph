"""
Result storage: JSONL lines for fast scanning, snapshot files for deep analysis.

Layout:
    results/<run_id>/
        <arm>.jsonl           one TaskResult line per completed task
        sessions/
            <task_id>.json    full SessionSnapshot
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

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
    snapshot_path: str | None = None

    def to_jsonl_line(self) -> str:
        d = asdict(self)
        d["status"] = self.status.value
        return json.dumps(d, separators=(",", ":"), default=str)


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
            for k in total_tokens:
                total_tokens[k] += msg.info.tokens.get(k, 0)
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
    """Writes task results (JSONL) and session snapshots (JSON) to disk."""

    def __init__(self, output_dir: str | Path, run_id: str) -> None:
        self.base = Path(output_dir) / run_id
        self.sessions_dir = self.base / "sessions"
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self._handles: dict[str, Any] = {}

    def _jsonl_path(self, arm: str) -> Path:
        return self.base / f"{arm}.jsonl"

    def write_result(self, result: TaskResult) -> None:
        path = self._jsonl_path(result.arm)
        with path.open("a") as f:
            f.write(result.to_jsonl_line() + "\n")

    def write_snapshot(self, task_id: str, snapshot: SessionSnapshot) -> str:
        rel = f"sessions/{task_id}.json"
        path = self.sessions_dir / f"{task_id}.json"
        with path.open("w") as f:
            json.dump(snapshot.to_dict(), f, indent=2, default=str)
        return rel

    def read_results(self, arm: str) -> list[TaskResult]:
        path = self._jsonl_path(arm)
        if not path.exists():
            return []
        results = []
        for line in path.read_text().strip().splitlines():
            d = json.loads(line)
            d["status"] = TaskStatus(d["status"])
            if d.get("session_summary"):
                d["session_summary"] = SessionSummary(**d["session_summary"])
            results.append(TaskResult(**d))
        return results

    def completed_task_ids(self, arm: str) -> set[str]:
        return {r.task_id for r in self.read_results(arm)}

    @staticmethod
    def make_run_id() -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

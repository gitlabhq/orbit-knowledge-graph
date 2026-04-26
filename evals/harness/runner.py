"""Core orchestration loop for the eval harness."""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

import harness.log as log
from harness.config import ArmConfig, EvalConfig
from harness.session import EventDemuxer, capture_snapshot
from harness.store import ResultStore, TaskResult, TaskStatus, summarize_snapshot


@dataclass
class EvalTask:
    id: str
    prompt: str
    category: str
    difficulty: str = "medium"
    description: str = ""
    structured_output_schema: dict[str, Any] | None = None
    tags: list[str] | None = None
    timeout_override: int | None = None


def load_tasks(config: EvalConfig) -> list[EvalTask]:
    tasks: list[EvalTask] = []
    for pattern in config.run.tasks.paths:
        for path in sorted(Path(".").glob(pattern)):
            data = yaml.safe_load(path.read_text())
            if not data:
                continue
            for t in (data if isinstance(data, list) else [data]):
                tasks.append(EvalTask(
                    id=t["id"], prompt=t["prompt"], category=t["category"],
                    difficulty=t.get("difficulty", "medium"),
                    description=t.get("description", ""),
                    structured_output_schema=t.get("structured_output_schema"),
                    tags=t.get("tags"), timeout_override=t.get("timeout_override"),
                ))
    filt = config.run.tasks.filter
    if filt.ids:
        tasks = [t for t in tasks if t.id in filt.ids]
    if filt.categories:
        tasks = [t for t in tasks if t.category in filt.categories]
    diff_order = ["easy", "medium", "hard", "very-hard"]
    min_idx = diff_order.index(filt.min_difficulty.value)
    return [t for t in tasks if t.difficulty in diff_order
            and diff_order.index(t.difficulty) >= min_idx]


def render_prompt(task: EvalTask, fixtures_path: str) -> str:
    params_file = Path(fixtures_path) / task.id / "params.json"
    if not params_file.exists():
        return task.prompt
    params = json.loads(params_file.read_text())
    prompt = task.prompt
    for key, val in params.items():
        prompt = prompt.replace(f"{{{{{key}}}}}", str(val))
    return prompt


_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*\n(.*?)```", re.DOTALL)


class EvalRunner:
    """Holds all run state. Methods use self instead of threading params."""

    def __init__(self, config: EvalConfig, work_dir: str | None = None) -> None:
        self.config = config
        self.work_dir = work_dir or os.getcwd()
        self.tasks = load_tasks(config)
        self.db = None
        self.mgr = None
        self.store = None

    def _render_prompt(self, task: EvalTask) -> str:
        return render_prompt(task, self.config.run.scoring.fixtures_path)

    async def _capture_and_save(self, client, arm_name, session_id, task_id, event_queue, started_at):
        try:
            snapshot = await capture_snapshot(client, session_id, event_queue, started_at)
            self.store.write_snapshot(arm_name, task_id, snapshot)
            return snapshot, summarize_snapshot(snapshot)
        except Exception:
            log.event("snapshot", "capture failed", arm=arm_name, task_id=task_id, level="warn")
            return None, None

    @staticmethod
    def _extract_structured_output(snapshot) -> dict[str, Any] | None:
        if not snapshot:
            return None
        for msg in reversed(snapshot.messages):
            if msg.info.role != "assistant":
                continue
            for part in msg.parts:
                if part.type in ("tool-invocation", "tool") and part.tool == "StructuredOutput":
                    if isinstance(part.input, dict):
                        return part.input
                if part.type == "text" and part.text:
                    try:
                        return json.loads(part.text)
                    except (json.JSONDecodeError, TypeError):
                        pass
                    for m in _JSON_FENCE_RE.finditer(part.text):
                        try:
                            return json.loads(m.group(1))
                        except (json.JSONDecodeError, TypeError):
                            continue
        return None

    async def _execute_task(self, task: EvalTask, arm: ArmConfig) -> TaskResult:
        started_at = datetime.now(timezone.utc)
        timeout = task.timeout_override or self.config.run.timeouts.task
        session_id = None
        event_queue = None
        demuxer = None

        log.event("task", "starting", arm=arm.name, task_id=task.id,
                  data={"category": task.category, "timeout": timeout})

        handle = await self.mgr.start(arm, self.work_dir, timeout=self.config.run.timeouts.server_start)
        demuxer = EventDemuxer(base_url=f"http://localhost:{handle.port}")
        await demuxer.start()

        try:
            session = await handle.client.create_session(title=f"eval:{arm.name}:{task.id}")
            session_id = session.id

            def _on_event(_sid, evt):
                self.store.write_live_event(arm.name, task.id, evt)

            event_queue = demuxer.subscribe(session_id, on_event=_on_event)

            try:
                await asyncio.wait_for(
                    handle.client.send_message(
                        session_id, self._render_prompt(task),
                        agent=arm.agent,
                        model={"providerID": arm.model.provider, "modelID": arm.model.model}),
                    timeout=timeout)
            except asyncio.TimeoutError:
                await handle.client.abort_session(session_id)
                _, summary = await self._capture_and_save(handle.client, arm.name, session_id, task.id, event_queue, started_at)
                return TaskResult(task_id=task.id, arm=arm.name, status=TaskStatus.TIMEOUT,
                                  timestamp=started_at.isoformat(),
                                  error=f"timed out after {timeout}s", error_type="TimeoutError",
                                  session_summary=summary)

            snapshot, summary = await self._capture_and_save(handle.client, arm.name, session_id, task.id, event_queue, started_at)
            log.event("task", "success", arm=arm.name, task_id=task.id,
                      duration_ms=summary.duration_ms if summary else 0,
                      data={"steps": summary.steps if summary else 0,
                            "cost": summary.cost if summary else 0})
            return TaskResult(task_id=task.id, arm=arm.name, status=TaskStatus.SUCCESS,
                              timestamp=started_at.isoformat(),
                              structured_output=self._extract_structured_output(snapshot),
                              session_summary=summary)

        except Exception as e:
            etype = type(e).__name__
            log.event("task", f"failed: {etype}: {e}", arm=arm.name, task_id=task.id, level="error")
            summary = None
            if session_id and event_queue:
                _, summary = await self._capture_and_save(handle.client, arm.name, session_id, task.id, event_queue, started_at)
            status = (TaskStatus.AGENT_ERROR
                      if "structured" in etype.lower() or "step" in str(e).lower()
                      else TaskStatus.INFRA_ERROR)
            return TaskResult(task_id=task.id, arm=arm.name, status=status,
                              timestamp=started_at.isoformat(),
                              error=str(e), error_type=etype, session_summary=summary)
        finally:
            if session_id and demuxer:
                demuxer.unsubscribe(session_id)
            if demuxer:
                await demuxer.stop()
            await self.mgr.stop(handle.arm)

    async def _run_arm(self, arm: ArmConfig) -> list[TaskResult]:
        completed = self.store.completed_task_ids(arm.name)
        remaining = [t for t in self.tasks if t.id not in completed]
        if not remaining:
            return self.store.read_results(arm.name)

        log.event("arm", "starting", arm=arm.name,
                  data={"remaining": len(remaining), "done": len(completed)})

        sem = asyncio.Semaphore(self.config.run.concurrency)
        done_count = 0

        async def _run_one(task: EvalTask) -> TaskResult:
            nonlocal done_count
            async with sem:
                result = await self._execute_task(task, arm)
                self.store.write_result(result)
                done_count += 1
                log.event("progress", f"{done_count}/{len(remaining)}", arm=arm.name, task_id=task.id,
                          data={"status": result.status.value,
                                "cost": result.session_summary.cost if result.session_summary else 0})
                return result

        results = await asyncio.gather(*[_run_one(t) for t in remaining])
        return list(results)

    async def run(self) -> dict[str, list[TaskResult]]:
        from harness.db import DbClient
        from harness.server import ServerManager

        if not self.tasks:
            log.event("run", "no tasks matched filters", level="warn")
            return {}

        with _db_server() as db_port:
            self.db = DbClient(base_url=f"http://localhost:{db_port}")
            for _ in range(50):
                if self.db.is_alive():
                    break
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError("db server failed to start")

            self.mgr = ServerManager(db=self.db)

            # Check for resumable run
            probe = ResultStore(db=self.db, run_id="probe")
            config_hash = probe.snapshot_config(self.config)
            row = self.db.query_one("SELECT resumable_run(?)", [config_hash])
            resumed_id = row[0] if row and row[0] else None

            if resumed_id:
                run_id = resumed_id
                log.setup(run_id)
                log.event("run", "resuming", data={"run_id": run_id, "config_hash": config_hash})
            else:
                run_id = ResultStore.make_run_id()
                log.setup(run_id)
                log.event("run", "starting", data={
                    "run_id": run_id, "tasks": len(self.tasks),
                    "arms": [a.name for a in self.config.arms],
                    "config_hash": config_hash})

            self.store = ResultStore(db=self.db, run_id=run_id)
            self.store.snapshot_config(self.config, image_hash=self.mgr.image_hash())
            if not resumed_id:
                self.mgr.begin_run(run_id, self.config.arms, len(self.tasks))

            all_results: dict[str, list[TaskResult]] = {}
            try:
                arm_results = await asyncio.gather(
                    *[self._run_arm(arm) for arm in self.config.arms])
                for arm, results in zip(self.config.arms, arm_results):
                    all_results[arm.name] = results
                self.mgr.end_run(run_id, "completed")
            except Exception:
                self.mgr.end_run(run_id, "failed")
                raise

            total = sum(len(v) for v in all_results.values())
            ok = sum(1 for rs in all_results.values() for r in rs if r.status == TaskStatus.SUCCESS)
            cost = sum(r.session_summary.cost for rs in all_results.values() for r in rs if r.session_summary)
            log.event("run", "complete", data={"run_id": run_id, "successes": ok,
                                               "total": total, "cost": round(cost, 4)})
            self.db.close()
        return all_results


@contextmanager
def _db_server():
    import socket
    with socket.socket() as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "harness.db_server:app",
         "--port", str(port), "--log-level", "warning"],
        env={**os.environ, "PYTHONPATH": "."},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        yield port
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()


async def run_eval(config: EvalConfig, work_dir: str | None = None) -> dict[str, list[TaskResult]]:
    """Convenience wrapper for CLI."""
    return await EvalRunner(config, work_dir).run()

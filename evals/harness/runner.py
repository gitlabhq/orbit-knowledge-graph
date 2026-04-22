"""
Core orchestration loop for the eval harness.

Per arm:
  1. Spawn scode + opencode serve on a dedicated port
  2. Health-poll until ready
  3. Start EventDemuxer (single SSE connection)
  4. Execute tasks in batches (asyncio.gather + semaphore)
  5. Capture snapshot + write results after each task
  6. Tear down server

Prompt execution is NOT retried (non-deterministic).
Session create and data extraction GETs are retried via httpx defaults.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from harness.config import ArmConfig, EvalConfig
from harness.opencode import OpenCodeClient
from harness.session import EventDemuxer, capture_snapshot
from harness.store import ResultStore, SessionSummary, TaskResult, TaskStatus, summarize_snapshot

if TYPE_CHECKING:
    from harness.server import ServerManager

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Task loading
# ---------------------------------------------------------------------------

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
    """Load and filter tasks from YAML files matching config paths."""
    tasks: list[EvalTask] = []
    evals_dir = Path(".")

    for pattern in config.run.tasks.paths:
        for path in sorted(evals_dir.glob(pattern)):
            with path.open() as f:
                data = yaml.safe_load(f)
            if not data:
                continue

            task_list = data if isinstance(data, list) else [data]
            for t in task_list:
                tasks.append(EvalTask(
                    id=t["id"],
                    prompt=t["prompt"],
                    category=t["category"],
                    difficulty=t.get("difficulty", "medium"),
                    description=t.get("description", ""),
                    structured_output_schema=t.get("structured_output_schema"),
                    tags=t.get("tags"),
                    timeout_override=t.get("timeout_override"),
                ))

    filt = config.run.tasks.filter
    if filt.categories:
        tasks = [t for t in tasks if t.category in filt.categories]

    difficulty_order = ["easy", "medium", "hard"]
    min_idx = difficulty_order.index(filt.min_difficulty.value)
    tasks = [t for t in tasks if difficulty_order.index(t.difficulty) >= min_idx]

    return tasks


def render_prompt(task: EvalTask, fixtures_path: str) -> str:
    """Render a task prompt, substituting {{param}} from params.json if present."""
    params_file = Path(fixtures_path) / task.id / "params.json"
    if params_file.exists():
        with params_file.open() as f:
            params = json.load(f)
        prompt = task.prompt
        for key, val in params.items():
            prompt = prompt.replace(f"{{{{{key}}}}}", str(val))
        return prompt
    return task.prompt


# ---------------------------------------------------------------------------
# Server lifecycle (delegated to ServerManager)
# ---------------------------------------------------------------------------

# Kept for backward compat / inline usage -- prefer ServerManager for detached mode.


# ---------------------------------------------------------------------------
# Per-task execution
# ---------------------------------------------------------------------------

async def execute_task(
    client: OpenCodeClient,
    demuxer: EventDemuxer,
    task: EvalTask,
    arm: ArmConfig,
    config: EvalConfig,
    store: ResultStore,
) -> TaskResult:
    """Execute a single task against an OpenCode server. Always returns a TaskResult."""
    started_at = datetime.now(timezone.utc)
    timeout = task.timeout_override or config.run.timeouts.task

    session_id: str | None = None
    event_queue = None

    try:
        session = await client.create_session(title=f"eval:{arm.name}:{task.id}")
        session_id = session.id
        event_queue = demuxer.subscribe(session_id)

        prompt = render_prompt(task, config.run.scoring.fixtures_path)

        # Load agent instructions as system prompt
        system_prompt: str | None = None
        agent_path = Path(arm.agent)
        if agent_path.exists():
            system_prompt = agent_path.read_text()

        # Send prompt with timeout -- NOT retried
        try:
            await asyncio.wait_for(
                client.send_message(
                    session_id,
                    prompt,
                    system=system_prompt,
                    model={"providerID": arm.model.provider, "modelID": arm.model.model},
                ),
                timeout=timeout,
            )
        except asyncio.TimeoutError:
            await client.abort_session(session_id)
            snapshot = await capture_snapshot(client, session_id, event_queue, started_at)
            snap_path = store.write_snapshot(task.id, snapshot)
            return TaskResult(
                task_id=task.id,
                arm=arm.name,
                status=TaskStatus.TIMEOUT,
                timestamp=started_at.isoformat(),
                error=f"task timed out after {timeout}s",
                error_type="TimeoutError",
                session_summary=summarize_snapshot(snapshot),
                snapshot_path=snap_path,
            )

        # Capture full snapshot
        snapshot = await capture_snapshot(client, session_id, event_queue, started_at)
        snap_path = store.write_snapshot(task.id, snapshot)

        # Extract structured output from the last assistant message
        structured = _extract_structured_output(snapshot)

        return TaskResult(
            task_id=task.id,
            arm=arm.name,
            status=TaskStatus.SUCCESS,
            timestamp=started_at.isoformat(),
            structured_output=structured,
            session_summary=summarize_snapshot(snapshot),
            snapshot_path=snap_path,
        )

    except Exception as e:
        error_type = type(e).__name__
        logger.error("task %s/%s failed: %s: %s", arm.name, task.id, error_type, e)

        snapshot_path = None
        summary = None
        if session_id and event_queue:
            try:
                snapshot = await capture_snapshot(
                    client, session_id, event_queue, started_at
                )
                snapshot_path = store.write_snapshot(task.id, snapshot)
                summary = summarize_snapshot(snapshot)
            except Exception:
                logger.warning("failed to capture snapshot for %s/%s", arm.name, task.id)

        status = (
            TaskStatus.AGENT_ERROR
            if "structured" in error_type.lower() or "step" in str(e).lower()
            else TaskStatus.INFRA_ERROR
        )

        return TaskResult(
            task_id=task.id,
            arm=arm.name,
            status=status,
            timestamp=started_at.isoformat(),
            error=str(e),
            error_type=error_type,
            session_summary=summary,
            snapshot_path=snapshot_path,
        )

    finally:
        if session_id:
            demuxer.unsubscribe(session_id)
            try:
                await client.delete_session(session_id)
            except Exception:
                pass


def _extract_structured_output(snapshot: Any) -> dict[str, Any] | None:
    """Extract structured output from the last assistant message parts."""
    for msg in reversed(snapshot.messages):
        if msg.info.role != "assistant":
            continue
        for part in msg.parts:
            if part.type == "tool-invocation" and part.tool == "StructuredOutput":
                if isinstance(part.input, dict):
                    return part.input
            if part.type == "text" and part.text:
                try:
                    return json.loads(part.text)
                except (json.JSONDecodeError, TypeError):
                    continue
    return None


# ---------------------------------------------------------------------------
# Run orchestration
# ---------------------------------------------------------------------------

async def run_arm(
    arm: ArmConfig,
    tasks: list[EvalTask],
    config: EvalConfig,
    store: ResultStore,
    work_dir: str,
    mgr: ServerManager | None = None,
) -> list[TaskResult]:
    """Run all tasks for a single arm."""
    from harness.server import ServerManager

    # Skip already-completed tasks (resume support)
    completed = store.completed_task_ids(arm.name)
    remaining = [t for t in tasks if t.id not in completed]
    if not remaining:
        logger.info("arm %s: all %d tasks already completed", arm.name, len(tasks))
        return store.read_results(arm.name)

    logger.info(
        "arm %s: %d tasks remaining (%d already done)",
        arm.name, len(remaining), len(completed),
    )

    if mgr is None:
        mgr = ServerManager()

    handle = await mgr.start(arm, work_dir, timeout=config.run.timeouts.server_start)
    client = handle.client
    demuxer = EventDemuxer(base_url=f"http://localhost:{arm.port}")
    await demuxer.start()

    sem = asyncio.Semaphore(config.run.concurrency)
    results: list[TaskResult] = []
    consecutive_failures = 0

    try:
        for task in remaining:
            async with sem:
                result = await execute_task(client, demuxer, task, arm, config, store)
                store.write_result(result)
                results.append(result)

                if result.status in (TaskStatus.SUCCESS, TaskStatus.TIMEOUT):
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1

                if consecutive_failures >= 10:
                    logger.error(
                        "arm %s: %d consecutive failures, aborting arm",
                        arm.name, consecutive_failures,
                    )
                    break

                logger.info(
                    "arm %s: %s [%s] (%.1fs, $%.4f)",
                    arm.name,
                    task.id,
                    result.status.value,
                    (result.session_summary.duration_ms / 1000) if result.session_summary else 0,
                    result.session_summary.cost if result.session_summary else 0,
                )
    finally:
        await demuxer.stop()
        await mgr.stop(arm.name)

    return results


async def run_eval(config: EvalConfig, work_dir: str | None = None) -> dict[str, list[TaskResult]]:
    """Run the full evaluation across all arms."""
    from harness.server import ServerManager

    work_dir = work_dir or os.getcwd()
    run_id = ResultStore.make_run_id()
    store = ResultStore(config.run.output_dir, run_id)
    tasks = load_tasks(config)

    if not tasks:
        logger.warning("no tasks matched filters")
        return {}

    logger.info("starting eval run %s: %d tasks, %d arms", run_id, len(tasks), len(config.arms))

    mgr = ServerManager()
    mgr.begin_run(run_id, config.arms, len(tasks))

    all_results: dict[str, list[TaskResult]] = {}
    try:
        for arm in config.arms:
            results = await run_arm(arm, tasks, config, store, work_dir, mgr)
            all_results[arm.name] = results
        mgr.end_run(run_id, "completed")
    except Exception:
        mgr.end_run(run_id, "failed")
        raise
    finally:
        await mgr.stop_all()
        mgr.close()

    logger.info("eval run %s complete", run_id)
    return all_results

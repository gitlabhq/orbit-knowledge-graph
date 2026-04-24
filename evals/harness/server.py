"""
Detached OpenCode server manager backed by DuckDB.

All state lives in the shared DuckDB file managed by harness.db.
Connections are short-lived with retry on lock contention.
Log files are on disk at .eval-servers/logs/<arm>.log.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from harness.config import ArmConfig
from harness.db import WORKSPACE_DIR, connect, default_db_path, ensure_schema
from harness.opencode import OpenCodeClient

logger = logging.getLogger(__name__)


@dataclass
class ServerHandle:
    arm: str
    port: int
    pid: int
    proc: subprocess.Popen[bytes] | None
    client: OpenCodeClient
    log_path: Path


class ServerManager:
    """Manages detached OpenCode server processes. State in DuckDB."""

    def __init__(self, workspace: str | Path = WORKSPACE_DIR) -> None:
        self.workspace = Path(workspace)
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / "logs").mkdir(exist_ok=True)
        self.db_path = default_db_path(self.workspace)
        ensure_schema(self.db_path)
        self._handles: dict[str, ServerHandle] = {}

    # -- runs -----------------------------------------------------------------

    def begin_run(self, run_id: str, arms: list[ArmConfig], task_count: int) -> None:
        arm_names = [a.name for a in arms]
        with connect(self.db_path) as db:
            db.execute(
                "INSERT OR REPLACE INTO runs (run_id, started_at, arms, task_count, status) "
                "VALUES (?, ?, ?, ?, 'running')",
                [run_id, datetime.now(timezone.utc), arm_names, task_count],
            )

    def end_run(self, run_id: str, status: str = "completed") -> None:
        with connect(self.db_path) as db:
            db.execute(
                "UPDATE runs SET completed_at = ?, status = ? WHERE run_id = ?",
                [datetime.now(timezone.utc), status, run_id],
            )

    # -- servers --------------------------------------------------------------

    def _set_server(self, arm: str, status: str, port: int, **kw: Any) -> None:
        now = datetime.now(timezone.utc)
        pid = kw.get("pid")
        work_dir = kw.get("work_dir")
        log_path = kw.get("log_path")
        error = kw.get("error")
        started = now if status == "ready" else None
        stopped = now if status in ("stopped", "error") else None

        with connect(self.db_path) as db:
            db.execute(
                "INSERT OR REPLACE INTO servers "
                "(arm, status, port, pid, work_dir, log_path, started_at, stopped_at, error) "
                "VALUES (?, ?, ?, ?, ?, ?, "
                "  COALESCE(?, (SELECT started_at FROM servers WHERE arm = ?)), "
                "  ?, ?)",
                [arm, status, port, pid, work_dir, log_path, started, arm, stopped, error],
            )

    async def start(
        self,
        arm: ArmConfig,
        work_dir: str,
        timeout: float = 30.0,
    ) -> ServerHandle:
        log_path = self.workspace / "logs" / f"{arm.name}.log"

        self._kill_by_arm(arm.name)
        self._set_server(arm.name, "starting", arm.port, work_dir=work_dir,
                         log_path=str(log_path))

        env = {**os.environ, **arm.env}
        cmd = ["opencode", "serve", "--port", str(arm.port), "--print-logs"]
        if shutil.which("scode"):
            cmd = ["scode", "--strict"] + cmd

        log_file = log_path.open("w")
        logger.info("starting server %s on port %d (log: %s)", arm.name, arm.port, log_path)

        proc = subprocess.Popen(
            cmd, env=env, cwd=work_dir,
            stdout=log_file, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

        self._set_server(arm.name, "starting", arm.port, pid=proc.pid,
                         work_dir=work_dir, log_path=str(log_path))

        client = OpenCodeClient(base_url=f"http://localhost:{arm.port}")
        try:
            await client.wait_ready(timeout=timeout)
        except TimeoutError:
            proc.kill()
            proc.wait()
            self._set_server(arm.name, "error", arm.port, pid=proc.pid,
                             error=f"timeout after {timeout}s")
            raise

        self._set_server(arm.name, "ready", arm.port, pid=proc.pid,
                         work_dir=work_dir, log_path=str(log_path))

        handle = ServerHandle(
            arm=arm.name, port=arm.port, pid=proc.pid,
            proc=proc, client=client, log_path=log_path,
        )
        self._handles[arm.name] = handle
        logger.info("server %s ready (pid=%d, port=%d)", arm.name, proc.pid, arm.port)
        return handle

    async def stop(self, arm: str) -> None:
        handle = self._handles.pop(arm, None)
        if handle:
            await handle.client.close()
            if handle.proc and handle.proc.poll() is None:
                _kill_pid(handle.pid)
            self._set_server(arm, "stopped", handle.port, pid=handle.pid)
            return
        self._kill_by_arm(arm)

    async def stop_all(self) -> None:
        for arm in list(self._handles.keys()):
            await self.stop(arm)
        with connect(self.db_path) as db:
            rows = db.execute(
                "SELECT arm, pid FROM servers WHERE status IN ('ready', 'starting')"
            ).fetchall()
        for arm_name, pid in rows:
            if pid and _pid_alive(pid):
                _kill_pid(pid)
            self._set_server(arm_name, "stopped", 0, pid=pid)

    def _kill_by_arm(self, arm: str) -> None:
        with connect(self.db_path, read_only=True) as db:
            row = db.execute(
                "SELECT pid FROM servers WHERE arm = ? AND status IN ('ready', 'starting')", [arm]
            ).fetchone()
        if row and row[0]:
            _kill_pid(row[0])
            self._set_server(arm, "stopped", 0, pid=row[0])
            logger.info("killed existing server %s (pid=%d)", arm, row[0])

    # -- queries (for CLI) ----------------------------------------------------

    def status(self) -> list[dict]:
        with connect(self.db_path, read_only=True) as db:
            rows = db.execute(
                "SELECT arm, status, port, pid, started_at, stopped_at, error, log_path "
                "FROM servers ORDER BY arm"
            ).fetchall()

        results = []
        for arm, st, port, pid, started, stopped, error, log_path in rows:
            if st == "ready" and pid and not _pid_alive(pid):
                st = "dead"
                self._set_server(arm, "dead", port, pid=pid)
            results.append({
                "arm": arm, "status": st, "port": port, "pid": pid,
                "started_at": str(started) if started else None,
                "stopped_at": str(stopped) if stopped else None,
                "error": error, "log_path": log_path,
            })
        return results

    def get_runs(self) -> list[dict]:
        with connect(self.db_path, read_only=True) as db:
            rows = db.execute(
                "SELECT run_id, started_at, completed_at, arms, task_count, status "
                "FROM runs ORDER BY started_at DESC LIMIT 10"
            ).fetchall()
        return [
            {"run_id": r[0], "started_at": str(r[1]),
             "completed_at": str(r[2]) if r[2] else None,
             "arms": r[3], "task_count": r[4], "status": r[5]}
            for r in rows
        ]

    def logs(self, arm: str, tail: int = 50) -> str:
        with connect(self.db_path, read_only=True) as db:
            row = db.execute(
                "SELECT log_path FROM servers WHERE arm = ?", [arm]
            ).fetchone()
        if not row or not row[0]:
            return f"no log path for {arm}"
        log_path = Path(row[0])
        if not log_path.exists():
            return f"log file not found: {log_path}"
        lines = log_path.read_text().splitlines()
        return "\n".join(lines[-tail:])


def _kill_pid(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
        for _ in range(10):
            time.sleep(0.1)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False

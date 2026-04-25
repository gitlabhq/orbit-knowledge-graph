"""
Docker-based OpenCode server manager.

Each arm runs in an isolated container. State tracked in DuckDB
via the db server. Containers expose OpenCode's HTTP API on a
mapped port; the host consumes SSE and proxies results to DuckDB.
"""

from __future__ import annotations

import logging
import os
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from harness.config import ArmConfig
from harness.db import DbClient, WORKSPACE_DIR
from harness.opencode import OpenCodeClient

logger = logging.getLogger(__name__)

EVAL_IMAGE = "gkg-eval"


@dataclass
class ServerHandle:
    arm: str
    port: int
    container_id: str
    client: OpenCodeClient
    log_path: Path


class ServerManager:
    """Manages Docker containers running OpenCode servers."""

    def __init__(self, db: DbClient, workspace: str | Path = WORKSPACE_DIR) -> None:
        self.db = db
        self.workspace = Path(workspace)
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / "logs").mkdir(exist_ok=True)
        self._handles: dict[str, ServerHandle] = {}

    # -- image ----------------------------------------------------------------

    def build_image(self, context_dir: Path | None = None) -> str:
        """Build the eval container image. Returns the image ID."""
        ctx = context_dir or Path("container")
        result = subprocess.run(
            ["docker", "build", "-q", "-t", EVAL_IMAGE, str(ctx)],
            capture_output=True, text=True, check=True,
        )
        image_id = result.stdout.strip()
        logger.info("built image %s (%s)", EVAL_IMAGE, image_id[:12])
        return image_id

    def image_hash(self) -> str | None:
        """Get the current image ID."""
        result = subprocess.run(
            ["docker", "inspect", "--format", "{{.Id}}", EVAL_IMAGE],
            capture_output=True, text=True,
        )
        return result.stdout.strip() if result.returncode == 0 else None

    # -- runs -----------------------------------------------------------------

    def begin_run(self, run_id: str, arms: list[ArmConfig], task_count: int) -> None:
        arm_names = [a.name for a in arms]
        self.db.write(
            "INSERT OR REPLACE INTO runs (run_id, started_at, arms, task_count, status) "
            "VALUES (?, ?, ?, ?, 'running')",
            [run_id, datetime.now(timezone.utc).isoformat(), arm_names, task_count],
        )

    def end_run(self, run_id: str, status: str = "completed") -> None:
        self.db.write(
            "UPDATE runs SET completed_at = ?, status = ? WHERE run_id = ?",
            [datetime.now(timezone.utc).isoformat(), status, run_id],
        )

    # -- servers --------------------------------------------------------------

    def _set_server(self, arm: str, status: str, port: int, **kw: Any) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.db.write(
            "INSERT OR REPLACE INTO servers "
            "(arm, status, port, pid, work_dir, log_path, started_at, stopped_at, error) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                arm, status, port,
                kw.get("pid"), kw.get("work_dir"), kw.get("log_path"),
                now if status == "ready" else None,
                now if status in ("stopped", "error") else None,
                kw.get("error"),
            ],
        )

    async def start(
        self,
        arm: ArmConfig,
        work_dir: str,
        timeout: float = 30.0,
    ) -> ServerHandle:
        log_path = self.workspace / "logs" / f"{arm.name}.log"

        self._kill_container(arm.name)
        self._set_server(arm.name, "starting", arm.port, work_dir=work_dir,
                         log_path=str(log_path))

        env_args = []
        for k, v in arm.env.items():
            env_args.extend(["-e", f"{k}={v}"])

        # Model config
        env_args.extend([
            "-e", f"ANTHROPIC_API_KEY={arm.env.get('ANTHROPIC_API_KEY', os.environ.get('ANTHROPIC_API_KEY', ''))}",
        ])

        container_name = f"eval-{arm.name}"
        cmd = [
            "docker", "run", "--rm", "-d",
            "--name", container_name,
            "-p", f"{arm.port}:4096",
            *env_args,
            EVAL_IMAGE,
        ]

        log_file = log_path.open("w")
        logger.info("starting container %s on port %d", container_name, arm.port)

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            error = result.stderr.strip()
            self._set_server(arm.name, "error", arm.port, error=error)
            raise RuntimeError(f"docker run failed for {arm.name}: {error}")

        container_id = result.stdout.strip()[:12]

        # Stream logs in background
        log_proc = subprocess.Popen(
            ["docker", "logs", "-f", container_name],
            stdout=log_file, stderr=subprocess.STDOUT,
        )

        client = OpenCodeClient(base_url=f"http://localhost:{arm.port}")
        try:
            await client.wait_ready(timeout=timeout)
        except TimeoutError:
            subprocess.run(["docker", "kill", container_name], capture_output=True)
            self._set_server(arm.name, "error", arm.port,
                             error=f"timeout after {timeout}s")
            raise

        self._set_server(arm.name, "ready", arm.port, pid=log_proc.pid,
                         work_dir=work_dir, log_path=str(log_path))

        handle = ServerHandle(
            arm=arm.name, port=arm.port, container_id=container_id,
            client=client, log_path=log_path,
        )
        self._handles[arm.name] = handle
        logger.info("container %s ready (id=%s, port=%d)", arm.name, container_id, arm.port)
        return handle

    async def stop(self, arm: str) -> None:
        handle = self._handles.pop(arm, None)
        if handle:
            await handle.client.close()
        self._kill_container(arm)
        self._set_server(arm, "stopped", handle.port if handle else 0)

    async def stop_all(self) -> None:
        for arm in list(self._handles.keys()):
            await self.stop(arm)

    def _kill_container(self, arm: str) -> None:
        name = f"eval-{arm}"
        result = subprocess.run(
            ["docker", "kill", name], capture_output=True, text=True,
        )
        if result.returncode == 0:
            logger.info("killed container %s", name)

    # -- queries --------------------------------------------------------------

    def status(self) -> list[dict]:
        rows = self.db.query(
            "SELECT arm, status, port, pid, started_at, stopped_at, error, log_path "
            "FROM servers ORDER BY arm"
        )
        return [
            {"arm": r[0], "status": r[1], "port": r[2], "pid": r[3],
             "started_at": r[4], "stopped_at": r[5], "error": r[6], "log_path": r[7]}
            for r in rows
        ]

    def get_runs(self) -> list[dict]:
        rows = self.db.query(
            "SELECT run_id, started_at, completed_at, arms, task_count, status "
            "FROM runs ORDER BY started_at DESC LIMIT 10"
        )
        return [
            {"run_id": r[0], "started_at": r[1], "completed_at": r[2],
             "arms": r[3], "task_count": r[4], "status": r[5]}
            for r in rows
        ]

    def logs(self, arm: str, tail: int = 50) -> str:
        row = self.db.query_one(
            "SELECT log_path FROM servers WHERE arm = ?", [arm]
        )
        if not row or not row[0]:
            return f"no log path for {arm}"
        log_path = Path(row[0])
        if not log_path.exists():
            return f"log file not found: {log_path}"
        lines = log_path.read_text().splitlines()
        return "\n".join(lines[-tail:])

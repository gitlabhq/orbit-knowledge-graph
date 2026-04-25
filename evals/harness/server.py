"""Docker-based OpenCode server manager. State tracked in DuckDB via db server."""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from harness.config import ArmConfig
from harness.db import DbClient, WORKSPACE_DIR
from harness.opencode import OpenCodeClient

logger = logging.getLogger(__name__)

EVAL_IMAGE = "gkg-eval"


PROVIDER_HOSTS = {
    "anthropic": "api.anthropic.com",
    "openai": "api.openai.com",
    "google": "generativelanguage.googleapis.com",
}


def _provider_host(provider: str) -> str | None:
    return PROVIDER_HOSTS.get(provider)


def _resolve_host(hostname: str) -> str:
    """Resolve hostname to IP for --add-host. Falls back to hostname if resolution fails."""
    import socket
    try:
        return socket.getaddrinfo(hostname, None, socket.AF_INET)[0][4][0]
    except (socket.gaierror, IndexError):
        return hostname


@dataclass
class ServerHandle:
    arm: str
    port: int
    container_id: str
    client: OpenCodeClient
    log_path: Path


class ServerManager:
    def __init__(self, db: DbClient, workspace: str | Path = WORKSPACE_DIR) -> None:
        self.db = db
        self.workspace = Path(workspace)
        self.workspace.mkdir(parents=True, exist_ok=True)
        (self.workspace / "logs").mkdir(exist_ok=True)
        self._handles: dict[str, ServerHandle] = {}

    def build_image(self, context_dir: Path | None = None) -> str:
        result = subprocess.run(
            ["docker", "build", "-q", "-t", EVAL_IMAGE, str(context_dir or Path("container"))],
            capture_output=True, text=True, check=True,
        )
        return result.stdout.strip()

    def image_hash(self) -> str | None:
        r = subprocess.run(["docker", "inspect", "--format", "{{.Id}}", EVAL_IMAGE],
                           capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else None

    def begin_run(self, run_id: str, arms: list[ArmConfig], task_count: int) -> None:
        self.db.write(
            "INSERT OR REPLACE INTO runs (run_id,started_at,arms,task_count,status) VALUES (?,?,?,?,'running')",
            [run_id, datetime.now(timezone.utc).isoformat(), [a.name for a in arms], task_count],
        )

    def end_run(self, run_id: str, status: str = "completed") -> None:
        self.db.write("UPDATE runs SET completed_at=?,status=? WHERE run_id=?",
                      [datetime.now(timezone.utc).isoformat(), status, run_id])

    def _set_server(self, arm: str, status: str, port: int, **kw) -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.db.write(
            "INSERT OR REPLACE INTO servers (arm,status,port,pid,work_dir,log_path,started_at,stopped_at,error) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            [arm, status, port, kw.get("pid"), kw.get("work_dir"), kw.get("log_path"),
             now if status == "ready" else None,
             now if status in ("stopped", "error") else None,
             kw.get("error")],
        )

    async def start(self, arm: ArmConfig, work_dir: str, timeout: float = 30.0) -> ServerHandle:
        log_path = self.workspace / "logs" / f"{arm.name}.log"
        self._kill_container(arm.name)
        self._set_server(arm.name, "starting", arm.port, work_dir=work_dir, log_path=str(log_path))

        env_args = [x for k, v in arm.env.items() for x in ("-e", f"{k}={v}")]
        name = f"eval-{arm.name}"
        workspace = str((Path(work_dir) / "container").resolve())
        # Network allowlist: gitlab host + model provider
        allowed_hosts = {
            arm.env.get("GITLAB_HOST", "staging.gitlab.com"),
            _provider_host(arm.model.provider),
        }
        host_args = [x for h in allowed_hosts if h
                     for x in ("--add-host", f"{h}:{_resolve_host(h)}")]
        r = subprocess.run(
            ["docker", "run", "--rm", "-d", "--name", name,
             "-p", f"{arm.port}:4096",
             "-v", f"{workspace}:/mnt/workspace:ro",
             *host_args, "--dns", "0.0.0.0",
             *env_args, EVAL_IMAGE],
            capture_output=True, text=True,
        )
        if r.returncode != 0:
            self._set_server(arm.name, "error", arm.port, error=r.stderr.strip())
            raise RuntimeError(f"docker run failed for {arm.name}: {r.stderr.strip()}")

        container_id = r.stdout.strip()[:12]
        log_proc = subprocess.Popen(["docker", "logs", "-f", name],
                                    stdout=log_path.open("w"), stderr=subprocess.STDOUT)

        client = OpenCodeClient(base_url=f"http://localhost:{arm.port}")
        try:
            await client.wait_ready(timeout=timeout)
        except TimeoutError:
            subprocess.run(["docker", "kill", name], capture_output=True)
            self._set_server(arm.name, "error", arm.port, error=f"timeout after {timeout}s")
            raise

        self._set_server(arm.name, "ready", arm.port, pid=log_proc.pid,
                         work_dir=work_dir, log_path=str(log_path))
        handle = ServerHandle(arm=arm.name, port=arm.port, container_id=container_id,
                              client=client, log_path=log_path)
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
        for arm in list(self._handles):
            await self.stop(arm)

    def _kill_container(self, arm: str) -> None:
        subprocess.run(["docker", "kill", f"eval-{arm}"], capture_output=True)

    def status(self) -> list[dict]:
        cols = ["arm", "status", "port", "pid", "started_at", "stopped_at", "error", "log_path"]
        return [dict(zip(cols, r)) for r in self.db.query("SELECT * FROM servers ORDER BY arm")]

    def get_runs(self) -> list[dict]:
        cols = ["run_id", "started_at", "completed_at", "arms", "task_count", "status"]
        return [dict(zip(cols, r)) for r in self.db.query(
            "SELECT run_id,started_at,completed_at,arms,task_count,status FROM runs ORDER BY started_at DESC LIMIT 10")]

    def logs(self, arm: str, tail: int = 50) -> str:
        row = self.db.query_one("SELECT log_path FROM servers WHERE arm=?", [arm])
        if not row or not row[0]:
            return f"no log path for {arm}"
        p = Path(row[0])
        return "\n".join(p.read_text().splitlines()[-tail:]) if p.exists() else f"log not found: {p}"

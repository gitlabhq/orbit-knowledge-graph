"""CLI entry point for the GKG eval harness."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path

import click

from harness.config import load_config


def _setup_logging(verbose: bool) -> None:
    if not logging.root.handlers:
        logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO,
                            format="%(asctime)s %(levelname)-5s %(message)s", datefmt="%H:%M:%S")
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


class Ctx:
    """Shared CLI context. Lazily creates db client and store."""

    def __init__(self, config_path: str) -> None:
        self.config_path = config_path
        self._config = None
        self._db = None

    @property
    def config(self):
        if not self._config:
            self._config = load_config(self.config_path)
        return self._config

    @property
    def db(self):
        if not self._db:
            from harness.db import get_client
            self._db = get_client()
        return self._db

    def store(self, run_id: str | None = None):
        from harness.store import ResultStore, list_run_ids
        if not run_id:
            ids = list_run_ids(self.db)
            if not ids:
                click.echo("error: no runs found", err=True)
                sys.exit(1)
            run_id = ids[0]
        return ResultStore(db=self.db, run_id=run_id)


@click.group()
@click.option("--config", "config_path", default="eval.yaml")
@click.option("-v", "--verbose", is_flag=True)
@click.pass_context
def cli(ctx: click.Context, config_path: str, verbose: bool) -> None:
    """GKG Agent Evaluation Harness."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["ctx"] = Ctx(config_path)
    ctx.obj["verbose"] = verbose


@cli.command()
@click.option("--arm", "arm_name", default=None)
@click.option("--resume", "run_id", default=None)
@click.option("--bg", is_flag=True, help="Run detached in background")
@click.pass_context
def run(ctx: click.Context, arm_name: str | None, run_id: str | None, bg: bool) -> None:
    """Run the evaluation."""
    c: Ctx = ctx.obj["ctx"]
    config = c.config
    if arm_name:
        arms = [a for a in config.arms if a.name == arm_name]
        if not arms:
            click.echo(f"error: arm {arm_name!r} not found", err=True)
            sys.exit(1)
        config = config.model_copy(update={"arms": arms})

    if bg:
        import subprocess as _sp
        cmd = [sys.executable, "-m", "harness.cli", "--config", c.config_path, "run"]
        if arm_name: cmd.extend(["--arm", arm_name])
        if run_id: cmd.extend(["--resume", run_id])
        if ctx.obj.get("verbose"): cmd.insert(3, "-v")
        log_path = Path(".eval-servers") / "run.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        proc = _sp.Popen(cmd, stdout=log_path.open("w"), stderr=_sp.STDOUT,
                         start_new_session=True, cwd=os.getcwd())
        click.echo(f"eval running (pid={proc.pid}, log={log_path})")
        return

    from harness.runner import run_eval
    results = asyncio.run(run_eval(config))
    total = sum(len(v) for v in results.values())
    ok = sum(1 for rs in results.values() for r in rs if r.status.value == "success")
    click.echo(f"\n{ok}/{total} succeeded")


@cli.command()
@click.option("--run-id", default=None)
@click.pass_context
def score(ctx: click.Context, run_id: str | None) -> None:
    """Score evaluation results."""
    c: Ctx = ctx.obj["ctx"]
    from harness.evaluators import load_evaluators
    store = c.store(run_id)
    click.echo(f"scoring run {store.run_id}")

    for arm_cfg in c.config.arms:
        arm_scores = []
        evaluators = load_evaluators(c.config.evaluators)
        for r in store.read_results(arm_cfg.name):
            if r.status.value != "success":
                continue
            snapshot = store.read_snapshot(arm_cfg.name, r.task_id)
            fixture_path = Path(c.config.run.scoring.fixtures_path) / r.task_id / "expected.json"
            fixture = json.loads(fixture_path.read_text()) if fixture_path.exists() else None
            arm_scores.append({"task_id": r.task_id,
                               "scores": {ev.name: ev.evaluate(r, snapshot, fixture) for ev in evaluators}})
        store.write_scores(arm_cfg.name, arm_scores)
    click.echo(f"scores written (run_id={store.run_id})")


@cli.command()
@click.option("--run-id", default=None)
@click.pass_context
def report(ctx: click.Context, run_id: str | None) -> None:
    """Generate report from scored run."""
    c: Ctx = ctx.obj["ctx"]
    from harness.report import generate_report
    store = c.store(run_id)
    generate_report(c.config, store.run_id, store)
    click.echo(f"report generated for run {store.run_id}")


@cli.command("dry-run")
@click.option("--check-infra", is_flag=True)
@click.pass_context
def dry_run(ctx: click.Context, check_infra: bool) -> None:
    """Validate config and tasks without running."""
    c: Ctx = ctx.obj["ctx"]
    errors = []
    config = c.config
    click.echo(f"[ok] config: {c.config_path}")

    container = Path("container")
    for arm in config.arms:
        agent_path = container / ".opencode" / "agents" / f"{arm.agent}.md"
        if not agent_path.exists(): errors.append(f"agent not found: {agent_path}")
        for s in arm.skills:
            skill_path = container / ".opencode" / "skills" / s / "SKILL.md"
            if not skill_path.exists(): errors.append(f"skill not found: {skill_path}")
    click.echo(f"[{'FAIL' if errors else 'ok'}] file refs ({len(errors)} errors)")

    from harness.runner import load_tasks
    tasks = load_tasks(config)
    click.echo(f"[ok] {len(tasks)} tasks loaded")

    fixtures = Path(config.run.scoring.fixtures_path)
    missing = [t for t in tasks if not (fixtures / t.id / "expected.json").exists()]
    click.echo(f"[{'warn' if missing else 'ok'}] fixtures ({len(missing)} missing)")

    import subprocess as _sp
    if _sp.run(["docker", "info"], capture_output=True).returncode == 0:
        click.echo("[ok] docker")
    else:
        errors.append("docker not available")

    click.echo(f"\n{len(config.arms)} arms, {len(tasks)} tasks")
    if errors:
        click.echo(f"\n{len(errors)} errors"); sys.exit(1)


@cli.command()
@click.option("--logs", "log_arm", default=None)
@click.option("--tail", default=50)
@click.option("--kill", "kill_arm", default=None, is_flag=False, flag_value="__all__")
@click.option("--runs", "show_runs", is_flag=True)
def servers(log_arm, tail, kill_arm, show_runs) -> None:
    """Manage eval containers."""
    from harness.db import get_client
    from harness.server import ServerManager
    mgr = ServerManager(db=get_client())

    if kill_arm:
        asyncio.run(mgr.stop_all() if kill_arm == "__all__" else mgr.stop(kill_arm))
        click.echo("stopped"); return
    if log_arm:
        click.echo(mgr.logs(log_arm, tail=tail)); return
    if show_runs:
        for r in mgr.get_runs():
            click.echo(f"  {r['run_id']}  {r['status']:<10}  {r['started_at']}")
        return
    for s in mgr.status():
        click.echo(f"  {s['arm']:<12} {s['status']:<10} :{s['port']}  {s.get('started_at') or '-'}")


def main() -> None:
    cli()

if __name__ == "__main__":
    main()

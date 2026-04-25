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


def _get_db():
    from harness.db import DbClient
    db = DbClient()
    if db.is_alive():
        return db
    from harness.db import default_db_path, ensure_schema, direct_connect
    db_path = default_db_path()
    ensure_schema(db_path)

    class _Direct:
        def write(self, sql, params=None):
            with direct_connect(db_path) as c: c.execute(sql, params or [])
        def write_batch(self, stmts):
            with direct_connect(db_path) as c:
                for s in stmts: c.execute(s["sql"], s.get("params", []))
        def query(self, sql, params=None):
            with direct_connect(db_path, read_only=True) as c:
                return [list(r) for r in c.execute(sql, params or []).fetchall()]
        def query_one(self, sql, params=None):
            rows = self.query(sql, params)
            return rows[0] if rows else None
        def is_alive(self): return True

    return _Direct()


def _resolve_run_id(db, run_id: str | None) -> str:
    if run_id:
        return run_id
    from harness.store import list_run_ids
    ids = list_run_ids(db)
    if not ids:
        click.echo("error: no runs found", err=True)
        sys.exit(1)
    return ids[0]


@click.group()
@click.option("--config", "config_path", default="eval.yaml")
@click.option("-v", "--verbose", is_flag=True)
@click.pass_context
def cli(ctx: click.Context, config_path: str, verbose: bool) -> None:
    """GKG Agent Evaluation Harness."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path
    ctx.obj["verbose"] = verbose


@cli.command()
@click.option("--arm", "arm_name", default=None)
@click.option("--resume", "run_id", default=None)
@click.option("--bg", is_flag=True, help="Run detached in background")
@click.pass_context
def run(ctx: click.Context, arm_name: str | None, run_id: str | None, bg: bool) -> None:
    """Run the evaluation."""
    config = load_config(ctx.obj["config_path"])
    if arm_name:
        arms = [a for a in config.arms if a.name == arm_name]
        if not arms:
            click.echo(f"error: arm {arm_name!r} not found", err=True)
            sys.exit(1)
        config = config.model_copy(update={"arms": arms})

    if bg:
        import subprocess as _sp
        cmd = [sys.executable, "-m", "harness.cli", "--config", ctx.obj["config_path"], "run"]
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
    config = load_config(ctx.obj["config_path"])
    from harness.evaluators import load_evaluators
    from harness.store import ResultStore

    db = _get_db()
    run_id = _resolve_run_id(db, run_id)
    store = ResultStore(db=db, run_id=run_id)
    click.echo(f"scoring run {run_id}")

    evaluators = load_evaluators(config.evaluators)
    for arm_cfg in config.arms:
        arm_scores = []
        for r in store.read_results(arm_cfg.name):
            if r.status.value != "success":
                continue
            snapshot = store.read_snapshot(arm_cfg.name, r.task_id)
            fixture_path = Path(config.run.scoring.fixtures_path) / r.task_id / "expected.json"
            fixture = json.loads(fixture_path.read_text()) if fixture_path.exists() else None
            arm_scores.append({"task_id": r.task_id,
                               "scores": {ev.name: ev.evaluate(r, snapshot, fixture) for ev in evaluators}})
        store.write_scores(arm_cfg.name, arm_scores)
    click.echo(f"scores written (run_id={run_id})")


@cli.command()
@click.option("--run-id", default=None)
@click.pass_context
def report(ctx: click.Context, run_id: str | None) -> None:
    """Generate report from scored run."""
    config = load_config(ctx.obj["config_path"])
    from harness.report import generate_report
    from harness.store import ResultStore
    db = _get_db()
    run_id = _resolve_run_id(db, run_id)
    generate_report(config, run_id, ResultStore(db=db, run_id=run_id))
    click.echo(f"report generated for run {run_id}")


@cli.command("dry-run")
@click.option("--check-infra", is_flag=True)
@click.pass_context
def dry_run(ctx: click.Context, check_infra: bool) -> None:
    """Validate config and tasks without running."""
    errors, warnings = [], []
    try:
        config = load_config(ctx.obj["config_path"])
        click.echo(f"[ok] config: {ctx.obj['config_path']}")
    except Exception as e:
        click.echo(f"[FAIL] {e}", err=True); sys.exit(1)

    for arm in config.arms:
        if not Path(arm.agent).exists(): errors.append(f"agent not found: {arm.agent}")
        for s in arm.skills:
            if not (Path(s) / "SKILL.md").exists(): errors.append(f"skill not found: {s}/SKILL.md")
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

    click.echo(f"\n{len(config.arms)} arms, {len(tasks)} tasks, ports {','.join(str(a.port) for a in config.arms)}")
    if errors:
        click.echo(f"\n{len(errors)} errors"); sys.exit(1)


@cli.command()
@click.option("--logs", "log_arm", default=None)
@click.option("--tail", default=50)
@click.option("--kill", "kill_arm", default=None, is_flag=False, flag_value="__all__")
@click.option("--runs", "show_runs", is_flag=True)
def servers(log_arm, tail, kill_arm, show_runs) -> None:
    """Manage eval containers."""
    from harness.server import ServerManager
    db = _get_db()
    mgr = ServerManager(db=db)

    if kill_arm:
        asyncio.run(mgr.stop_all() if kill_arm == "__all__" else mgr.stop(kill_arm))
        click.echo("stopped")
        return
    if log_arm:
        click.echo(mgr.logs(log_arm, tail=tail))
        return
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

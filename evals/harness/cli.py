"""
CLI entry point for the GKG eval harness.

Commands:
  run       Run full evaluation (or --arm to run a single arm)
  score     Score latest run (or --run-id for a specific run)
  report    Generate report from scored run
  dry-run   Validate config and tasks without running
"""

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
        level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)-5s %(message)s",
            datefmt="%H:%M:%S",
        )
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)


def _get_db():
    """Get a DbClient. Uses db server if running, direct mode otherwise."""
    from harness.db import DbClient
    db = DbClient()
    if db.is_alive():
        return db
    # Fallback to direct mode for offline CLI commands
    from harness.db import default_db_path, ensure_schema, direct_connect
    db_path = default_db_path()
    ensure_schema(db_path)
    return _DirectDbAdapter(db_path)


class _DirectDbAdapter:
    """Adapts direct DuckDB connection to the DbClient interface for offline use."""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def write(self, sql: str, params: list | None = None) -> None:
        from harness.db import direct_connect
        with direct_connect(self._db_path) as conn:
            conn.execute(sql, params or [])

    def write_batch(self, statements: list) -> None:
        from harness.db import direct_connect
        with direct_connect(self._db_path) as conn:
            for stmt in statements:
                conn.execute(stmt["sql"], stmt.get("params", []))

    def query(self, sql: str, params: list | None = None) -> list:
        from harness.db import direct_connect
        with direct_connect(self._db_path, read_only=True) as conn:
            return conn.execute(sql, params or []).fetchall()

    def query_one(self, sql: str, params: list | None = None) -> list | None:
        rows = self.query(sql, params)
        return list(rows[0]) if rows else None

    def is_alive(self) -> bool:
        return True


@click.group()
@click.option("--config", "config_path", default="eval.yaml", help="Path to eval config")
@click.option("-v", "--verbose", is_flag=True, help="Verbose output")
@click.pass_context
def cli(ctx: click.Context, config_path: str, verbose: bool) -> None:
    """GKG Agent Evaluation Harness."""
    _setup_logging(verbose)
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path
    ctx.obj["verbose"] = verbose


@cli.command()
@click.option("--arm", "arm_name", default=None, help="Run a single arm only")
@click.option("--resume", "run_id", default=None, help="Resume a previous run by ID")
@click.option("--bg", is_flag=True, help="Run detached in background")
@click.pass_context
def run(ctx: click.Context, arm_name: str | None, run_id: str | None, bg: bool) -> None:
    """Run the evaluation."""
    config = load_config(ctx.obj["config_path"])

    if arm_name:
        arm_names = [a.name for a in config.arms]
        if arm_name not in arm_names:
            click.echo(f"error: arm {arm_name!r} not found (available: {arm_names})", err=True)
            sys.exit(1)
        config = config.model_copy(
            update={"arms": [a for a in config.arms if a.name == arm_name]}
        )

    if bg:
        import subprocess as _sp
        cmd = [sys.executable, "-m", "harness.cli"]
        if ctx.obj.get("verbose"):
            cmd.append("-v")
        cmd.extend(["--config", ctx.obj["config_path"], "run"])
        if arm_name:
            cmd.extend(["--arm", arm_name])
        if run_id:
            cmd.extend(["--resume", run_id])

        log_path = Path(".eval-servers") / "run.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_file = log_path.open("w")
        proc = _sp.Popen(
            cmd, stdout=log_file, stderr=_sp.STDOUT,
            start_new_session=True, cwd=os.getcwd(),
        )
        click.echo(f"eval running in background (pid={proc.pid}, log={log_path})")
        click.echo(f"  tail -f {log_path}")
        return

    from harness.runner import run_eval
    results = asyncio.run(run_eval(config))

    total = sum(len(v) for v in results.values())
    successes = sum(
        1 for rs in results.values() for r in rs if r.status.value == "success"
    )
    click.echo(f"\nCompleted: {successes}/{total} tasks succeeded")
    for arm, rs in results.items():
        statuses = {}
        for r in rs:
            statuses[r.status.value] = statuses.get(r.status.value, 0) + 1
        click.echo(f"  {arm}: {statuses}")


@cli.command()
@click.option("--run-id", default=None, help="Score a specific run (default: latest)")
@click.pass_context
def score(ctx: click.Context, run_id: str | None) -> None:
    """Score evaluation results."""
    config = load_config(ctx.obj["config_path"])

    from harness.evaluators import load_evaluators
    from harness.store import ResultStore, list_run_ids

    db = _get_db()

    if run_id is None:
        run_ids = list_run_ids(db)
        if not run_ids:
            click.echo("error: no runs found", err=True)
            sys.exit(1)
        run_id = run_ids[0]

    store = ResultStore(db=db, run_id=run_id)
    click.echo(f"scoring run {run_id}")

    evaluators = load_evaluators(config.evaluators)

    for arm_cfg in config.arms:
        results = store.read_results(arm_cfg.name)
        arm_scores = []
        for result in results:
            if result.status.value != "success":
                continue
            snapshot_data = store.read_snapshot(arm_cfg.name, result.task_id)

            fixture_path = Path(config.run.scoring.fixtures_path) / result.task_id / "expected.json"
            fixture = json.loads(fixture_path.read_text()) if fixture_path.exists() else None

            task_scores = {}
            for ev in evaluators:
                task_scores[ev.name] = ev.evaluate(result, snapshot_data, fixture)
            arm_scores.append({"task_id": result.task_id, "scores": task_scores})

        store.write_scores(arm_cfg.name, arm_scores)

    click.echo(f"scores written to DuckDB (run_id={run_id})")


@cli.command()
@click.option("--run-id", default=None, help="Report on a specific run (default: latest)")
@click.pass_context
def report(ctx: click.Context, run_id: str | None) -> None:
    """Generate report from scored run."""
    config = load_config(ctx.obj["config_path"])

    from harness.store import ResultStore, list_run_ids

    db = _get_db()

    if run_id is None:
        run_ids = list_run_ids(db)
        if not run_ids:
            click.echo("error: no runs found", err=True)
            sys.exit(1)
        run_id = run_ids[0]

    from harness.report import generate_report

    store = ResultStore(db=db, run_id=run_id)
    generate_report(config, run_id, store)
    click.echo(f"report generated for run {run_id}")


@cli.command("dry-run")
@click.option("--check-infra", is_flag=True, help="Also check infrastructure connectivity")
@click.pass_context
def dry_run(ctx: click.Context, check_infra: bool) -> None:
    """Validate config and tasks without running."""
    errors: list[str] = []
    warnings: list[str] = []

    try:
        config = load_config(ctx.obj["config_path"])
        click.echo(f"[ok] config parsed: {ctx.obj['config_path']}")
    except Exception as e:
        click.echo(f"[FAIL] config parse error: {e}", err=True)
        sys.exit(1)

    for arm in config.arms:
        agent_path = Path(arm.agent)
        if not agent_path.exists():
            errors.append(f"agent file not found: {arm.agent}")
        for skill in arm.skills:
            skill_path = Path(skill) / "SKILL.md"
            if not skill_path.exists():
                errors.append(f"skill not found: {skill}/SKILL.md")

    if errors:
        click.echo(f"[FAIL] {len(errors)} file ref errors")
        for e in errors:
            click.echo(f"  - {e}", err=True)
    else:
        click.echo("[ok] all file refs resolve")

    from harness.runner import load_tasks
    try:
        tasks = load_tasks(config)
        click.echo(f"[ok] {len(tasks)} tasks loaded and filtered")
    except Exception as e:
        errors.append(f"task loading failed: {e}")
        tasks = []

    fixtures_path = Path(config.run.scoring.fixtures_path)
    missing_fixtures = 0
    for task in tasks:
        expected = fixtures_path / task.id / "expected.json"
        if not expected.exists():
            missing_fixtures += 1
            warnings.append(f"missing fixture: {expected}")
    if missing_fixtures:
        click.echo(f"[warn] {missing_fixtures} tasks missing fixtures")
    else:
        click.echo(f"[ok] all {len(tasks)} task fixtures found")

    from harness.evaluators import load_evaluators
    from harness.aggregators import load_aggregators
    try:
        evs = load_evaluators(config.evaluators)
        click.echo(f"[ok] {len(evs)} evaluators loaded")
    except Exception as e:
        errors.append(f"evaluator loading failed: {e}")

    try:
        aggs = load_aggregators(config.aggregators)
        click.echo(f"[ok] {len(aggs)} aggregators loaded")
    except Exception as e:
        errors.append(f"aggregator loading failed: {e}")

    # Check Docker
    import subprocess as _sp
    docker_ok = _sp.run(["docker", "info"], capture_output=True).returncode == 0
    if docker_ok:
        click.echo("[ok] docker available")
    else:
        errors.append("docker not available")

    # Check infrastructure
    if check_infra:
        click.echo("\nchecking infrastructure...")
        import httpx
        for arm in config.arms:
            gitlab_host = arm.env.get("GITLAB_HOST")
            if gitlab_host:
                try:
                    r = httpx.get(f"https://{gitlab_host}/api/v4/version", timeout=5.0)
                    click.echo(f"  [{arm.name}] {gitlab_host}: reachable (HTTP {r.status_code})")
                except Exception as e:
                    warnings.append(f"{gitlab_host} unreachable: {e}")
                    click.echo(f"  [{arm.name}] {gitlab_host}: unreachable")

    click.echo(f"\n{'='*50}")
    click.echo(f"arms:        {len(config.arms)} ({', '.join(a.name for a in config.arms)})")
    click.echo(f"tasks:       {len(tasks)}")
    click.echo(f"evaluators:  {len(config.evaluators)}")
    click.echo(f"aggregators: {len(config.aggregators)}")
    click.echo(f"ports:       {', '.join(str(a.port) for a in config.arms)}")

    if errors:
        click.echo(f"\n{len(errors)} errors, {len(warnings)} warnings")
        sys.exit(1)
    elif warnings:
        click.echo(f"\n0 errors, {len(warnings)} warnings")
    else:
        click.echo("\nall checks passed")


@cli.command()
@click.option("--logs", "log_arm", default=None, help="Tail logs for a specific arm")
@click.option("--tail", default=50, help="Number of log lines to show")
@click.option("--kill", "kill_arm", default=None, is_flag=False, flag_value="__all__",
              help="Kill containers. No value = all, or specify arm name")
@click.option("--runs", "show_runs", is_flag=True, help="Show recent eval runs")
def servers(log_arm: str | None, tail: int, kill_arm: str | None, show_runs: bool) -> None:
    """Manage eval containers."""
    db = _get_db()

    from harness.server import ServerManager
    mgr = ServerManager(db=db)

    if kill_arm:
        if kill_arm == "__all__":
            asyncio.run(mgr.stop_all())
            click.echo("all containers stopped")
        else:
            asyncio.run(mgr.stop(kill_arm))
            click.echo(f"container {kill_arm} stopped")
        return

    if log_arm:
        click.echo(mgr.logs(log_arm, tail=tail))
        return

    if show_runs:
        runs = mgr.get_runs()
        if not runs:
            click.echo("no runs recorded")
        else:
            click.echo(f"{'run_id':<22} {'status':<12} {'arms':<20} {'tasks':<6} {'started'}")
            click.echo("-" * 80)
            for r in runs:
                arms_str = ",".join(r["arms"]) if r["arms"] else "-"
                click.echo(
                    f"{r['run_id']:<22} {r['status']:<12} {arms_str:<20} "
                    f"{r['task_count'] or '-':<6} {r['started_at']}"
                )
        return

    statuses = mgr.status()
    if not statuses:
        click.echo("no containers tracked")
    else:
        click.echo(f"{'arm':<12} {'status':<10} {'port':<7} {'started'}")
        click.echo("-" * 55)
        for s in statuses:
            click.echo(
                f"{s['arm']:<12} {s['status']:<10} {s['port']:<7} "
                f"{s['started_at'] or '-'}"
            )


def main() -> None:
    cli()


if __name__ == "__main__":
    main()

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
import importlib
import json
import logging
import os
import shutil
import sys
from pathlib import Path

import click
import yaml

from harness.config import EvalConfig, load_config


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


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
        click.echo(f"  gkg-eval servers")
        click.echo(f"  gkg-eval servers --logs orbit")
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
    output_dir = Path(config.run.output_dir)

    if run_id is None:
        runs = sorted(output_dir.iterdir()) if output_dir.exists() else []
        if not runs:
            click.echo("error: no runs found", err=True)
            sys.exit(1)
        run_id = runs[-1].name

    click.echo(f"scoring run {run_id}")

    from harness.evaluators import load_evaluators
    from harness.store import ResultStore

    store = ResultStore(config.run.output_dir, run_id)
    evaluators = load_evaluators(config.evaluators)

    all_scores: dict[str, list[dict]] = {}
    for arm_cfg in config.arms:
        results = store.read_results(arm_cfg.name)
        arm_scores = []
        for result in results:
            if result.status.value != "success":
                continue
            snapshot_path = output_dir / run_id / result.snapshot_path if result.snapshot_path else None
            snapshot_data = None
            if snapshot_path and snapshot_path.exists():
                snapshot_data = json.loads(snapshot_path.read_text())

            fixture_path = Path(config.run.scoring.fixtures_path) / result.task_id / "expected.json"
            fixture = json.loads(fixture_path.read_text()) if fixture_path.exists() else None

            task_scores = {}
            for ev in evaluators:
                task_scores[ev.name] = ev.evaluate(result, snapshot_data, fixture)
            arm_scores.append({"task_id": result.task_id, "scores": task_scores})

        all_scores[arm_cfg.name] = arm_scores

    scores_path = output_dir / run_id / "scores.json"
    scores_path.write_text(json.dumps(all_scores, indent=2, default=str))
    click.echo(f"scores written to {scores_path}")


@cli.command()
@click.option("--run-id", default=None, help="Report on a specific run (default: latest)")
@click.pass_context
def report(ctx: click.Context, run_id: str | None) -> None:
    """Generate report from scored run."""
    config = load_config(ctx.obj["config_path"])
    output_dir = Path(config.run.output_dir)

    if run_id is None:
        runs = sorted(output_dir.iterdir()) if output_dir.exists() else []
        if not runs:
            click.echo("error: no runs found", err=True)
            sys.exit(1)
        run_id = runs[-1].name

    from harness.report import generate_report

    generate_report(config, run_id)
    click.echo(f"report generated for run {run_id}")


@cli.command("dry-run")
@click.option("--check-infra", is_flag=True, help="Also check infrastructure connectivity")
@click.pass_context
def dry_run(ctx: click.Context, check_infra: bool) -> None:
    """Validate config and tasks without running."""
    errors: list[str] = []
    warnings: list[str] = []

    # 1. Parse + validate config
    try:
        config = load_config(ctx.obj["config_path"])
        click.echo(f"[ok] config parsed: {ctx.obj['config_path']}")
    except Exception as e:
        click.echo(f"[FAIL] config parse error: {e}", err=True)
        sys.exit(1)

    # 2. Resolve file refs
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

    # 3. Load + validate tasks
    from harness.runner import load_tasks
    try:
        tasks = load_tasks(config)
        click.echo(f"[ok] {len(tasks)} tasks loaded and filtered")
    except Exception as e:
        errors.append(f"task loading failed: {e}")
        tasks = []

    # 4. Check fixtures
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

    # 5. Check evaluators/aggregators resolve
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

    # 6. Check tools
    if shutil.which("scode"):
        click.echo("[ok] scode found")
    else:
        warnings.append("scode not found (sandboxing disabled)")
        click.echo("[warn] scode not found")

    if shutil.which("opencode"):
        click.echo("[ok] opencode found")
    else:
        errors.append("opencode not found in PATH")

    # 7. Infrastructure checks
    if check_infra:
        click.echo("\nchecking infrastructure...")
        import httpx

        for arm in config.arms:
            grpc_ep = arm.env.get("GRPC_ENDPOINT")
            if grpc_ep:
                click.echo(f"  [{arm.name}] GRPC_ENDPOINT={grpc_ep} (connectivity not checked)")
            gitlab_host = arm.env.get("GITLAB_HOST")
            if gitlab_host:
                try:
                    r = httpx.get(f"https://{gitlab_host}/api/v4/version", timeout=5.0)
                    click.echo(f"  [{arm.name}] {gitlab_host}: reachable (HTTP {r.status_code})")
                except Exception as e:
                    warnings.append(f"{gitlab_host} unreachable: {e}")
                    click.echo(f"  [{arm.name}] {gitlab_host}: unreachable")

    # Summary
    click.echo(f"\n{'='*50}")
    click.echo(f"arms:        {len(config.arms)} ({', '.join(a.name for a in config.arms)})")
    click.echo(f"tasks:       {len(tasks)}")
    click.echo(f"evaluators:  {len(config.evaluators)}")
    click.echo(f"aggregators: {len(config.aggregators)}")
    click.echo(f"concurrency: {config.run.concurrency}")
    click.echo(f"ports:       {', '.join(str(a.port) for a in config.arms)}")

    est_time = len(tasks) * config.run.timeouts.task / config.run.concurrency * len(config.arms)
    click.echo(f"est. time:   {est_time/60:.0f}min (worst case)")

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
              help="Kill server(s). No value = all, or specify arm name")
@click.option("--runs", "show_runs", is_flag=True, help="Show recent eval runs")
def servers(log_arm: str | None, tail: int, kill_arm: str | None, show_runs: bool) -> None:
    """Manage eval server processes."""
    from harness.server import ServerManager

    mgr = ServerManager()

    if kill_arm:
        if kill_arm == "__all__":
            asyncio.run(mgr.stop_all())
            click.echo("all servers stopped")
        else:
            asyncio.run(mgr.stop(kill_arm))
            click.echo(f"server {kill_arm} stopped")
        mgr.close()
        return

    if log_arm:
        click.echo(mgr.logs(log_arm, tail=tail))
        mgr.close()
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
        mgr.close()
        return

    # Default: show server status
    statuses = mgr.status()
    if not statuses:
        click.echo("no servers tracked")
    else:
        click.echo(f"{'arm':<12} {'status':<10} {'port':<7} {'pid':<8} {'started'}")
        click.echo("-" * 65)
        for s in statuses:
            click.echo(
                f"{s['arm']:<12} {s['status']:<10} {s['port']:<7} "
                f"{s['pid'] or '-':<8} {s['started_at'] or '-'}"
            )
    mgr.close()


def main() -> None:
    cli()


if __name__ == "__main__":
    main()

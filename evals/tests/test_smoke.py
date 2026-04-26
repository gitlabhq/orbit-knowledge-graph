"""
Smoke tests for the eval harness.

Validates the full pipeline without a live OpenCode server:
config loading, task loading, fixture resolution, prompt rendering,
store round-trip, evaluators, aggregators, and report generation.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import pytest

# All tests run from evals/ as cwd
EVALS_DIR = Path(__file__).parent.parent


@pytest.fixture(autouse=True)
def _chdir():
    """Run all tests from the evals/ directory with dummy env vars."""
    orig = os.getcwd()
    os.chdir(EVALS_DIR)
    os.environ.setdefault("ANTHROPIC_API_KEY", "test-key")
    os.environ.setdefault("GITLAB_TOKEN", "test-token")
    os.environ.setdefault("GITLAB_HOST", "gitlab.com")
    yield
    os.chdir(orig)


def _make_test_db(db_path: Path):
    """Create a DirectClient for tests (no server needed)."""
    from harness.db import DirectClient
    return DirectClient(db_path)


@pytest.fixture(autouse=True)
def _set_env(monkeypatch):
    """Set required env vars so config loading doesn't fail."""
    monkeypatch.setenv("GITLAB_TOKEN", "glpat-fake-test-token")
    monkeypatch.setenv("GITLAB_HOST", "gitlab.com")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def test_load_config(self):
        from harness.config import load_config

        cfg = load_config("eval.yaml")
        assert cfg.run.name == "orbit-vs-glab-baseline"
        assert len(cfg.arms) == 2
        assert cfg.arms[0].name == "orbit"
        assert cfg.arms[1].name == "glab"

    def test_env_var_resolution(self):
        from harness.config import load_config

        cfg = load_config("eval.yaml")
        assert cfg.arms[0].env["GITLAB_TOKEN"] == "glpat-fake-test-token"
        assert cfg.arms[0].env["GITLAB_HOST"] == "gitlab.com"

    def test_missing_env_var_raises(self, monkeypatch):
        from harness.config import load_config

        monkeypatch.delenv("GITLAB_TOKEN")
        with pytest.raises(ValueError, match="GITLAB_TOKEN"):
            load_config("eval.yaml")

    def test_evaluators_configured(self):
        from harness.config import load_config

        cfg = load_config("eval.yaml")
        assert "graph" in cfg.evaluators
        assert "efficiency" in cfg.evaluators
        assert "behavior" in cfg.evaluators

    def test_aggregators_configured(self):
        from harness.config import load_config

        cfg = load_config("eval.yaml")
        assert "descriptive" in cfg.aggregators
        assert "comparative" in cfg.aggregators
        assert "distributional" in cfg.aggregators


# ---------------------------------------------------------------------------
# Task loading
# ---------------------------------------------------------------------------


class TestTaskLoading:
    def test_load_all_tasks(self):
        from harness.config import load_config
        from harness.runner import load_tasks

        cfg = load_config("eval.yaml")
        tasks = load_tasks(cfg)
        assert len(tasks) >= 5
        ids = {t.id for t in tasks}
        assert "search-user" in ids
        assert "user-open-mrs" in ids
        assert "mr-count-by-project" in ids
        assert "mr-neighbors" in ids
        assert "path-between-users" in ids

    def test_task_categories(self):
        from harness.config import load_config
        from harness.runner import load_tasks

        cfg = load_config("eval.yaml")
        tasks = load_tasks(cfg)
        categories = {t.category for t in tasks}
        assert "search" in categories
        assert "traversal" in categories
        assert "aggregation" in categories

    def test_prompt_rendering_with_params(self):
        from harness.config import load_config
        from harness.runner import load_tasks, render_prompt

        cfg = load_config("eval.yaml")
        tasks = load_tasks(cfg)
        user_task = next(t for t in tasks if t.id == "search-user")
        rendered = render_prompt(user_task, cfg.run.scoring.fixtures_path)
        assert "root" in rendered
        assert "{{username}}" not in rendered


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


class TestFixtures:
    def test_all_tasks_have_fixtures(self):
        from harness.config import load_config
        from harness.runner import load_tasks

        cfg = load_config("eval.yaml")
        tasks = load_tasks(cfg)
        fixtures_path = Path(cfg.run.scoring.fixtures_path)

        for task in tasks:
            expected = fixtures_path / task.id / "expected.json"
            params = fixtures_path / task.id / "params.json"
            assert expected.exists(), f"missing expected.json for {task.id}"
            assert params.exists(), f"missing params.json for {task.id}"

    def test_fixtures_are_valid_json(self):
        from harness.config import load_config
        from harness.runner import load_tasks

        cfg = load_config("eval.yaml")
        tasks = load_tasks(cfg)
        fixtures_path = Path(cfg.run.scoring.fixtures_path)

        for task in tasks:
            expected = fixtures_path / task.id / "expected.json"
            params = fixtures_path / task.id / "params.json"
            json.loads(expected.read_text())
            json.loads(params.read_text())


# ---------------------------------------------------------------------------
# Store round-trip
# ---------------------------------------------------------------------------


class TestStore:
    def test_write_and_read_result(self):
        from harness.store import ResultStore, SessionSummary, TaskResult, TaskStatus

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            store = ResultStore(db=_make_test_db(db_path), run_id="test-run")
            result = TaskResult(
                task_id="search-user",
                arm="orbit",
                status=TaskStatus.SUCCESS,
                timestamp="2026-04-21T12:00:00Z",
                structured_output={"id": "1", "username": "root"},
                session_summary=SessionSummary(
                    session_id="sess_abc",
                    steps=3,
                    tool_calls=2,
                    tokens={"input": 1000, "output": 500},
                    cost=0.05,
                    duration_ms=5000,
                ),
            )
            store.write_result(result)

            results = store.read_results("orbit")
            assert len(results) == 1
            assert results[0].task_id == "search-user"
            assert results[0].status == TaskStatus.SUCCESS
            assert results[0].structured_output == {"id": "1", "username": "root"}
            assert results[0].session_summary is not None
            assert results[0].session_summary.steps == 3
            assert results[0].session_summary.cost == 0.05

    def test_write_and_read_snapshot(self):
        from unittest.mock import MagicMock

        from harness.session import SessionSnapshot
        from harness.store import ResultStore

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            store = ResultStore(db=_make_test_db(db_path), run_id="test-run")

            session = MagicMock()
            session.id = "sess_123"
            session.model_dump.return_value = {"id": "sess_123"}

            snapshot = SessionSnapshot(
                session=session,
                messages=[],
                children=[],
                diffs=[],
                todos=[],
                events=[{"type": "test", "data": {}}],
                timing={"duration_ms": 1000},
            )
            store.write_snapshot("orbit", "task-1", snapshot)

            loaded = store.read_snapshot("orbit", "task-1")
            assert loaded is not None
            assert loaded["timing"]["duration_ms"] == 1000

    def test_completed_task_ids(self):
        from harness.store import ResultStore, TaskResult, TaskStatus

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            store = ResultStore(db=_make_test_db(db_path), run_id="test-run")
            for tid in ["task-a", "task-b", "task-c"]:
                store.write_result(TaskResult(
                    task_id=tid, arm="orbit", status=TaskStatus.SUCCESS,
                    timestamp="2026-04-21T12:00:00Z",
                ))
            assert store.completed_task_ids("orbit") == {"task-a", "task-b", "task-c"}

    def test_resume_skips_completed(self):
        from harness.store import ResultStore, TaskResult, TaskStatus

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            store = ResultStore(db=_make_test_db(db_path), run_id="test-run")
            store.write_result(TaskResult(
                task_id="done-task", arm="orbit", status=TaskStatus.SUCCESS,
                timestamp="2026-04-21T12:00:00Z",
            ))
            completed = store.completed_task_ids("orbit")
            remaining = [t for t in ["done-task", "new-task"] if t not in completed]
            assert remaining == ["new-task"]

    def test_list_run_ids(self):
        from harness.store import ResultStore, TaskResult, TaskStatus

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            for rid in ["20260421_120000", "20260422_130000"]:
                store = ResultStore(db=_make_test_db(db_path), run_id=rid)
                store.write_result(TaskResult(
                    task_id="t1", arm="orbit", status=TaskStatus.SUCCESS,
                    timestamp="2026-04-21T12:00:00Z",
                ))
            store = ResultStore(db=_make_test_db(db_path), run_id="any")
            run_ids = store.list_run_ids()
            assert "20260421_120000" in run_ids
            assert "20260422_130000" in run_ids

    def test_write_and_read_scores(self):
        from harness.store import ResultStore

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            store = ResultStore(db=_make_test_db(db_path), run_id="test-run")
            store.write_scores("orbit", [
                {"task_id": "t1", "scores": {"graph": [{"name": "correctness", "value": 1.0}]}},
                {"task_id": "t2", "scores": {"graph": [{"name": "correctness", "value": 0.5}]}},
            ])
            scores = store.read_scores()
            assert "orbit" in scores
            assert len(scores["orbit"]) == 2
            assert scores["orbit"][0]["task_id"] == "t1"

    def test_snapshot_config(self):
        from harness.config import load_config
        from harness.store import ResultStore

        config = load_config("eval.yaml")

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "eval.duckdb"
            store = ResultStore(db=_make_test_db(db_path), run_id="run-1")
            hash1 = store.snapshot_config(config)

            assert len(hash1) == 16

            loaded = store.read_config()
            assert loaded is not None
            assert loaded["config_hash"] == hash1
            assert loaded["config_name"] == "orbit-vs-glab-baseline"
            assert loaded["config_version"] == "0.1.0"
            assert loaded["config"]["run"]["name"] == "orbit-vs-glab-baseline"
            assert "container/.opencode/agents/orbit.md" in loaded["files"]
            assert "container/.opencode/agents/glab.md" in loaded["files"]

            # Same config -> same hash
            store2 = ResultStore(db=_make_test_db(db_path), run_id="run-2")
            hash2 = store2.snapshot_config(config)
            assert hash1 == hash2

            # Both runs findable by hash
            runs = store.find_runs_by_config(hash1)
            assert "run-1" in runs
            assert "run-2" in runs


# ---------------------------------------------------------------------------
# Evaluators
# ---------------------------------------------------------------------------


class TestEvaluators:
    def test_load_all_evaluators(self):
        from harness.evaluators import load_evaluators

        evs = load_evaluators(["graph", "efficiency", "behavior"])
        assert len(evs) == 3
        names = {e.name for e in evs}
        assert names == {"graph", "efficiency", "behavior"}

    def test_unknown_evaluator_raises(self):
        from harness.evaluators import load_evaluators

        with pytest.raises(ValueError, match="unknown evaluator"):
            load_evaluators(["nonexistent"])

    def test_graph_evaluator_perfect_match(self):
        from harness.evaluators.graph import GraphEvaluator
        from harness.store import TaskResult, TaskStatus

        ev = GraphEvaluator()
        result = TaskResult(
            task_id="t", arm="a", status=TaskStatus.SUCCESS,
            timestamp="2026-04-21T12:00:00Z",
            structured_output={"rows": [{"id": "1"}, {"id": "2"}]},
        )
        fixture = {"rows": [{"id": "1"}, {"id": "2"}]}
        metrics = ev.evaluate(result, None, fixture)
        correctness = next(m for m in metrics if m.name == "correctness")
        completeness = next(m for m in metrics if m.name == "completeness")
        assert correctness.value == 1.0
        assert completeness.value == 1.0

    def test_graph_evaluator_partial_match(self):
        from harness.evaluators.graph import GraphEvaluator
        from harness.store import TaskResult, TaskStatus

        ev = GraphEvaluator()
        result = TaskResult(
            task_id="t", arm="a", status=TaskStatus.SUCCESS,
            timestamp="2026-04-21T12:00:00Z",
            structured_output={"rows": [{"id": "1"}]},
        )
        fixture = {"rows": [{"id": "1"}, {"id": "2"}]}
        metrics = ev.evaluate(result, None, fixture)
        correctness = next(m for m in metrics if m.name == "correctness")
        completeness = next(m for m in metrics if m.name == "completeness")
        assert correctness.value == 1.0  # precision: 1/1
        assert completeness.value == 0.5  # recall: 1/2

    def test_efficiency_evaluator(self):
        from harness.evaluators.efficiency import EfficiencyEvaluator
        from harness.store import SessionSummary, TaskResult, TaskStatus

        ev = EfficiencyEvaluator()
        result = TaskResult(
            task_id="t", arm="a", status=TaskStatus.SUCCESS,
            timestamp="2026-04-21T12:00:00Z",
            session_summary=SessionSummary(
                session_id="s", steps=4, tool_calls=3,
                tokens={"input": 1000, "output": 500, "cache_read": 200},
                cost=0.12, duration_ms=8000,
            ),
        )
        metrics = ev.evaluate(result, None, None)
        names = {m.name for m in metrics}
        assert "steps" in names
        assert "tool_calls" in names
        assert "cost_usd" in names
        assert "duration_ms" in names

    def test_behavior_evaluator_with_snapshot(self):
        from harness.evaluators.behavior import BehaviorEvaluator
        from harness.store import TaskResult, TaskStatus

        ev = BehaviorEvaluator()
        snapshot = {
            "messages": [
                {
                    "info": {"role": "user"},
                    "parts": [{"type": "text", "text": "find user"}],
                },
                {
                    "info": {"role": "assistant"},
                    "parts": [
                        {"type": "tool-invocation", "tool": "skill", "input": {"name": "orbit-query"}},
                        {"type": "tool-invocation", "tool": "bash", "input": {"command": "python tools/orbit_query.py query"}},
                    ],
                },
            ],
            "events": [],
        }
        result = TaskResult(
            task_id="t", arm="a", status=TaskStatus.SUCCESS,
            timestamp="2026-04-21T12:00:00Z",
        )
        metrics = ev.evaluate(result, snapshot, None)
        skill_loaded = next(m for m in metrics if m.name == "skill_loaded")
        assert skill_loaded.value == 1.0
        query_count = next(m for m in metrics if m.name == "query_count")
        assert query_count.value == 1.0


# ---------------------------------------------------------------------------
# Aggregators
# ---------------------------------------------------------------------------


class TestAggregators:
    def test_load_all_aggregators(self):
        from harness.aggregators import load_aggregators

        aggs = load_aggregators(["descriptive", "comparative", "distributional"])
        assert len(aggs) == 3

    def test_descriptive_aggregator(self):
        from harness.aggregators.descriptive import DescriptiveAggregator

        agg = DescriptiveAggregator()
        scores = {
            "orbit": [
                {"task_id": "t1", "scores": {"graph": [{"name": "correctness", "value": 1.0}]}},
                {"task_id": "t2", "scores": {"graph": [{"name": "correctness", "value": 0.5}]}},
                {"task_id": "t3", "scores": {"graph": [{"name": "correctness", "value": 0.8}]}},
            ],
        }
        results = agg.aggregate(scores)
        assert len(results) == 1
        data = results[0].data
        assert "graph.correctness" in data
        stats = data["graph.correctness"]
        assert stats["count"] == 3
        assert 0.7 < stats["mean"] < 0.8

    def test_comparative_needs_two_arms(self):
        from harness.aggregators.comparative import ComparativeAggregator

        agg = ComparativeAggregator()
        scores = {"orbit": [{"task_id": "t1", "scores": {}}]}
        results = agg.aggregate(scores)
        assert results[0].data.get("note") == "need >= 2 arms to compare"


# ---------------------------------------------------------------------------
# CLI dry-run (invoked as subprocess to test the entry point)
# ---------------------------------------------------------------------------


class TestCLIDryRun:
    def test_dry_run_passes(self):
        """dry-run should pass with valid config and mock fixtures."""
        from click.testing import CliRunner
        from harness.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["dry-run"], catch_exceptions=False)
        assert result.exit_code == 0 or "warn" in result.output.lower()
        assert "[ok] config" in result.output
        assert "tasks loaded" in result.output

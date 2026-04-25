"""Report generation: markdown from scored and aggregated results."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from harness.aggregators import load_aggregators
from harness.config import EvalConfig
from harness.db import WORKSPACE_DIR
from harness.store import ResultStore, TaskStatus


def generate_report(config: EvalConfig, run_id: str, store: ResultStore) -> None:
    scores = store.read_scores()
    if not scores:
        raise ValueError(f"no scores found for run {run_id} -- run 'score' first")

    aggregates: dict[str, Any] = {}
    for agg in load_aggregators(config.aggregators):
        for result in agg.aggregate(scores):
            aggregates[result.name] = result.data

    arm_results = {a.name: store.read_results(a.name) for a in config.arms}
    md = _render_markdown(config, run_id, scores, aggregates, arm_results)

    out = Path(WORKSPACE_DIR)
    out.mkdir(parents=True, exist_ok=True)
    (out / f"report_{run_id}.md").write_text(md)


def _render_markdown(config, run_id, scores, aggregates, arm_results) -> str:
    L = []
    L.append(f"# Eval Report: {config.run.name}")
    L.append(f"\nRun ID: `{run_id}`")
    L.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    L.append("\n## Status Summary\n")
    L.append("| Arm | Success | Timeout | Agent Error | Infra Error | Total |")
    L.append("|-----|---------|---------|-------------|-------------|-------|")
    for arm, results in arm_results.items():
        c = Counter(r.status.value for r in results)
        L.append(f"| {arm} | {c['success']} | {c['timeout']} "
                 f"| {c['agent_error']} | {c['infra_error']} | {len(results)} |")

    for name, data in aggregates.items():
        if name.startswith("descriptive_"):
            arm = name[len("descriptive_"):]
            L.append(f"\n## Descriptive Stats: {arm}\n")
            L.append("| Metric | Mean | Stdev | P50 | P95 | Min | Max |")
            L.append("|--------|------|-------|-----|-----|-----|-----|")
            for metric, s in data.items():
                L.append(f"| {metric} | {s['mean']:.3f} | {s['stdev']:.3f} "
                         f"| {s['p50']:.3f} | {s['p95']:.3f} | {s['min']:.3f} | {s['max']:.3f} |")

    if "comparative" in aggregates:
        L.append("\n## Comparative Analysis\n")
        for pair, metrics in aggregates["comparative"].items():
            L.append(f"\n### {pair}\n")
            L.append("| Metric | Mean A | Mean B | p-value | Significant? |")
            L.append("|--------|--------|--------|---------|-------------|")
            for metric, d in metrics.items():
                if "note" in d:
                    L.append(f"| {metric} | - | - | - | {d['note']} |")
                else:
                    L.append(f"| {metric} | {d['mean_a']:.3f} | {d['mean_b']:.3f} "
                             f"| {d['p_value']:.4f} | {'yes' if d.get('significant_005') else 'no'} |")

    L.append("\n## Error Analysis\n")
    for arm, results in arm_results.items():
        errors = [r for r in results if r.status.value != "success"]
        if not errors:
            L.append(f"**{arm}**: no errors\n")
            continue
        L.append(f"\n### {arm}\n")
        L.append("| Task | Status | Error Type | Error |")
        L.append("|------|--------|------------|-------|")
        for r in errors:
            L.append(f"| {r.task_id} | {r.status.value} | {r.error_type or '-'} | {(r.error or '')[:80]} |")

    return "\n".join(L) + "\n"

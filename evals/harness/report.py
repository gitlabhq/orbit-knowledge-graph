"""
Report generation: markdown from scored and aggregated results.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from harness.aggregators import load_aggregators
from harness.config import EvalConfig
from harness.store import ResultStore, TaskStatus


def generate_report(config: EvalConfig, run_id: str, store: ResultStore) -> None:
    scores = store.read_scores()

    if not scores:
        raise ValueError(f"no scores found for run {run_id} -- run 'score' first")

    aggregators = load_aggregators(config.aggregators)
    all_aggregates: dict[str, Any] = {}
    for agg in aggregators:
        for result in agg.aggregate(scores):
            all_aggregates[result.name] = result.data

    arm_results: dict[str, Any] = {}
    for arm_cfg in config.arms:
        arm_results[arm_cfg.name] = store.read_results(arm_cfg.name)

    md = _render_markdown(config, run_id, scores, all_aggregates, arm_results)

    output_dir = store.db_path.parent
    md_path = output_dir / f"report_{run_id}.md"
    md_path.write_text(md)


def _render_markdown(
    config: EvalConfig,
    run_id: str,
    scores: dict[str, list[dict]],
    aggregates: dict[str, Any],
    arm_results: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append(f"# Eval Report: {config.run.name}")
    lines.append(f"\nRun ID: `{run_id}`")
    lines.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    # Status summary
    lines.append("\n## Status Summary\n")
    lines.append("| Arm | Success | Timeout | Agent Error | Infra Error | Total |")
    lines.append("|-----|---------|---------|-------------|-------------|-------|")
    for arm_name, results in arm_results.items():
        counts = {s.value: 0 for s in TaskStatus}
        for r in results:
            counts[r.status.value] = counts.get(r.status.value, 0) + 1
        total = len(results)
        lines.append(
            f"| {arm_name} | {counts['success']} | {counts['timeout']} "
            f"| {counts['agent_error']} | {counts['infra_error']} | {total} |"
        )

    # Descriptive stats
    for agg_name, agg_data in aggregates.items():
        if agg_name.startswith("descriptive_"):
            arm = agg_name[len("descriptive_"):]
            lines.append(f"\n## Descriptive Stats: {arm}\n")
            lines.append("| Metric | Mean | Stdev | P50 | P95 | Min | Max |")
            lines.append("|--------|------|-------|-----|-----|-----|-----|")
            for metric, stats in agg_data.items():
                lines.append(
                    f"| {metric} | {stats['mean']:.3f} | {stats['stdev']:.3f} "
                    f"| {stats['p50']:.3f} | {stats['p95']:.3f} "
                    f"| {stats['min']:.3f} | {stats['max']:.3f} |"
                )

    # Comparative
    if "comparative" in aggregates:
        comp = aggregates["comparative"]
        lines.append("\n## Comparative Analysis\n")
        for pair, metrics in comp.items():
            lines.append(f"\n### {pair}\n")
            lines.append("| Metric | Mean A | Mean B | p-value | Significant? |")
            lines.append("|--------|--------|--------|---------|-------------|")
            for metric, data in metrics.items():
                if "note" in data:
                    lines.append(f"| {metric} | - | - | - | {data['note']} |")
                else:
                    sig = "yes" if data.get("significant_005") else "no"
                    lines.append(
                        f"| {metric} | {data['mean_a']:.3f} | {data['mean_b']:.3f} "
                        f"| {data['p_value']:.4f} | {sig} |"
                    )

    # Error analysis
    lines.append("\n## Error Analysis\n")
    for arm_name, results in arm_results.items():
        errors = [r for r in results if r.status.value != "success"]
        if not errors:
            lines.append(f"**{arm_name}**: no errors\n")
            continue
        lines.append(f"\n### {arm_name}\n")
        lines.append("| Task | Status | Error Type | Error |")
        lines.append("|------|--------|------------|-------|")
        for r in errors:
            err_msg = (r.error or "")[:80]
            lines.append(f"| {r.task_id} | {r.status.value} | {r.error_type or '-'} | {err_msg} |")

    return "\n".join(lines) + "\n"

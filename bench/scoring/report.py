#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml"]
# ///
"""Generate a markdown report from a scored bench run."""

import json
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent.parent


def main():
    run_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else BENCH_DIR / "results" / "latest"
    tier = sys.argv[2] if len(sys.argv) > 2 else "small"

    slo_file = run_dir / "slo_results.json"
    if not slo_file.exists():
        print(f"No slo_results.json in {run_dir}; run score.py first", file=sys.stderr)
        sys.exit(1)

    verdicts = json.loads(slo_file.read_text())
    phases_file = run_dir / "phases.jsonl"
    phases = []
    if phases_file.exists():
        for line in phases_file.read_text().strip().split("\n"):
            if line.strip():
                phases.append(json.loads(line))

    report = [f"# RA Bench Report: {run_dir.name}", f"Tier: {tier}\n"]

    if phases:
        report.append("## Phases\n")
        report.append("| Phase | Duration (s) |")
        report.append("|-------|-------------|")
        for p in phases:
            dur = p["t_end"] - p["t_start"]
            report.append(f"| {p['phase']} | {dur} |")
        report.append("")

    report.append("## SLO Scorecard\n")
    report.append("| Phase | SLO | Verdict | Detail |")
    report.append("|-------|-----|---------|--------|")
    for v in verdicts:
        detail = v.get("failing_signal") or ""
        if "p90_ms" in v:
            detail = f"p90={v['p90_ms']:.0f}ms, rate={v['achieved_qps']:.1f}/{v['target_qps']}"
        elif "achieved_rps" in v:
            detail = f"rate={v['achieved_rps']:.0f}/{v['target_rps']:.0f}"
        report.append(f"| {v['phase']} | {v['slo']} | {v['verdict']} | {detail} |")

    passes = sum(1 for v in verdicts if v["verdict"] == "PASS")
    fails = sum(1 for v in verdicts if v["verdict"] == "FAIL")
    report.append(f"\n**{passes} passed, {fails} failed**\n")

    out = run_dir / "report.md"
    out.write_text("\n".join(report) + "\n")
    print(f"Report written to {out}")


if __name__ == "__main__":
    main()

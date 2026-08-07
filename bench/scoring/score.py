#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["duckdb", "pyyaml"]
# ///
"""Score a bench run against tier SLOs (GPT threshold semantics)."""

import json
import sys
from pathlib import Path

import duckdb
import yaml

BENCH_DIR = Path(__file__).resolve().parent.parent


def load_tier_slos(tier: str) -> dict:
    tiers = yaml.safe_load((BENCH_DIR / "config" / "tiers.yaml").read_text())
    return tiers["tiers"][tier]["slos"]


def score_query_results(results_file: Path, slos: dict) -> list[dict]:
    if not results_file.exists():
        return []
    steps = json.loads(results_file.read_text())
    verdicts = []
    for step in steps:
        checks = []
        mult = slos.get("achieved_rate_multiplier", 0.8)
        if step["achieved_qps"] < mult * step["target_qps"]:
            checks.append(
                f"achieved {step['achieved_qps']:.1f} < {mult}x{step['target_qps']}"
            )
        if step["success_rate"] < slos.get("query_success_rate", 0.99):
            checks.append(f"success_rate {step['success_rate']:.3f}")
        p90_limit = slos.get("query_p90_ms", 500)
        if step["p90_ms"] > p90_limit:
            checks.append(f"p90 {step['p90_ms']:.0f}ms > {p90_limit}ms")
        verdicts.append(
            {
                "phase": "p4",
                "slo": f"query@{step['target_qps']}qps",
                "verdict": "FAIL" if checks else "PASS",
                "failing_signal": "; ".join(checks) if checks else None,
                "target_qps": step["target_qps"],
                "achieved_qps": step["achieved_qps"],
                "p50_ms": step["p50_ms"],
                "p90_ms": step["p90_ms"],
                "p99_ms": step["p99_ms"],
                "success_rate": step["success_rate"],
            }
        )
    return verdicts


def score_replay_results(results_file: Path, slos: dict, phase: str) -> list[dict]:
    if not results_file.exists():
        return []
    tables = json.loads(results_file.read_text())
    total_target = sum(t["target_rps"] for t in tables)
    total_achieved = sum(t["achieved_rps"] for t in tables)
    mult = slos.get("achieved_rate_multiplier", 0.8)
    checks = []
    if total_achieved < mult * total_target:
        checks.append(f"achieved {total_achieved:.0f} < {mult}x{total_target:.0f}")
    return [
        {
            "phase": phase,
            "slo": "replay_rate",
            "verdict": "FAIL" if checks else "PASS",
            "failing_signal": "; ".join(checks) if checks else None,
            "target_rps": total_target,
            "achieved_rps": total_achieved,
        }
    ]


def main():
    run_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else BENCH_DIR / "results" / "latest"
    tier = sys.argv[2] if len(sys.argv) > 2 else "small"

    slos = load_tier_slos(tier)
    all_verdicts = []

    all_verdicts.extend(
        score_query_results(run_dir / "p4-query-results.json", slos)
    )
    all_verdicts.extend(
        score_replay_results(run_dir / "p3a-replay-results.json", slos, "p3a")
    )

    out = run_dir / "slo_results.json"
    out.write_text(json.dumps(all_verdicts, indent=2))
    print(f"SLO results written to {out}")

    fails = [v for v in all_verdicts if v["verdict"] == "FAIL"]
    if fails:
        print(f"\n{len(fails)} SLO FAILURES:")
        for f in fails:
            print(f"  {f['phase']} {f['slo']}: {f['failing_signal']}")
        sys.exit(1)
    else:
        print(f"\nAll {len(all_verdicts)} SLOs passed")


if __name__ == "__main__":
    main()

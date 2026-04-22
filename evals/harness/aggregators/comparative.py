"""
Comparative aggregator: arm-vs-arm statistical tests.

Runs Mann-Whitney U tests on key metrics between treatment arms.
"""

from __future__ import annotations

from typing import Any

from harness.aggregators.protocol import Aggregate, Aggregator


class ComparativeAggregator(Aggregator):
    @property
    def name(self) -> str:
        return "comparative"

    def aggregate(
        self,
        scores: dict[str, list[dict[str, Any]]],
    ) -> list[Aggregate]:
        arms = list(scores.keys())
        if len(arms) < 2:
            return [Aggregate(name="comparative", data={"note": "need >= 2 arms to compare"})]

        # Collect per-metric values for each arm
        arm_metrics: dict[str, dict[str, list[float]]] = {}
        for arm, task_scores in scores.items():
            arm_metrics[arm] = {}
            for task in task_scores:
                for evaluator_name, metrics in task.get("scores", {}).items():
                    if not isinstance(metrics, list):
                        continue
                    for m in metrics:
                        key = f"{evaluator_name}.{m['name']}"
                        arm_metrics[arm].setdefault(key, []).append(m["value"])

        comparisons: dict[str, Any] = {}
        for i, arm_a in enumerate(arms):
            for arm_b in arms[i + 1 :]:
                pair_key = f"{arm_a}_vs_{arm_b}"
                pair_results: dict[str, Any] = {}

                shared_metrics = set(arm_metrics.get(arm_a, {}).keys()) & set(
                    arm_metrics.get(arm_b, {}).keys()
                )
                for metric in sorted(shared_metrics):
                    vals_a = arm_metrics[arm_a][metric]
                    vals_b = arm_metrics[arm_b][metric]

                    if len(vals_a) < 3 or len(vals_b) < 3:
                        pair_results[metric] = {"note": "insufficient data for test"}
                        continue

                    try:
                        from scipy.stats import mannwhitneyu

                        stat, p_value = mannwhitneyu(vals_a, vals_b, alternative="two-sided")
                        import statistics

                        pair_results[metric] = {
                            "mean_a": statistics.mean(vals_a),
                            "mean_b": statistics.mean(vals_b),
                            "u_statistic": stat,
                            "p_value": p_value,
                            "significant_005": p_value < 0.05,
                        }
                    except ImportError:
                        import statistics

                        pair_results[metric] = {
                            "mean_a": statistics.mean(vals_a),
                            "mean_b": statistics.mean(vals_b),
                            "note": "scipy not available, no significance test",
                        }

                comparisons[pair_key] = pair_results

        return [Aggregate(name="comparative", data=comparisons)]

"""
Descriptive aggregator: mean, p50/p95/p99, stdev for each metric per arm.
"""

from __future__ import annotations

import statistics
from typing import Any

from harness.aggregators.protocol import Aggregate, Aggregator


class DescriptiveAggregator(Aggregator):
    @property
    def name(self) -> str:
        return "descriptive"

    def aggregate(
        self,
        scores: dict[str, list[dict[str, Any]]],
    ) -> list[Aggregate]:
        aggregates: list[Aggregate] = []

        for arm, task_scores in scores.items():
            metric_values: dict[str, list[float]] = {}

            for task in task_scores:
                for evaluator_name, metrics in task.get("scores", {}).items():
                    if not isinstance(metrics, list):
                        continue
                    for m in metrics:
                        key = f"{evaluator_name}.{m['name']}"
                        metric_values.setdefault(key, []).append(m["value"])

            arm_stats: dict[str, Any] = {}
            for metric_name, values in sorted(metric_values.items()):
                if not values:
                    continue
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                arm_stats[metric_name] = {
                    "count": n,
                    "mean": statistics.mean(values),
                    "stdev": statistics.stdev(values) if n > 1 else 0.0,
                    "p50": sorted_vals[n // 2],
                    "p95": sorted_vals[int(n * 0.95)] if n >= 20 else sorted_vals[-1],
                    "p99": sorted_vals[int(n * 0.99)] if n >= 100 else sorted_vals[-1],
                    "min": sorted_vals[0],
                    "max": sorted_vals[-1],
                }

            aggregates.append(Aggregate(name=f"descriptive_{arm}", data=arm_stats))

        return aggregates

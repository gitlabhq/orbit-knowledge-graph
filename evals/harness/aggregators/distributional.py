"""
Distributional aggregator: tool call distributions by category/arm.

Groups tasks by category and analyzes how tool usage patterns differ
across arms and task types.
"""

from __future__ import annotations

from typing import Any

from harness.aggregators.protocol import Aggregate, Aggregator


class DistributionalAggregator(Aggregator):
    @property
    def name(self) -> str:
        return "distributional"

    def aggregate(
        self,
        scores: dict[str, list[dict[str, Any]]],
    ) -> list[Aggregate]:
        distributions: dict[str, Any] = {}

        for arm, task_scores in scores.items():
            tool_counts: dict[str, list[float]] = {}
            sequence_lengths: list[float] = []

            for task in task_scores:
                for evaluator_name, metrics in task.get("scores", {}).items():
                    if not isinstance(metrics, list):
                        continue
                    for m in metrics:
                        if m["name"].startswith("tool_"):
                            tool_name = m["name"][5:]
                            tool_counts.setdefault(tool_name, []).append(m["value"])
                        elif m["name"] == "tool_sequence_length":
                            sequence_lengths.append(m["value"])

            import statistics

            arm_dist: dict[str, Any] = {}
            for tool_name, counts in sorted(tool_counts.items()):
                arm_dist[f"tool_{tool_name}"] = {
                    "total": sum(counts),
                    "mean_per_task": statistics.mean(counts) if counts else 0,
                    "max_per_task": max(counts) if counts else 0,
                    "tasks_using": sum(1 for c in counts if c > 0),
                }

            if sequence_lengths:
                arm_dist["sequence_length"] = {
                    "mean": statistics.mean(sequence_lengths),
                    "stdev": statistics.stdev(sequence_lengths) if len(sequence_lengths) > 1 else 0,
                    "min": min(sequence_lengths),
                    "max": max(sequence_lengths),
                }

            distributions[arm] = arm_dist

        return [Aggregate(name="distributional", data=distributions)]

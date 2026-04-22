"""
Graph evaluator: row-set comparison against fixture.

Compares the agent's structured output against the expected result from
fixtures/<task_id>/expected.json. Produces correctness and completeness metrics.
"""

from __future__ import annotations

from typing import Any

from harness.evaluators.protocol import Evaluator, Metric
from harness.store import TaskResult


class GraphEvaluator(Evaluator):
    @property
    def name(self) -> str:
        return "graph"

    def evaluate(
        self,
        result: TaskResult,
        snapshot: dict[str, Any] | None,
        fixture: dict[str, Any] | None,
    ) -> list[Metric]:
        if fixture is None or result.structured_output is None:
            return [
                Metric(name="correctness", value=0.0, metadata={"reason": "missing data"}),
                Metric(name="completeness", value=0.0, metadata={"reason": "missing data"}),
            ]

        expected = fixture.get("rows", fixture) if isinstance(fixture, dict) else fixture
        actual = result.structured_output.get("rows", result.structured_output)

        if not isinstance(expected, list) or not isinstance(actual, list):
            return [
                Metric(name="correctness", value=0.0, metadata={"reason": "non-list result"}),
                Metric(name="completeness", value=0.0, metadata={"reason": "non-list result"}),
            ]

        expected_set = _normalize_rows(expected)
        actual_set = _normalize_rows(actual)

        if not expected_set:
            correctness = 1.0 if not actual_set else 0.0
            return [
                Metric(name="correctness", value=correctness),
                Metric(name="completeness", value=1.0 if not actual_set else 0.0),
            ]

        true_positives = expected_set & actual_set
        false_positives = actual_set - expected_set
        false_negatives = expected_set - actual_set

        precision = len(true_positives) / len(actual_set) if actual_set else 0.0
        recall = len(true_positives) / len(expected_set)

        return [
            Metric(
                name="correctness",
                value=precision,
                metadata={
                    "true_positives": len(true_positives),
                    "false_positives": len(false_positives),
                },
            ),
            Metric(
                name="completeness",
                value=recall,
                metadata={
                    "true_positives": len(true_positives),
                    "false_negatives": len(false_negatives),
                },
            ),
        ]


def _normalize_rows(rows: list[Any]) -> frozenset[str]:
    """Normalize rows to a set of canonical JSON strings for comparison."""
    normalized = set()
    for row in rows:
        if isinstance(row, dict):
            canonical = tuple(sorted((k, str(v)) for k, v in row.items()))
            normalized.add(str(canonical))
        else:
            normalized.add(str(row))
    return frozenset(normalized)

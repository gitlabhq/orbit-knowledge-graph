"""
Efficiency evaluator: tool calls, tokens, cost, timing.

Operates on the session summary and snapshot to measure how efficiently
the agent solved the task.
"""

from __future__ import annotations

from typing import Any

from harness.evaluators.protocol import Evaluator, Metric
from harness.store import TaskResult


class EfficiencyEvaluator(Evaluator):
    @property
    def name(self) -> str:
        return "efficiency"

    def evaluate(
        self,
        result: TaskResult,
        snapshot: dict[str, Any] | None,
        fixture: dict[str, Any] | None,
    ) -> list[Metric]:
        metrics: list[Metric] = []
        s = result.session_summary

        if s is None:
            return [Metric(name="efficiency_score", value=0.0, metadata={"reason": "no summary"})]

        metrics.append(Metric(name="steps", value=float(s.steps)))
        metrics.append(Metric(name="tool_calls", value=float(s.tool_calls)))
        metrics.append(Metric(name="cost_usd", value=s.cost))
        metrics.append(Metric(name="duration_ms", value=float(s.duration_ms)))
        metrics.append(Metric(
            name="tokens_total",
            value=float(sum(s.tokens.values())),
            metadata=s.tokens,
        ))

        # Count specific tool types from snapshot
        if snapshot:
            tool_breakdown = _count_tool_types(snapshot)
            for tool_name, count in tool_breakdown.items():
                metrics.append(Metric(
                    name=f"tool_{tool_name}",
                    value=float(count),
                ))

        return metrics


def _count_tool_types(snapshot: dict[str, Any]) -> dict[str, int]:
    """Count tool invocations by tool name from a snapshot."""
    counts: dict[str, int] = {}
    for msg in snapshot.get("messages", []):
        for part in msg.get("parts", []):
            if part.get("type") == "tool-invocation":
                tool = part.get("tool", "unknown")
                counts[tool] = counts.get(tool, 0) + 1
    return counts

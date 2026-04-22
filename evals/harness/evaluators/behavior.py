"""
Behavior evaluator: skill loads, permission events, doom loops, tool call sequences.

Analyzes the agent's behavioral patterns from the full session trace.
"""

from __future__ import annotations

from typing import Any

from harness.evaluators.protocol import Evaluator, Metric
from harness.store import TaskResult


class BehaviorEvaluator(Evaluator):
    @property
    def name(self) -> str:
        return "behavior"

    def evaluate(
        self,
        result: TaskResult,
        snapshot: dict[str, Any] | None,
        fixture: dict[str, Any] | None,
    ) -> list[Metric]:
        if snapshot is None:
            return [Metric(name="behavior_score", value=0.0, metadata={"reason": "no snapshot"})]

        metrics: list[Metric] = []
        messages = snapshot.get("messages", [])
        events = snapshot.get("events", [])

        # Skill loading
        skill_loaded = _check_skill_loaded(messages)
        metrics.append(Metric(name="skill_loaded", value=1.0 if skill_loaded else 0.0))

        if skill_loaded:
            skill_position = _skill_load_position(messages)
            metrics.append(Metric(name="skill_load_position", value=float(skill_position)))

        # Query refinement
        query_count = _count_query_calls(messages)
        metrics.append(Metric(name="query_count", value=float(query_count)))

        # Doom loop detection (same tool call 3+ times)
        doom_loop = _detect_doom_loop(messages)
        metrics.append(Metric(name="doom_loop_detected", value=1.0 if doom_loop else 0.0))

        # Permission events
        perm_denied = _count_permission_denied(events)
        metrics.append(Metric(name="permission_denied_count", value=float(perm_denied)))

        # Tool call sequence
        sequence = _extract_tool_sequence(messages)
        metrics.append(Metric(
            name="tool_sequence_length",
            value=float(len(sequence)),
            metadata={"sequence": sequence[:20]},
        ))

        return metrics


def _check_skill_loaded(messages: list[dict[str, Any]]) -> bool:
    for msg in messages:
        for part in msg.get("parts", []):
            if part.get("type") in ("tool-invocation", "tool") and part.get("tool") == "skill":
                return True
    return False


def _skill_load_position(messages: list[dict[str, Any]]) -> int:
    """Return the 0-indexed assistant message position where skill was first loaded."""
    pos = 0
    for msg in messages:
        info = msg.get("info", {})
        if info.get("role") != "assistant":
            continue
        for part in msg.get("parts", []):
            if part.get("type") in ("tool-invocation", "tool") and part.get("tool") == "skill":
                return pos
        pos += 1
    return -1


def _count_query_calls(messages: list[dict[str, Any]]) -> int:
    count = 0
    for msg in messages:
        for part in msg.get("parts", []):
            if part.get("type") in ("tool-invocation", "tool") and part.get("tool") == "bash":
                cmd = ""
                inp = part.get("input", {})
                if isinstance(inp, dict):
                    cmd = inp.get("command", "")
                # also check state.input for the OpenCode part format
                state = part.get("state", {})
                if isinstance(state, dict) and isinstance(state.get("input"), dict):
                    cmd = cmd or state["input"].get("command", "")
                if "orbit_query" in cmd or "glab" in cmd:
                    count += 1
    return count


def _detect_doom_loop(messages: list[dict[str, Any]], threshold: int = 3) -> bool:
    """Detect if the same tool call was repeated threshold+ times in sequence."""
    recent_calls: list[str] = []
    for msg in messages:
        for part in msg.get("parts", []):
            if part.get("type") not in ("tool-invocation", "tool"):
                continue
            inp = part.get("input", {})
            call_sig = f"{part.get('tool')}:{str(inp)}"
            recent_calls.append(call_sig)

    if len(recent_calls) < threshold:
        return False

    for i in range(len(recent_calls) - threshold + 1):
        window = recent_calls[i : i + threshold]
        if len(set(window)) == 1:
            return True
    return False


def _count_permission_denied(events: list[dict[str, Any]]) -> int:
    count = 0
    for evt in events:
        if evt.get("type") == "permission.replied":
            data = evt.get("data", {})
            if not data.get("allowed", True):
                count += 1
    return count


def _extract_tool_sequence(messages: list[dict[str, Any]]) -> list[str]:
    sequence = []
    for msg in messages:
        for part in msg.get("parts", []):
            if part.get("type") in ("tool-invocation", "tool"):
                sequence.append(part.get("tool", "unknown"))
    return sequence

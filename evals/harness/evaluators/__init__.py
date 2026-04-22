"""Evaluator registry and loader."""

from __future__ import annotations

from harness.evaluators.protocol import Evaluator

_REGISTRY: dict[str, type[Evaluator]] = {}


def _ensure_registered() -> None:
    if _REGISTRY:
        return
    from harness.evaluators.behavior import BehaviorEvaluator
    from harness.evaluators.efficiency import EfficiencyEvaluator
    from harness.evaluators.graph import GraphEvaluator

    _REGISTRY["graph"] = GraphEvaluator
    _REGISTRY["efficiency"] = EfficiencyEvaluator
    _REGISTRY["behavior"] = BehaviorEvaluator


def load_evaluators(names: list[str]) -> list[Evaluator]:
    _ensure_registered()
    evaluators = []
    for name in names:
        cls = _REGISTRY.get(name)
        if cls is None:
            raise ValueError(f"unknown evaluator: {name!r} (available: {list(_REGISTRY)})")
        evaluators.append(cls())
    return evaluators

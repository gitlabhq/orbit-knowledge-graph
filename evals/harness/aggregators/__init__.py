"""Aggregator registry and loader."""

from __future__ import annotations

from harness.aggregators.protocol import Aggregator

_REGISTRY: dict[str, type[Aggregator]] = {}


def _ensure_registered() -> None:
    if _REGISTRY:
        return
    from harness.aggregators.comparative import ComparativeAggregator
    from harness.aggregators.descriptive import DescriptiveAggregator
    from harness.aggregators.distributional import DistributionalAggregator

    _REGISTRY["descriptive"] = DescriptiveAggregator
    _REGISTRY["comparative"] = ComparativeAggregator
    _REGISTRY["distributional"] = DistributionalAggregator


def load_aggregators(names: list[str]) -> list[Aggregator]:
    _ensure_registered()
    aggregators = []
    for name in names:
        cls = _REGISTRY.get(name)
        if cls is None:
            raise ValueError(f"unknown aggregator: {name!r} (available: {list(_REGISTRY)})")
        aggregators.append(cls())
    return aggregators

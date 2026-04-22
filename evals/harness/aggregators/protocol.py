"""
Aggregator protocol: cross-arm, cross-task analysis.

An aggregator takes all scored results from a run and produces
aggregate statistics, comparisons, or distributions.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass
class Aggregate:
    name: str
    data: dict[str, Any]


class Aggregator(ABC):
    """Base class for run-level aggregators."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def aggregate(
        self,
        scores: dict[str, list[dict[str, Any]]],
    ) -> list[Aggregate]: ...

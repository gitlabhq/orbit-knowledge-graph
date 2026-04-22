"""
Evaluator protocol: per-task scoring.

An evaluator takes a TaskResult, the full SessionSnapshot data, and an optional
fixture (expected result), and returns a list of named metrics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from harness.store import TaskResult


@dataclass
class Metric:
    name: str
    value: float
    metadata: dict[str, Any] | None = None


class Evaluator(ABC):
    """Base class for task-level evaluators."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def evaluate(
        self,
        result: TaskResult,
        snapshot: dict[str, Any] | None,
        fixture: dict[str, Any] | None,
    ) -> list[Metric]: ...

"""
SSOT config loader for the GKG eval harness.

Parses eval.yaml once at startup into frozen pydantic models.
All env var references (${VAR}) are resolved during load.
"""

from __future__ import annotations

import os
import re
from enum import Enum
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class Difficulty(str, Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"
    VERY_HARD = "very-hard"


class TimeoutConfig(BaseModel, frozen=True):
    task: int = 300
    server_start: int = 30


class TaskFilter(BaseModel, frozen=True):
    ids: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)
    min_difficulty: Difficulty = Difficulty.EASY


class TasksConfig(BaseModel, frozen=True):
    paths: list[str]
    filter: TaskFilter = Field(default_factory=TaskFilter)


class ScoringConfig(BaseModel, frozen=True):
    fixtures_path: str = "fixtures"


class RunConfig(BaseModel, frozen=True):
    name: str
    version: str = "0.1.0"
    concurrency: int = 4
    tasks: TasksConfig
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)


class ModelConfig(BaseModel, frozen=True):
    provider: str
    model: str


class ArmConfig(BaseModel, frozen=True):
    name: str
    agent: str
    skills: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    model: ModelConfig
    port: int

    @field_validator("name")
    @classmethod
    def name_is_slug(cls, v: str) -> str:
        if not re.match(r"^[a-z0-9][a-z0-9_-]*$", v):
            raise ValueError(f"arm name must be a slug (got {v!r})")
        return v


class EvalConfig(BaseModel, frozen=True):
    run: RunConfig
    arms: list[ArmConfig]
    evaluators: list[str] = Field(default_factory=list)
    aggregators: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def unique_arm_names(self) -> EvalConfig:
        names = [a.name for a in self.arms]
        dupes = [n for n in names if names.count(n) > 1]
        if dupes:
            raise ValueError(f"duplicate arm names: {set(dupes)}")
        return self

    @model_validator(mode="after")
    def unique_ports(self) -> EvalConfig:
        ports = [a.port for a in self.arms]
        dupes = [p for p in ports if ports.count(p) > 1]
        if dupes:
            raise ValueError(f"duplicate ports: {set(dupes)}")
        return self


_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _resolve_env_vars(obj: object) -> object:
    """Recursively resolve ${VAR} references in strings."""
    if isinstance(obj, str):
        def _replace(m: re.Match[str]) -> str:
            var = m.group(1)
            val = os.environ.get(var)
            if val is None:
                raise ValueError(f"env var ${{{var}}} is not set")
            return val
        return _ENV_VAR_RE.sub(_replace, obj)
    if isinstance(obj, dict):
        return {k: _resolve_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_env_vars(v) for v in obj]
    return obj


def load_config(path: str | Path) -> EvalConfig:
    """Load, resolve env vars, and validate eval config from YAML."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"config not found: {path}")

    with path.open() as f:
        raw = yaml.safe_load(f)

    resolved = _resolve_env_vars(raw)
    return EvalConfig.model_validate(resolved)

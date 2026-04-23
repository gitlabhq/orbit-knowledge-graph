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


class TaskStatus(str, Enum):
    SUCCESS = "success"
    TIMEOUT = "timeout"
    AGENT_ERROR = "agent_error"
    INFRA_ERROR = "infra_error"


class Difficulty(str, Enum):
    EASY = "easy"
    MEDIUM = "medium"
    HARD = "hard"


class TaskCategory(str, Enum):
    TRAVERSAL = "traversal"
    SEARCH = "search"
    AGGREGATION = "aggregation"
    MUTATION = "mutation"
    MULTI_HOP = "multi-hop"


class RetryConfig(BaseModel, frozen=True):
    max_attempts: int = 3
    base_delay_ms: int = 1000
    max_delay_ms: int = 30000
    backoff_factor: int = 2
    jitter: bool = True


class TimeoutConfig(BaseModel, frozen=True):
    task: int = 300
    server_start: int = 30
    session_create: int = 10
    data_extract: int = 10


class TaskFilter(BaseModel, frozen=True):
    categories: list[str] = Field(default_factory=list)
    min_difficulty: Difficulty = Difficulty.EASY


class TasksConfig(BaseModel, frozen=True):
    paths: list[str]
    filter: TaskFilter = Field(default_factory=TaskFilter)


class ScoringConfig(BaseModel, frozen=True):
    fixtures_path: str = "fixtures"
    report_formats: list[str] = Field(default_factory=lambda: ["markdown", "json"])


class RunConfig(BaseModel, frozen=True):
    name: str
    version: str = "0.1.0"
    concurrency: int = 4
    task_timeout: int = 300
    output_dir: str = "results"
    tasks: TasksConfig
    scoring: ScoringConfig = Field(default_factory=ScoringConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    timeouts: TimeoutConfig = Field(default_factory=TimeoutConfig)


class ModelConfig(BaseModel, frozen=True):
    provider: str
    model: str
    temperature: float = 0.0
    max_tokens: int = 16384


class ToolsConfig(BaseModel, frozen=True):
    allow: list[str] = Field(default_factory=list)
    bash_scope: list[str] = Field(default_factory=list)


class ArmConfig(BaseModel, frozen=True):
    name: str
    agent: str
    skills: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    model: ModelConfig
    tools: ToolsConfig = Field(default_factory=ToolsConfig)
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

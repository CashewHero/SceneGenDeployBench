from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from app.config import RunnerDefinition


@dataclass(frozen=True)
class LauncherPreflightResult:
    available: bool
    code: str | None = None
    message: str | None = None


class RunnerLauncher(Protocol):
    def validate(self) -> None: ...

    def preflight(self) -> LauncherPreflightResult: ...

    def start_runner(self) -> None: ...

    def get_endpoint(self) -> str: ...

    def stop_runner(self) -> None: ...

@dataclass(frozen=True)
class RunnerLaunchContext:
    runner: RunnerDefinition
    endpoint_override: str | None = None
    dataset_root: str = "/data/datasets"
    model_cache_root: str = "/data/model_cache"
    output_root: str = "/data/output"
    pipeline_root: str = "/data/pipelines"
    runner_env: dict[str, str] | None = None

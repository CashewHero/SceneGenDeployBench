from __future__ import annotations

from .base import RunnerLaunchContext, RunnerLauncher
from .docker import DockerRunnerLauncher
from .static_http import StaticHttpRunnerLauncher


def create_runner_launcher(context: RunnerLaunchContext) -> RunnerLauncher:
    if context.endpoint_override:
        return StaticHttpRunnerLauncher(context)

    driver = str(context.runner.launcher.get("driver", "")).strip()
    if driver == "static_http":
        return StaticHttpRunnerLauncher(context)
    if driver == "docker":
        return DockerRunnerLauncher(context)
    raise ValueError(
        f"runner {context.runner.selector} uses unsupported launcher driver {driver!r}"
    )

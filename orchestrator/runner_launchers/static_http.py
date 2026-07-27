from __future__ import annotations

from dataclasses import dataclass

from .base import LauncherPreflightResult, RunnerLaunchContext


@dataclass(frozen=True)
class StaticHttpRunnerLauncher:
    context: RunnerLaunchContext

    def validate(self) -> None:
        compat_version = int(self.context.runner.launcher.get("compat_version", 0))
        if compat_version != 1:
            raise ValueError(
                f"runner {self.context.runner.selector} uses unsupported static_http compat_version {compat_version}"
            )

    def start_runner(self) -> None:
        self.validate()
        if not self._base_url():
            raise RuntimeError(
                "static_http launcher requires an endpoint override or launcher.endpoint.base_url in the selected runner config"
            )

    def preflight(self) -> LauncherPreflightResult:
        self.validate()
        if not self._base_url():
            return LauncherPreflightResult(
                available=False,
                code="ENDPOINT_MISSING",
                message=(
                    "static_http launcher requires an endpoint override or "
                    "launcher.endpoint.base_url"
                ),
            )
        return LauncherPreflightResult(available=True)

    def get_endpoint(self) -> str:
        self.start_runner()
        return self._base_url().rstrip("/")

    def stop_runner(self) -> None:
        return

    def _base_url(self) -> str | None:
        if self.context.endpoint_override:
            return self.context.endpoint_override
        endpoint = self.context.runner.launcher.get("endpoint") or {}
        if not isinstance(endpoint, dict):
            raise ValueError(
                f"runner {self.context.runner.selector} launcher.endpoint must be a mapping"
            )
        base_url = endpoint.get("base_url")
        return str(base_url).strip() if base_url else None

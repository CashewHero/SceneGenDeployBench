from __future__ import annotations

import json
import time
from typing import Any
from typing import Callable
from urllib import error, request

from app.config import PollingConfig

_MIN_POLL_INTERVAL_SECONDS = 0.25
_MAX_READY_POLL_INTERVAL_SECONDS = 2.0


def _http_json(method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = request.Request(url, data=data, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {url} failed with HTTP {exc.code}: {body}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"{method} {url} failed: {exc.reason}") from exc


def get_status(base_url: str) -> dict[str, Any]:
    return _http_json("GET", f"{base_url.rstrip('/')}/status")


def run_job(base_url: str, request_payload: dict[str, Any]) -> dict[str, Any]:
    return _http_json("POST", f"{base_url.rstrip('/')}/run-job", request_payload)


def shutdown_runner(base_url: str) -> dict[str, Any]:
    return _http_json("POST", f"{base_url.rstrip('/')}/shutdown", {})


def _sleep_for_retry(*, deadline: float, poll_seconds: float, max_interval_seconds: float | None = None) -> bool:
    remaining = deadline - time.time()
    if remaining <= 0:
        return False

    interval = max(float(poll_seconds), _MIN_POLL_INTERVAL_SECONDS)
    if max_interval_seconds is not None:
        interval = min(interval, max_interval_seconds)

    time.sleep(min(interval, remaining))
    return True


def wait_until_ready(base_url: str, polling: PollingConfig, timeout_seconds: float = 60.0) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    last_error: Exception | None = None
    while True:
        try:
            status = get_status(base_url)
        except Exception as exc:
            last_error = exc
        else:
            if status.get("state") in {"idle", "finished", "failed"}:
                return status
        if not _sleep_for_retry(
            deadline=deadline,
            poll_seconds=polling.startup_seconds,
            max_interval_seconds=_MAX_READY_POLL_INTERVAL_SECONDS,
        ):
            break
    if last_error is not None:
        raise TimeoutError(
            f"runner did not become ready within {timeout_seconds} seconds; last error: {last_error}"
        ) from last_error
    raise TimeoutError(f"runner did not become ready within {timeout_seconds} seconds")


def wait_for_terminal_state(
    base_url: str,
    job_id: str,
    polling: PollingConfig,
    timeout_seconds: float = 3600.0,
    on_poll: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    deadline = time.time() + timeout_seconds
    while True:
        status = get_status(base_url)
        state = status.get("state")
        current_job_id = status.get("current_job_id")
        if current_job_id == job_id and state in {"finished", "failed"}:
            return status
        if on_poll is not None:
            override = on_poll(status)
            if override is not None:
                return override
        sleep_seconds = polling.running_seconds if state == "running" else polling.post_submit_seconds
        if not _sleep_for_retry(deadline=deadline, poll_seconds=sleep_seconds):
            break
    raise TimeoutError(f"job {job_id} did not reach a terminal state within {timeout_seconds} seconds")

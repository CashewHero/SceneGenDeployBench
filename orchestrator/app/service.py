from __future__ import annotations

import json
import logging
import threading
import time
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from domain.batches import BatchPlan, JobRecord
from cli.commands import (
    event_message,
    load_runtime_config,
    resolve_runner,
    show_config_payload,
)
from storage.db import claim_pending_batch, ensure_schema, sync_reference_state, update_batch_runner_endpoint
from execution.dispatch import dispatch_batch
from execution.pipelines import reconcile_pipelines
from execution.script_run import remove_script_containers
from domain.scheduling import WindowState, evaluate_window_state

logger = logging.getLogger("scenegendeploybench.orchestrator.service")
STARTUP_DB_MAX_ATTEMPTS = 60
STARTUP_DB_RETRY_SECONDS = 30


def utc_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def service_status_payload(service: "OrchestratorService", accepted: bool | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "online": True,
        "service_name": "scenegendeploybench-orchestrator",
        "state": service.state,
        "current_operation": service.current_operation,
        "window_active": service.window_active,
        "updated_at": service.updated_at,
        "last_result": service.last_result,
        "last_error": service.last_error,
    }
    if accepted is not None:
        payload["accepted"] = accepted
    return payload


@dataclass
class OrchestratorService:
    config_path: str | None
    state: str = "starting"
    current_operation: str | None = None
    window_active: bool = True
    last_result: dict[str, object] | None = None
    last_error: dict[str, object] | None = None
    updated_at: str = field(default_factory=utc_now)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def mark_ready(self) -> None:
        with self.lock:
            self.state = "idle"
            self.current_operation = None
            self.updated_at = utc_now()
            logger.info(event_message("service_ready", config_path=self.config_path))

    def start_operation(self, operation: str) -> bool:
        with self.lock:
            if self.state in {"running", "shutting_down"}:
                return False
            self.state = "running"
            self.current_operation = operation
            self.updated_at = utc_now()
            self.last_error = None
            return True

    def set_operation(self, operation: str) -> None:
        with self.lock:
            if self.state == "running":
                self.current_operation = operation
                self.updated_at = utc_now()

    def finish_operation(
        self,
        *,
        result: dict[str, object] | None = None,
        error: dict[str, object] | None = None,
    ) -> None:
        with self.lock:
            self.state = "idle"
            self.current_operation = None
            self.last_result = result
            self.last_error = error
            self.updated_at = utc_now()

    def request_shutdown(self) -> bool:
        with self.lock:
            if self.state == "running":
                return False
            self.state = "shutting_down"
            self.current_operation = "shutdown"
            self.updated_at = utc_now()
            return True

    def set_window_state(self, active: bool) -> None:
        with self.lock:
            self.window_active = active
            self.updated_at = utc_now()


class OrchestratorHandler(BaseHTTPRequestHandler):
    server_version = "DeployBenchOrchestrator/1.0"

    def _send_json(self, status_code: int, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload, indent=2).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:
        if self.path == "/status":
            with self.server.orchestrator.lock:
                payload = service_status_payload(self.server.orchestrator)
            self._send_json(HTTPStatus.OK, payload)
            return

        if self.path == "/config":
            try:
                payload = show_config_payload(self.server.orchestrator.config_path)
            except (FileNotFoundError, NotADirectoryError, RuntimeError, ValueError) as exc:
                self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                return
            self._send_json(HTTPStatus.OK, payload)
            return

        logger.warning(event_message("http_not_found", method="GET", path=self.path))
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        if self.path == "/shutdown":
            self._handle_shutdown()
            return

        logger.warning(event_message("http_not_found", method="POST", path=self.path))
        self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _handle_shutdown(self) -> None:
        accepted = self.server.orchestrator.request_shutdown()
        with self.server.orchestrator.lock:
            payload = service_status_payload(self.server.orchestrator, accepted=accepted)
        self._send_json(HTTPStatus.OK, payload)
        if accepted:
            threading.Thread(target=self.server.shutdown, daemon=True).start()

    def log_message(self, format: str, *args: object) -> None:
        return


class OrchestratorHTTPServer(ThreadingHTTPServer):
    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[OrchestratorHandler],
        orchestrator: OrchestratorService,
    ) -> None:
        super().__init__(server_address, handler_class)
        self.orchestrator = orchestrator


def _effective_runner_window_state(config, runner) -> WindowState:
    return evaluate_window_state(runner.scheduling or config.orchestrator.scheduling or {})


def _any_runner_window_active(config) -> bool:
    if not config.runners:
        return True
    return any(_effective_runner_window_state(config, runner).active for runner in config.runners.values())


def _materialize_claimed_batch(claim: dict[str, Any]) -> BatchPlan:
    jobs: list[JobRecord] = []
    for job in claim["jobs"]:
        jobs.append(
            JobRecord(
                job_id=job["job_id"],
                request_payload=job["request_payload"],
                output_dir=Path(job["output_dir"]),
            )
        )

    return BatchPlan(
        batch_id=claim["batch_id"],
        jobs=jobs,
    )


def scheduler_loop(server: "OrchestratorHTTPServer") -> None:
    busy_sleep_seconds = 1
    while True:
        sleep_seconds: int | None = None
        with server.orchestrator.lock:
            if server.orchestrator.state == "shutting_down":
                return
            if server.orchestrator.state == "running":
                sleep_seconds = busy_sleep_seconds
        if sleep_seconds is not None:
            time.sleep(sleep_seconds)
            continue

        try:
            config = load_runtime_config(server.orchestrator.config_path)
            server.orchestrator.set_window_state(_any_runner_window_active(config))
            if not server.orchestrator.start_operation("scheduler-reconcile"):
                time.sleep(busy_sleep_seconds)
                continue
            pipeline_result = reconcile_pipelines(config)
            if pipeline_result["pipeline_count"]:
                logger.info(event_message("pipelines_polled", **pipeline_result))
            server.orchestrator.set_operation("scheduler-claim")
            claim = claim_pending_batch(config)
            if claim is None:
                server.orchestrator.finish_operation(
                    result={
                        "event": "scheduler_idle",
                        "pipelines": pipeline_result,
                    }
                )
                time.sleep(config.orchestrator.polling.running_seconds)
                continue

            batch_id = str(claim["batch_id"])
            server.orchestrator.set_operation(f"scheduler-dispatch:{batch_id}")

            runner = resolve_runner(config, str(claim["runner_selector"]))
            window_state = _effective_runner_window_state(config, runner)
            server.orchestrator.set_window_state(window_state.active)
            plan = _materialize_claimed_batch(claim)
            logger.info(
                event_message(
                    "scheduler_batch_start",
                    batch_id=batch_id,
                    job_count=len(plan.jobs),
                    window_active=window_state.active,
                    start_policy=window_state.start_policy,
                    end_policy=window_state.end_policy,
                )
            )

            def current_window_state() -> WindowState:
                state = _effective_runner_window_state(config, runner)
                server.orchestrator.set_window_state(state.active)
                return state

            exit_code = dispatch_batch(
                config,
                plan,
                runner=runner,
                runner_url=str(claim.get("runner_endpoint") or "") or None,
                keep_runner=False,
                on_endpoint_ready=lambda endpoint: update_batch_runner_endpoint(
                    config,
                    batch_id=plan.batch_id,
                    runner_endpoint=endpoint,
                ),
                initial_window_state=window_state,
                window_state_provider=current_window_state,
            )
            server.orchestrator.finish_operation(
                result={
                    "event": "scheduler_batch_finished",
                    "batch_id": batch_id,
                    "job_count": len(plan.jobs),
                    "exit_code": exit_code,
                    "window_active": window_state.active,
                }
            )
        except Exception as exc:
            logger.exception(event_message("scheduler_loop_failed", error=str(exc)))
            server.orchestrator.finish_operation(
                error={"message": str(exc), "type": exc.__class__.__name__}
            )
            time.sleep(busy_sleep_seconds)


def run_service(host: str, port: int, config_path: str | None) -> None:
    logger.info(
        event_message(
            "service_starting",
            host=host,
            port=port,
            config_path=config_path,
        )
    )
    config = load_runtime_config(config_path)
    for attempt in range(1, STARTUP_DB_MAX_ATTEMPTS + 1):
        try:
            ensure_schema(config)
            sync_reference_state(config)
            break
        except Exception as exc:
            logger.exception(
                event_message(
                    "service_startup_db_check_failed",
                    attempt=attempt,
                    max_attempts=STARTUP_DB_MAX_ATTEMPTS,
                    error=str(exc),
                )
            )
            if attempt == STARTUP_DB_MAX_ATTEMPTS:
                raise
            time.sleep(STARTUP_DB_RETRY_SECONDS)
    try:
        removed = remove_script_containers()
        if removed:
            logger.info(
                event_message(
                    "orphan_script_containers_removed",
                    container_count=removed,
                )
            )
    except Exception as exc:
        logger.warning(
            event_message(
                "orphan_script_container_cleanup_failed",
                error=str(exc),
            )
        )
    orchestrator = OrchestratorService(config_path=config_path)
    server = OrchestratorHTTPServer((host, port), OrchestratorHandler, orchestrator)
    orchestrator.mark_ready()
    scheduler_thread = threading.Thread(target=scheduler_loop, args=(server,), daemon=True)
    scheduler_thread.start()
    try:
        server.serve_forever()
    finally:
        logger.info(event_message("service_closed", host=host, port=port))
        server.server_close()

from __future__ import annotations

import json
import logging
from typing import Any
from typing import Callable

from domain.batches import BatchPlan
from app.config import OrchestratorConfig, RunnerDefinition
from storage.db import (
    fetch_job_status,
    release_batch_pending_jobs,
    sync_dataset_state,
    write_job_dispatch_failure,
    write_job_terminal_result,
)
from execution.runner_client import get_status, run_job, shutdown_runner, wait_for_terminal_state, wait_until_ready
from runner_launchers import create_runner_launcher
from runner_launchers.base import RunnerLaunchContext
from domain.scheduling import END_POLICY_END_NOW, END_POLICY_FINISH_JOB, WindowState

logger = logging.getLogger("scenegendeploybench.orchestrator.dispatch")
DEFAULT_RUNNER_STARTUP_TIMEOUT_MINUTES = 1.0


def _event_message(event: str, **fields: object) -> str:
    return json.dumps({"event": event, **fields}, sort_keys=True)


def _record_terminal_job(
    config: OrchestratorConfig,
    *,
    batch_id: str,
    job,
    terminal: dict[str, Any],
) -> dict[str, Any]:
    result = terminal.get("result") or {}
    terminal_write = write_job_terminal_result(
        config=config,
        job_id=job.job_id,
        batch_id=batch_id,
        output_dir=str(job.output_dir),
        terminal_state=str(terminal.get("state") or ""),
        updated_at=terminal.get("updated_at"),
        result_payload=result if isinstance(result, dict) else {},
        artifacts_payload=list(result.get("artifacts", [])) if isinstance(result.get("artifacts"), list) else [],
    )
    job_payload = dict(job.request_payload.get("job") or {})
    if (
        terminal_write.get("status") == "completed"
        and job_payload.get("job_type") == "dataset_download"
        and job_payload.get("rescan_after_download", True) is not False
    ):
        dataset_name = str(
            dict(job_payload.get("parameters") or {}).get("dataset_name") or ""
        ).strip()
        if dataset_name:
            try:
                rescan_result = sync_dataset_state(
                    config,
                    dataset_name=dataset_name,
                )
                logger.info(
                    _event_message(
                        "dataset_rescanned_after_download",
                        job_id=job.job_id,
                        dataset=dataset_name,
                        rescan=rescan_result,
                    )
                )
            except Exception as exc:
                logger.warning(
                    _event_message(
                        "dataset_rescan_after_download_failed",
                        job_id=job.job_id,
                        dataset=dataset_name,
                        error=str(exc),
                    )
                )
    return terminal_write


def _release_pending_jobs_for_batch(config: OrchestratorConfig, batch_id: str, policy: str) -> dict[str, Any]:
    payload = release_batch_pending_jobs(config, batch_id=batch_id)
    logger.info(
        _event_message(
            "batch_pending_jobs_released",
            batch_id=batch_id,
            policy=policy,
            released=payload["released"],
            jobs=payload["jobs"],
        )
    )
    return payload


def _runner_startup_timeout_seconds(runner: RunnerDefinition) -> float:
    value = runner.scheduling.get("startup_timeout_minutes", DEFAULT_RUNNER_STARTUP_TIMEOUT_MINUTES)
    timeout_minutes = float(value)
    if timeout_minutes <= 0:
        raise ValueError(
            f"runner {runner.selector} scheduling.startup_timeout_minutes must be greater than 0"
        )
    return timeout_minutes * 60


def _record_dispatch_failure_for_claimed_jobs(
    config: OrchestratorConfig,
    *,
    plan: BatchPlan,
    error_message: str,
) -> int:
    failures = 0
    for job in plan.jobs:
        dispatch_failure = write_job_dispatch_failure(
            config,
            job_id=job.job_id,
            batch_id=plan.batch_id,
            output_dir=str(job.output_dir),
            error_message=error_message,
        )
        event = (
            "job_dispatch_cancelled"
            if dispatch_failure.get("status") == "cancelled"
            else "job_dispatch_failed"
        )
        log = logger.info if event == "job_dispatch_cancelled" else logger.warning
        log(
            _event_message(
                event,
                batch_id=plan.batch_id,
                job_id=job.job_id,
                error=error_message,
                retry_scheduled=dispatch_failure.get("retry_scheduled"),
                next_attempt=dispatch_failure.get("attempt_count"),
                max_attempts=dispatch_failure.get("max_attempts"),
            )
        )
        if (
            dispatch_failure.get("status") != "cancelled"
            and not dispatch_failure.get("retry_scheduled")
        ):
            failures += 1
    _release_pending_jobs_for_batch(config, plan.batch_id, "dispatch_error")
    return failures


def _runner_batch_id(status_payload: dict[str, Any]) -> str:
    return str(status_payload.get("batch_id") or "").strip()


def _recover_terminal_if_available(
    *,
    endpoint: str,
    plan: BatchPlan,
    job,
    polling,
    on_poll: Callable[[dict[str, Any]], dict[str, Any] | None] | None = None,
) -> dict[str, Any] | None:
    status = get_status(endpoint)
    runner_batch_id = _runner_batch_id(status)
    if runner_batch_id and runner_batch_id != plan.batch_id:
        raise RuntimeError(
            f"runner at {endpoint} is bound to batch {runner_batch_id}, expected {plan.batch_id}"
        )
    current_job_id = str(status.get("current_job_id") or "")
    state = str(status.get("state") or "")
    if current_job_id == job.job_id and state == "running":
        return wait_for_terminal_state(
            endpoint,
            job_id=job.job_id,
            polling=polling,
            timeout_seconds=job.request_payload["job"]["timeout_seconds"],
            on_poll=on_poll,
        )
    if current_job_id == job.job_id and state in {"finished", "failed"}:
        return status
    if state == "running":
        wait_until_ready(endpoint, polling)
    return None


def _window_policy_requeue(policy: str) -> dict[str, Any]:
    return {
        "requeue_pending": True,
        "reason": (
            f"job interrupted because the active scheduling window closed "
            f"with end_policy={policy}"
        ),
    }


class BatchWindowPolicyController:
    def __init__(
        self,
        initial_window_state: WindowState | None,
        window_state_provider: Callable[[], WindowState] | None,
    ) -> None:
        self._window_state_provider = window_state_provider
        self._tracking = False
        self._last_active_end_policy: str | None = None
        self.stop_policy: str | None = None

        if initial_window_state is not None:
            self._tracking = initial_window_state.active
            if initial_window_state.active:
                self._last_active_end_policy = initial_window_state.end_policy

        if window_state_provider is None:
            return

    def _refresh(self) -> WindowState | None:
        if self._window_state_provider is None or not self._tracking:
            return None

        state = self._window_state_provider()
        if state.active and state.end_policy:
            self._last_active_end_policy = state.end_policy
        return state

    def stop_before_next_job(self) -> str | None:
        state = self._refresh()
        if state is None or state.active:
            return None
        if self._last_active_end_policy in {END_POLICY_END_NOW, END_POLICY_FINISH_JOB}:
            self.stop_policy = self._last_active_end_policy
            return self.stop_policy
        return None

    def should_interrupt_running_job(self) -> str | None:
        state = self._refresh()
        if state is None or state.active:
            return None
        if self._last_active_end_policy == END_POLICY_END_NOW:
            self.stop_policy = END_POLICY_END_NOW
            return self.stop_policy
        return None


def dispatch_batch(
    config: OrchestratorConfig,
    plan: BatchPlan,
    runner: RunnerDefinition,
    runner_url: str | None,
    keep_runner: bool,
    on_endpoint_ready: Callable[[str], None] | None = None,
    initial_window_state: WindowState | None = None,
    window_state_provider: Callable[[], WindowState] | None = None,
) -> int:
    launcher = create_runner_launcher(
        RunnerLaunchContext(
            runner=runner,
            endpoint_override=runner_url,
            dataset_root=str(config.storage.dataset_root),
            model_cache_root=str(config.storage.model_cache_root),
            output_root=str(config.storage.output_root),
            pipeline_root=str(config.storage.pipeline_root),
            runner_env=config.orchestrator.runner_env,
        )
    )
    endpoint: str | None = None
    failures = 0
    runner_shutdown_requested = False
    window_controller = BatchWindowPolicyController(initial_window_state, window_state_provider)
    try:
        try:
            endpoint = launcher.get_endpoint()
            if on_endpoint_ready is not None:
                on_endpoint_ready(endpoint)
            logger.info(_event_message("runner_wait_start", endpoint=endpoint, runner_selector=runner.selector))
            ready_status = wait_until_ready(
                endpoint,
                config.orchestrator.polling,
                timeout_seconds=_runner_startup_timeout_seconds(runner),
            )
            runner_batch_id = _runner_batch_id(ready_status)
            if runner_batch_id and runner_batch_id != plan.batch_id:
                raise RuntimeError(
                    f"runner at {endpoint} is bound to batch {runner_batch_id}, expected {plan.batch_id}"
                )
        except Exception as exc:
            failures += _record_dispatch_failure_for_claimed_jobs(
                config,
                plan=plan,
                error_message=str(exc),
            )
            logger.warning(
                _event_message(
                    "batch_dispatch_startup_failed",
                    batch_id=plan.batch_id,
                    error=str(exc),
                    failures=failures,
                )
            )
            return 1 if failures else 0
        logger.info(
            _event_message(
                "runner_ready",
                endpoint=endpoint,
                state=ready_status.get("state"),
                runner_name=ready_status.get("runner_name") or runner.display_name,
            )
        )
        print(
            json.dumps(
                {
                    "event": "runner_ready",
                    "state": ready_status.get("state"),
                    "runner_name": ready_status.get("runner_name") or runner.display_name,
                    "endpoint": endpoint,
                }
            )
        )

        for job in plan.jobs:
            if fetch_job_status(config, job.job_id) != "pending":
                logger.info(
                    _event_message(
                        "job_dispatch_skipped",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        reason="job is no longer pending",
                    )
                )
                continue
            stop_policy = window_controller.stop_before_next_job()
            if stop_policy is not None:
                _release_pending_jobs_for_batch(config, plan.batch_id, stop_policy)
                logger.info(
                    _event_message(
                        "batch_dispatch_stopped_at_window_boundary",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        policy=stop_policy,
                    )
                )
                break

            def _handle_job_poll(_: dict[str, Any]) -> dict[str, Any] | None:
                nonlocal runner_shutdown_requested
                if fetch_job_status(config, job.job_id) == "cancelled":
                    logger.info(
                        _event_message(
                            "job_interrupted_after_cancel",
                            batch_id=plan.batch_id,
                            job_id=job.job_id,
                        )
                    )
                    try:
                        shutdown_runner(endpoint)
                        runner_shutdown_requested = True
                    except Exception as exc:
                        logger.warning(
                            _event_message(
                                "runner_shutdown_failed_during_cancel",
                                endpoint=endpoint,
                                batch_id=plan.batch_id,
                                job_id=job.job_id,
                                error=str(exc),
                            )
                        )
                    return {"job_cancelled": True}
                interrupt_policy = window_controller.should_interrupt_running_job()
                if interrupt_policy != END_POLICY_END_NOW:
                    return None
                logger.info(
                    _event_message(
                        "job_interrupted_at_window_boundary",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        policy=interrupt_policy,
                    )
                )
                try:
                    shutdown_runner(endpoint)
                    runner_shutdown_requested = True
                except Exception as exc:
                    logger.warning(
                        _event_message(
                            "runner_shutdown_failed_during_window_interrupt",
                            endpoint=endpoint,
                            batch_id=plan.batch_id,
                            job_id=job.job_id,
                            error=str(exc),
                        )
                    )
                return _window_policy_requeue(interrupt_policy)

            recovered_terminal = _recover_terminal_if_available(
                endpoint=endpoint,
                plan=plan,
                job=job,
                polling=config.orchestrator.polling,
                on_poll=_handle_job_poll,
            )
            try:
                if recovered_terminal is not None:
                    if recovered_terminal.get("job_cancelled"):
                        _release_pending_jobs_for_batch(
                            config, plan.batch_id, "job_cancelled"
                        )
                        break
                    if recovered_terminal.get("requeue_pending"):
                        _release_pending_jobs_for_batch(config, plan.batch_id, window_controller.stop_policy or END_POLICY_END_NOW)
                        logger.info(
                            _event_message(
                                "job_requeued_at_window_boundary",
                                batch_id=plan.batch_id,
                                job_id=job.job_id,
                                policy=window_controller.stop_policy,
                                reason=recovered_terminal.get("reason"),
                            )
                        )
                        break
                    result = recovered_terminal.get("result") or {}
                    logger.info(
                        _event_message(
                            "job_terminal_recovered",
                            batch_id=plan.batch_id,
                            job_id=job.job_id,
                            state=recovered_terminal.get("state"),
                            result_status=result.get("status"),
                            artifact_count=len(result.get("artifacts", [])),
                            metric_count=len(result.get("metrics", [])),
                        )
                    )
                    terminal_write = _record_terminal_job(
                        config,
                        batch_id=plan.batch_id,
                        job=job,
                        terminal=recovered_terminal,
                    )
                    if terminal_write.get("retry_scheduled"):
                        logger.info(
                            _event_message(
                                "job_retry_scheduled",
                                batch_id=plan.batch_id,
                                job_id=job.job_id,
                                next_attempt=terminal_write.get("attempt_count"),
                                max_attempts=terminal_write.get("max_attempts"),
                                terminal_status=terminal_write.get("terminal_status"),
                            )
                        )
                    elif terminal_write.get("status") == "failed":
                        failures += 1
                    if window_controller.stop_policy == END_POLICY_END_NOW:
                        break
                    continue

                logger.info(
                    _event_message(
                        "job_dispatch_start",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        endpoint=endpoint,
                    )
                )
                accepted = run_job(endpoint, job.request_payload)
                if not accepted.get("accepted"):
                    raise RuntimeError(
                        f"runner rejected job {job.job_id} for batch {plan.batch_id}: "
                        f"state={accepted.get('state')!r} batch_id={accepted.get('batch_id')!r}"
                    )
                logger.info(
                    _event_message(
                        "job_dispatch_accepted",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        runner_state=accepted.get("state"),
                        updated_at=accepted.get("updated_at"),
                    )
                )

                terminal = wait_for_terminal_state(
                    endpoint,
                    job_id=job.job_id,
                    polling=config.orchestrator.polling,
                    timeout_seconds=job.request_payload["job"]["timeout_seconds"],
                    on_poll=_handle_job_poll,
                )
                if terminal.get("job_cancelled"):
                    _release_pending_jobs_for_batch(
                        config, plan.batch_id, "job_cancelled"
                    )
                    break
                if terminal.get("requeue_pending"):
                    _release_pending_jobs_for_batch(config, plan.batch_id, window_controller.stop_policy or END_POLICY_END_NOW)
                    logger.info(
                        _event_message(
                            "job_requeued_at_window_boundary",
                            batch_id=plan.batch_id,
                            job_id=job.job_id,
                            policy=window_controller.stop_policy,
                            reason=terminal.get("reason"),
                        )
                    )
                    break
                result = terminal.get("result") or {}
                logger.info(
                    _event_message(
                        "job_terminal_state",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        state=terminal.get("state"),
                        result_status=result.get("status"),
                        artifact_count=len(result.get("artifacts", [])),
                        metric_count=len(result.get("metrics", [])),
                    )
                )
                terminal_write = _record_terminal_job(
                    config,
                    batch_id=plan.batch_id,
                    job=job,
                    terminal=terminal,
                )
                if terminal_write.get("retry_scheduled"):
                    logger.info(
                        _event_message(
                            "job_retry_scheduled",
                            batch_id=plan.batch_id,
                            job_id=job.job_id,
                            next_attempt=terminal_write.get("attempt_count"),
                            max_attempts=terminal_write.get("max_attempts"),
                            terminal_status=terminal_write.get("terminal_status"),
                        )
                    )
                elif terminal_write.get("status") == "failed":
                    failures += 1
                if window_controller.stop_policy == END_POLICY_END_NOW:
                    break
            except Exception as exc:
                dispatch_failure = write_job_dispatch_failure(
                    config,
                    job_id=job.job_id,
                    batch_id=plan.batch_id,
                    output_dir=str(job.output_dir),
                    error_message=str(exc),
                )
                if dispatch_failure.get("status") == "cancelled":
                    runner_shutdown_requested = True
                    logger.info(
                        _event_message(
                            "job_dispatch_cancelled",
                            batch_id=plan.batch_id,
                            job_id=job.job_id,
                        )
                    )
                    _release_pending_jobs_for_batch(
                        config, plan.batch_id, "job_cancelled"
                    )
                    break
                logger.warning(
                    _event_message(
                        "job_dispatch_failed",
                        batch_id=plan.batch_id,
                        job_id=job.job_id,
                        error=str(exc),
                        retry_scheduled=dispatch_failure.get("retry_scheduled"),
                        next_attempt=dispatch_failure.get("attempt_count"),
                        max_attempts=dispatch_failure.get("max_attempts"),
                    )
                )
                _release_pending_jobs_for_batch(config, plan.batch_id, "dispatch_error")
                if not dispatch_failure.get("retry_scheduled"):
                    failures += 1
                break

        logger.info(
            _event_message(
                "batch_dispatch_finished",
                batch_id=plan.batch_id,
                job_count=len(plan.jobs),
                failures=failures,
            )
        )
        return 1 if failures else 0
    except Exception as exc:
        logger.warning(
            _event_message(
                "batch_dispatch_aborted",
                batch_id=plan.batch_id,
                error=str(exc),
            )
        )
        raise
    finally:
        if not keep_runner and endpoint is not None and not runner_shutdown_requested:
            try:
                logger.info(_event_message("runner_shutdown_start", endpoint=endpoint))
                shutdown_payload = shutdown_runner(endpoint)
                logger.info(_event_message("runner_shutdown", state=shutdown_payload.get("state")))
                print(json.dumps({"event": "runner_shutdown", "state": shutdown_payload.get("state")}))
            except Exception as exc:
                logger.warning(
                    _event_message(
                        "runner_shutdown_failed",
                        endpoint=endpoint,
                        error=str(exc),
                    )
                )

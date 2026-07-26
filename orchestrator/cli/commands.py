from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO
from typing import Any

from app.config import OrchestratorConfig, RunnerDefinition, _env_config, _read_yaml, load_config
from storage.db import (
    _target_sample_rows,
    cancel_jobs as db_cancel_jobs,
    fetch_batch_row,
    fetch_batch_rows,
    fetch_dataset_usage_rows,
    fetch_job_group_count,
    fetch_job_group_rows,
    fetch_job_rows,
    fetch_job_summary,
    fetch_latest_job_rows_by_sample,
    fetch_output_sample_index_rows,
    fetch_runner_usage_rows,
    fetch_runner_rows,
    fetch_sample_rows,
    insert_dataset_download_job,
    insert_jobs,
    sync_reference_state,
    update_jobs_allow_outside_window,
)
from domain.pipelines import (
    list_pipeline_definitions,
    load_pipeline,
    matrix_lanes,
    resolve_pipeline_path,
)
from execution.runner_client import get_status
from execution.pipelines import cleanup_pipeline_outputs
from execution.script_run import remove_script_containers, run_script_container
from domain.targets import DatasetTarget
from storage.pipelines import (
    cancel_pipeline_run,
    create_pipeline_run,
    fetch_pipeline_run,
    fetch_pipeline_runs,
    fetch_pipeline_stage_executions,
)

logger = logging.getLogger("scenegendeploybench.orchestrator")


@dataclass(frozen=True)
class JobListOptions:
    job_ids: list[str]
    dataset: str | None = None
    runner: str | None = None
    states: list[str] | None = None
    view: str = "groups"
    sort: str = "updated_at"
    desc: bool = True
    limit: int | None = None
    created_since: str | None = None
    created_until: str | None = None
    updated_since: str | None = None
    updated_until: str | None = None
    finished_since: str | None = None
    finished_until: str | None = None
    failed: bool = False
    active: bool = False
    completed: bool = False


@dataclass(frozen=True)
class JobRecordView:
    job_ref: str
    batch_id: str
    job_id: str
    dataset_name: str
    dataset_version: str
    external_key: str
    subset_key: str
    state: str
    runner_name: str
    runner_version: str
    runner_selector: str
    job_type: str
    attempt: int
    source_job_id: str | None
    created_at: str | None
    updated_at: str | None
    completed_at: str | None
    output_dir: str | None
    failure_code: str | None
    failure_message: str | None
    artifact_count: int
    metric_count: int
    allow_start_outside_window: bool
    request_payload: dict[str, Any]
    result_payload: dict[str, Any]


def _docker_log_stream() -> IO[str] | None:
    stream_path = Path("/proc/1/fd/1")
    try:
        if stream_path.exists():
            return stream_path.open("w", encoding="utf-8", buffering=1)
    except OSError:
        return None
    return None


def configure_logging(*, service_mode: bool = False) -> None:
    level_name = os.getenv("SCENEGENDEPLOYBENCH_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    stream = sys.stdout if service_mode else _docker_log_stream()
    if stream is None:
        handlers: list[logging.Handler] = [logging.NullHandler()]
    else:
        handlers = [logging.StreamHandler(stream)]
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
        force=True,
    )


def event_message(event: str, **fields: object) -> str:
    return json.dumps({"event": event, **fields}, sort_keys=True)


def _timestamp_value(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    normalized = str(value).strip()
    if not normalized:
        return None
    if normalized.endswith("+00:00"):
        return normalized[:-6] + "Z"
    return normalized


def _preferred_runner_for_name(config: OrchestratorConfig, runner_name: str) -> RunnerDefinition:
    candidates = config.runners_by_name.get(runner_name, ())
    if not candidates:
        available = ", ".join(sorted(config.runners))
        raise ValueError(f"unknown runner {runner_name!r}; available runners: {available}")
    latest_selector = config.latest_runners.get(runner_name)
    if latest_selector:
        return config.runners[latest_selector]
    return candidates[0]


def _artifact_count(result_payload: dict[str, Any], artifacts_payload: Any) -> int:
    artifacts = result_payload.get("artifacts")
    if isinstance(artifacts, list):
        return len(artifacts)
    if isinstance(artifacts_payload, list):
        return len(artifacts_payload)
    return 0


def _job_records_from_rows(rows: list[dict[str, Any]]) -> list[JobRecordView]:
    records: list[JobRecordView] = []
    for row in rows:
        request_payload = dict(row.get("request_json") or {})
        result_payload = dict(row.get("result_json") or {})
        artifacts_payload = list(row.get("artifacts_json") or [])
        state = str(row.get("status", "unknown")).strip() or "unknown"
        updated_at = _timestamp_value(row.get("updated_at_utc"))
        batch_id = str(row.get("batch_id") or "")
        job_id = str(row.get("job_id"))
        records.append(
            JobRecordView(
                job_ref=job_id,
                batch_id=batch_id,
                job_id=job_id,
                dataset_name=str(row.get("dataset_name", "")),
                dataset_version=str(row.get("dataset_version", "unversioned")),
                external_key=str(row.get("external_key", "")),
                subset_key=str(row.get("subset_key") or ""),
                state=state,
                runner_name=str(row.get("runner_name", "")),
                runner_version=str(row.get("runner_version", "")),
                runner_selector=str(row.get("runner_selector", "")),
                job_type=str(row.get("job_type", "generation")),
                attempt=int(row.get("attempt_count", 1)),
                source_job_id=row.get("source_job_id"),
                created_at=_timestamp_value(row.get("created_at_utc")),
                updated_at=updated_at,
                completed_at=_timestamp_value(row.get("completed_at_utc")),
                output_dir=row.get("output_dir"),
                failure_code=str(row.get("failure_code")) if row.get("failure_code") else None,
                failure_message=str(row.get("failure_message")) if row.get("failure_message") else None,
                artifact_count=int(row.get("artifact_count", _artifact_count(result_payload, artifacts_payload))),
                metric_count=int(row.get("metric_count", len(result_payload.get("metrics", [])) if isinstance(result_payload.get("metrics"), list) else 0)),
                allow_start_outside_window=bool(row.get("allow_start_outside_window", False)),
                request_payload=request_payload,
                result_payload=result_payload,
            )
        )
    return records


def load_job_records(config: OrchestratorConfig, **filters: Any) -> list[JobRecordView]:
    return _job_records_from_rows(fetch_job_rows(config, **filters))


def _refresh_reference_cache(config: OrchestratorConfig) -> None:
    sync_reference_state(config)


def resolve_job_record(records: list[JobRecordView], query: str) -> JobRecordView:
    query = query.strip()
    exact_matches = [record for record in records if record.job_ref == query]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise ValueError(f"multiple jobs matched {query!r}")

    raw_matches = [record for record in records if record.job_id == query]
    if len(raw_matches) == 1:
        return raw_matches[0]
    if len(raw_matches) > 1:
        options = ", ".join(record.job_ref for record in raw_matches[:5])
        raise ValueError(f"job id {query!r} is ambiguous; use one of: {options}")
    raise ValueError(f"job {query!r} was not found")


def runner_live_status(runner: RunnerDefinition) -> dict[str, Any]:
    endpoint = runner.launcher.get("endpoint") or {}
    base_url = endpoint.get("base_url") if isinstance(endpoint, dict) else None
    if not base_url:
        return {
            "runner_selector": runner.selector,
            "state": "unknown",
            "message": "no static_http base_url configured",
        }
    payload = get_status(str(base_url))
    payload["runner_selector"] = runner.selector
    payload["base_url"] = str(base_url)
    return payload


def resolve_runner(config: OrchestratorConfig, runner_selector: str | None) -> RunnerDefinition:
    if runner_selector:
        normalized = runner_selector.strip()
        if normalized in config.runners:
            return config.runners[normalized]
        if "@" in normalized:
            runner_name, version = normalized.split("@", 1)
            if runner_name and version.lower() == "latest":
                return _preferred_runner_for_name(config, runner_name)
        if normalized in config.runners_by_name:
            return _preferred_runner_for_name(config, normalized)
        available = ", ".join(sorted(config.runners))
        raise ValueError(
            f"unknown runner {runner_selector!r}; available runners: {available}"
        )
    if len(config.runners) == 1:
        return next(iter(config.runners.values()))
    if len(config.runners_by_name) == 1:
        return _preferred_runner_for_name(config, next(iter(config.runners_by_name)))
    available = ", ".join(sorted(config.runners))
    raise ValueError(f"--runner is required when multiple runners exist: {available}")


def _resolve_runner_for_job_add(
    config: OrchestratorConfig,
    *,
    runner: str | None,
) -> RunnerDefinition:
    return resolve_runner(config, runner)


def _job_type_for_runner(runner: RunnerDefinition) -> str:
    job_types = {
        "generator": "generation",
        "evaluator": "evaluation",
    }
    try:
        return job_types[runner.kind]
    except KeyError as exc:
        if runner.kind == "dataset_downloader":
            raise ValueError(
                f"runner {runner.selector} is a dataset downloader; use 'dataset download' instead of 'job add'"
            ) from exc
        raise ValueError(
            f"runner {runner.selector} has unsupported kind {runner.kind!r} for 'job add'"
        ) from exc


def _resolve_dataset_downloader(
    config: OrchestratorConfig,
    *,
    runner: str | None,
) -> RunnerDefinition:
    if runner:
        resolved = resolve_runner(config, runner)
        if resolved.kind != "dataset_downloader":
            raise ValueError(f"runner {resolved.selector} has kind {resolved.kind!r}, expected 'dataset_downloader'")
        return resolved

    candidates = [candidate for candidate in config.runners.values() if candidate.kind == "dataset_downloader"]
    if len(candidates) == 1:
        return candidates[0]
    available = ", ".join(sorted(candidate.selector for candidate in candidates)) or "none"
    raise ValueError(
        "--runner is required when dataset downloader count is not one; "
        f"available dataset downloaders: {available}"
    )


def _coerce_setting_value(key: str, value: Any, default: Any) -> Any:
    values = value if isinstance(value, list) else [value]

    if isinstance(default, list):
        items: list[str] = []
        for raw_item in values:
            items.extend(
                item.strip()
                for item in str(raw_item).split(",")
                if item.strip()
            )
        return items

    if len(values) != 1:
        raise ValueError(f"--set {key} may only be specified once")
    raw_value = str(values[0])
    normalized = raw_value.strip()

    if isinstance(default, bool):
        boolean_values = {"true": True, "false": False}
        try:
            return boolean_values[normalized.lower()]
        except KeyError as exc:
            raise ValueError(
                f"--set {key} expects true or false, got {raw_value!r}"
            ) from exc
    if isinstance(default, int):
        try:
            return int(normalized)
        except ValueError as exc:
            raise ValueError(f"--set {key} expects an integer, got {raw_value!r}") from exc
    if isinstance(default, float):
        try:
            return float(normalized)
        except ValueError as exc:
            raise ValueError(f"--set {key} expects a number, got {raw_value!r}") from exc
    if isinstance(default, dict):
        try:
            parsed = json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise ValueError(f"--set {key} expects a JSON object") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"--set {key} expects a JSON object")
        return parsed
    return raw_value


def parse_key_value_settings(
    values: list[str] | None,
    defaults: dict[str, Any] | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for raw_value in values or []:
        key, separator, value = raw_value.partition("=")
        key = key.strip()
        if not key or not separator:
            raise ValueError("--set values must use key=value")
        if key in params:
            existing = params[key]
            if isinstance(existing, list):
                existing.append(value)
            else:
                params[key] = [existing, value]
        else:
            params[key] = value
    for key, value in list(params.items()):
        if defaults is not None and key in defaults:
            params[key] = _coerce_setting_value(key, value, defaults[key])
    return params


def _deep_merge_params(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_params(merged[key], value)
        else:
            merged[key] = value
    return merged


def _job_parameters(runner: RunnerDefinition, settings: list[str] | None) -> dict[str, Any]:
    overrides = parse_key_value_settings(settings, runner.job_parameters)
    return _deep_merge_params(runner.job_parameters, overrides)


def load_runtime_config(config_path: str | None) -> OrchestratorConfig:
    return load_config(config_path)


def _state_bucket(state: str) -> str:
    normalized = state.strip().lower()
    if normalized in {"finished", "completed", "finished_on_runner"}:
        return "completed"
    if normalized == "cancelled":
        return "cancelled"
    if normalized in {"failed", "rejected"}:
        return "failed"
    return "pending"


def _runner_filter_values(
    config: OrchestratorConfig,
    runner: str | None,
) -> tuple[str | None, str | None]:
    normalized = (runner or "").strip()
    if not normalized:
        return None, None
    if "@" in normalized or normalized in config.runners:
        resolved = resolve_runner(config, normalized)
        return None, resolved.selector
    if normalized in config.runners_by_name:
        return normalized, None
    available = ", ".join(sorted(config.runners))
    raise ValueError(f"unknown runner {runner!r}; available runners: {available}")


def _dataset_subset_path(dataset_name: str, subset_key: str) -> str:
    subset = subset_key.strip()
    if not subset:
        return dataset_name
    return f"{dataset_name}/{subset}"


def _group_job_records(records: list[JobRecordView], *, desc: bool = True) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for record in records:
        subset = record.subset_key or ""
        key = (record.dataset_name, subset, record.runner_selector)
        group = groups.setdefault(
            key,
            {
                "dataset": _dataset_subset_path(record.dataset_name, subset),
                "runner": record.runner_selector,
                "total": 0,
                "completed": 0,
                "pending": 0,
                "failed": 0,
                "cancelled": 0,
                "last_update": None,
            },
        )
        group["total"] += 1
        group[_state_bucket(record.state)] += 1
        last_update = record.updated_at or record.created_at
        if last_update and (group["last_update"] is None or last_update > group["last_update"]):
            group["last_update"] = last_update

    grouped_rows = list(groups.values())
    return sorted(grouped_rows, key=lambda row: row.get("last_update") or "", reverse=desc)


def _job_record_rows(records: list[JobRecordView]) -> list[dict[str, Any]]:
    return [
        {
            "job_ref": record.job_ref,
            "job_id": record.job_id,
            "batch_id": record.batch_id,
            "dataset": record.dataset_name,
            "sample": record.external_key,
            "runner": record.runner_selector,
            "state": record.state,
            "attempt": record.attempt,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
            "completed_at": record.completed_at,
        }
        for record in records
    ]


def _job_records_by_ids(config: OrchestratorConfig, job_ids: list[str]) -> list[JobRecordView]:
    if not job_ids:
        return []
    selected = set(job_ids)
    records = [
        record
        for record in load_job_records(config, job_ids=job_ids)
        if record.job_id in selected or record.job_ref in selected
    ]
    records_by_id = {record.job_id: record for record in records}
    return [records_by_id[job_id] for job_id in job_ids if job_id in records_by_id]


def list_jobs(config_path: str | None, options: JobListOptions) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    runner_name_filter, runner_selector_filter = _runner_filter_values(config, options.runner)
    raw_view = options.view == "jobs" or bool(options.job_ids)
    summary = fetch_job_summary(
        config,
        job_ids=options.job_ids,
        dataset=options.dataset,
        runner=runner_name_filter,
        runner_selector=runner_selector_filter,
        states=options.states,
        failed=options.failed,
        active=options.active,
        completed=options.completed,
        created_since=options.created_since,
        created_until=options.created_until,
        updated_since=options.updated_since,
        updated_until=options.updated_until,
        finished_since=options.finished_since,
        finished_until=options.finished_until,
    )

    if raw_view:
        records = _job_records_from_rows(
            fetch_job_rows(
                config,
                job_ids=options.job_ids,
                dataset=options.dataset,
                runner=runner_name_filter,
                runner_selector=runner_selector_filter,
                states=options.states,
                failed=options.failed,
                active=options.active,
                completed=options.completed,
                created_since=options.created_since,
                created_until=options.created_until,
                updated_since=options.updated_since,
                updated_until=options.updated_until,
                finished_since=options.finished_since,
                finished_until=options.finished_until,
                sort=options.sort,
                desc=options.desc,
                limit=options.limit,
            )
        )
        rows = _job_record_rows(records)
        return {"view": "jobs", "summary": summary, "rows": rows}

    grouped_rows = []
    for row in fetch_job_group_rows(
        config,
        job_ids=options.job_ids,
        dataset=options.dataset,
        runner=runner_name_filter,
        runner_selector=runner_selector_filter,
        states=options.states,
        failed=options.failed,
        active=options.active,
        completed=options.completed,
        created_since=options.created_since,
        created_until=options.created_until,
        updated_since=options.updated_since,
        updated_until=options.updated_until,
        finished_since=options.finished_since,
        finished_until=options.finished_until,
        sort=options.sort,
        desc=options.desc,
        limit=options.limit,
    ):
        subset = str(row.get("subset_key") or "")
        grouped_rows.append(
            {
                "dataset": _dataset_subset_path(str(row.get("dataset_name") or ""), subset),
                "runner": str(row.get("runner_selector") or ""),
                "total": int(row.get("total") or 0),
                "completed": int(row.get("completed") or 0),
                "pending": int(row.get("pending") or 0),
                "failed": int(row.get("failed") or 0),
                "cancelled": int(row.get("cancelled") or 0),
                "last_update": _timestamp_value(row.get("last_update_utc")),
            }
        )
    summary["group_count"] = fetch_job_group_count(
        config,
        job_ids=options.job_ids,
        dataset=options.dataset,
        runner=runner_name_filter,
        runner_selector=runner_selector_filter,
        states=options.states,
        failed=options.failed,
        active=options.active,
        completed=options.completed,
        created_since=options.created_since,
        created_until=options.created_until,
        updated_since=options.updated_since,
        updated_until=options.updated_until,
        finished_since=options.finished_since,
        finished_until=options.finished_until,
    )
    return {"view": "groups", "summary": summary, "rows": grouped_rows}


def show_job(config_path: str | None, job_id: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    record = resolve_job_record(load_job_records(config, job_ids=[job_id.strip()]), job_id)
    job_payload = record.request_payload.get("job") or {}
    return {
        "job_ref": record.job_ref,
        "job_id": record.job_id,
        "batch_id": record.batch_id,
        "state": record.state,
        "job_type": record.job_type,
        "runner": record.runner_selector,
        "dataset": record.dataset_name,
        "dataset_version": record.dataset_version,
        "sample": record.external_key,
        "attempt": record.attempt,
        "source_job_id": record.source_job_id,
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "completed_at": record.completed_at,
        "output_dir": record.output_dir,
        "failure_code": record.failure_code,
        "failure_message": record.failure_message,
        "artifact_count": record.artifact_count,
        "metric_count": record.metric_count,
        "allow_start_outside_window": record.allow_start_outside_window,
        "sample_metadata": job_payload.get("primary_sample_metadata") or {},
        "result": record.result_payload,
        "request": record.request_payload,
    }


def list_batches(
    config_path: str | None,
    *,
    runner: str | None = None,
    open_only: bool = False,
    closed_only: bool = False,
) -> list[dict[str, Any]]:
    if open_only and closed_only:
        raise ValueError("choose at most one of --open or --closed")
    config = load_runtime_config(config_path)
    runner_name_filter, runner_selector_filter = _runner_filter_values(config, runner)
    rows: list[dict[str, Any]] = []
    for row in fetch_batch_rows(config):
        if runner_name_filter and str(row.get("runner_name") or "") != runner_name_filter:
            continue
        if runner_selector_filter and str(row.get("runner_selector") or "") != runner_selector_filter:
            continue
        is_open = row.get("closed_at_utc") is None
        if open_only and not is_open:
            continue
        if closed_only and is_open:
            continue
        rows.append(
            {
                "batch_id": str(row.get("batch_id") or ""),
                "runner": str(row.get("runner_selector") or ""),
                "state": "open" if is_open else "closed",
                "runner_endpoint": str(row.get("runner_endpoint") or "").strip() or None,
                "job_count": int(row.get("job_count") or 0),
                "pending": int(row.get("pending_job_count") or 0),
                "completed": int(row.get("completed_job_count") or 0),
                "failed": int(row.get("failed_job_count") or 0),
                "cancelled": int(row.get("cancelled_job_count") or 0),
                "created_at": _timestamp_value(row.get("created_at_utc")),
                "updated_at": _timestamp_value(row.get("updated_at_utc")),
                "closed_at": _timestamp_value(row.get("closed_at_utc")),
            }
        )
    return rows


def show_batch(config_path: str | None, batch_id: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    normalized_batch_id = batch_id.strip()
    row = fetch_batch_row(config, normalized_batch_id)
    if row is None:
        raise ValueError(f"unknown batch_id {batch_id!r}")
    jobs = load_job_records(
        config,
        batch_id=normalized_batch_id,
        sort="created_at",
        desc=False,
    )
    return {
        "batch_id": str(row.get("batch_id") or ""),
        "runner": str(row.get("runner_selector") or ""),
        "runner_name": str(row.get("runner_name") or ""),
        "runner_type": str(row.get("runner_type") or ""),
        "runner_version": str(row.get("runner_version") or ""),
        "state": "open" if row.get("closed_at_utc") is None else "closed",
        "runner_endpoint": str(row.get("runner_endpoint") or "").strip() or None,
        "job_ids": list(row.get("job_ids_json") or []),
        "job_count": int(row.get("job_count") or 0),
        "pending": int(row.get("pending_job_count") or 0),
        "completed": int(row.get("completed_job_count") or 0),
        "failed": int(row.get("failed_job_count") or 0),
        "cancelled": int(row.get("cancelled_job_count") or 0),
        "created_at": _timestamp_value(row.get("created_at_utc")),
        "updated_at": _timestamp_value(row.get("updated_at_utc")),
        "closed_at": _timestamp_value(row.get("closed_at_utc")),
        "jobs": _job_record_rows(jobs),
    }


def list_runners(config_path: str | None) -> list[dict[str, Any]]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    usage_by_runner = {
        str(row.get("runner_selector") or ""): row
        for row in fetch_runner_usage_rows(config)
    }

    rows: list[dict[str, Any]] = []
    for runner in fetch_runner_rows(config):
        usage = usage_by_runner.get(str(runner["selector"]) or "", {})
        rows.append(
            {
                "runner": runner["runner_name"],
                "selector": runner["selector"],
                "type": runner["runner_type"],
                "version": runner["version"],
                "latest": bool(runner["latest"]),
                "image": runner.get("container_image"),
                "launcher_driver": runner.get("launcher_driver"),
                "last_seen": _timestamp_value(usage.get("last_seen_utc")),
                "missing": bool(runner["missing"]),
            }
        )
    return rows


def show_runner(config_path: str | None, runner_selector: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    runner = resolve_runner(config, runner_selector)
    job_counts = fetch_job_summary(config, runner_selector=runner.selector)
    cached_runner = next(iter(fetch_runner_rows(config, selector=runner.selector)), None)
    return {
        "requested_runner": runner_selector,
        "selector": runner.selector,
        "name": runner.runner,
        "display_name": runner.display_name,
        "type": runner.kind,
        "version": runner.version,
        "latest": runner.latest,
        "contract_version": runner.contract_version,
        "inputs": runner.inputs,
        "job_parameters": runner.job_parameters,
        "scheduling": runner.scheduling,
        "launcher": runner.launcher,
        "missing": bool(cached_runner["missing"]) if cached_runner is not None else False,
        "job_counts": {
            "total": job_counts["job_count"],
            "completed": job_counts["completed"],
            "pending": job_counts["pending"],
            "failed": job_counts["failed"],
            "cancelled": job_counts["cancelled"],
        },
    }


def get_runner_status(config_path: str | None, runner_selector: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    runner = resolve_runner(config, runner_selector)
    try:
        return runner_live_status(runner)
    except RuntimeError as exc:
        return {
            "runner_selector": runner.selector,
            "state": "unknown",
            "message": str(exc),
        }


def list_datasets(config_path: str | None, target: str | None = None) -> list[dict[str, Any]]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    dataset_target = (target or "").strip()
    if dataset_target:
        parsed_target = DatasetTarget.parse(dataset_target)
        samples = [
            sample for sample in fetch_sample_rows(config, dataset=dataset_target)
            if parsed_target.matches(sample["dataset_name"], sample["external_key"])
        ]
        latest_job = {
            (record.dataset_name, record.external_key): record
            for record in _job_records_from_rows(fetch_latest_job_rows_by_sample(config, dataset=dataset_target))
        }
        children: dict[str, dict[str, Any]] = {}
        for sample in samples:
            external_key = str(sample["external_key"])
            relative_path = parsed_target.relative_external_key(external_key)
            if relative_path is None:
                continue
            child_name, separator, _ = relative_path.partition("/")
            kind = "subset" if separator else "sample"
            child_path = parsed_target.child_path(child_name)
            child = children.setdefault(
                child_path,
                {
                    "path": child_path,
                    "kind": kind,
                    "version": sample["dataset_version"],
                    "samples": 0,
                    "data_types": [],
                    "last_job": None,
                    "last_state": None,
                    "updated_at": None,
                },
            )
            if child["kind"] != "subset" and kind == "subset":
                child["kind"] = "subset"
            child["samples"] += 1
            for data_type in sample["data_types_json"] or []:
                if data_type not in child["data_types"]:
                    child["data_types"].append(data_type)
            record = latest_job.get((str(sample["dataset_name"]), external_key))
            if record is not None:
                updated_at = record.updated_at or record.created_at
                if updated_at and (child["updated_at"] is None or updated_at > child["updated_at"]):
                    child["last_job"] = record.job_ref
                    child["last_state"] = record.state
                    child["updated_at"] = updated_at
        return sorted(children.values(), key=lambda row: (row["kind"] != "subset", row["path"]))

    usage_by_dataset = {
        str(row.get("dataset_name") or ""): row
        for row in fetch_dataset_usage_rows(config)
    }
    samples_by_dataset: dict[str, list[dict[str, Any]]] = {}
    for sample in fetch_sample_rows(config):
        samples_by_dataset.setdefault(sample["dataset_name"], []).append(sample)
    rows: list[dict[str, Any]] = []
    for dataset_name in sorted(samples_by_dataset):
        samples = samples_by_dataset[dataset_name]
        usage = usage_by_dataset.get(dataset_name, {})
        data_types: list[str] = []
        for sample in samples:
            for data_type in sample["dataset_data_types_json"]:
                if data_type not in data_types:
                    data_types.append(data_type)
        version = samples[0]["dataset_version"] if samples else "unversioned"
        rows.append(
            {
                "dataset": dataset_name,
                "version": version,
                "samples": len(samples),
                "data_types": data_types,
                "last_job": _timestamp_value(usage.get("last_job_utc")),
            }
        )
    return rows


def show_dataset(config_path: str | None, target: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    dataset_target = DatasetTarget.parse(target)
    samples = [
        sample for sample in fetch_sample_rows(config, dataset=target)
        if dataset_target.matches(sample["dataset_name"], sample["external_key"])
    ]
    job_counts = fetch_job_summary(config, dataset=target)
    data_types: list[str] = []
    subset_counts: dict[str, int] = {}
    for sample in samples:
        for data_type in sample["dataset_data_types_json"]:
            if data_type not in data_types:
                data_types.append(data_type)
        subset = sample.get("subset_key")
        if not subset:
            external_key = str(sample["external_key"])
            subset = external_key.rsplit("/", 1)[0] if "/" in external_key else ""
        subset_path = _dataset_subset_path(dataset_target.dataset_name, str(subset or ""))
        subset_counts[subset_path] = subset_counts.get(subset_path, 0) + 1
    return {
        "target": target,
        "dataset": dataset_target.path,
        "version": samples[0]["dataset_version"] if samples else "unversioned",
        "sample_count": len(samples),
        "data_types": data_types,
        "subset_counts": subset_counts,
        "job_counts": {
            "total": job_counts["job_count"],
            "completed": job_counts["completed"],
            "pending": job_counts["pending"],
            "failed": job_counts["failed"],
            "cancelled": job_counts["cancelled"],
        },
    }


def list_dataset_samples(config_path: str | None, target: str) -> list[dict[str, Any]]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    dataset_target = DatasetTarget.parse(target)
    samples = [
        sample for sample in fetch_sample_rows(config, dataset=target)
        if dataset_target.matches(sample["dataset_name"], sample["external_key"])
    ]
    latest_job = {
        (record.dataset_name, record.external_key): record
        for record in _job_records_from_rows(fetch_latest_job_rows_by_sample(config, dataset=target))
    }
    rows: list[dict[str, Any]] = []
    for sample in samples:
        record = latest_job.get((sample["dataset_name"], sample["external_key"]))
        rows.append(
            {
                "sample": dataset_target.display_relative_external_key(sample["external_key"]),
                "external_key": sample["external_key"],
                "data_types": list(sample["data_types_json"] or []),
                "last_job": record.job_ref if record else None,
                "last_state": record.state if record else None,
                "updated_at": record.updated_at if record else None,
            }
        )
    return rows


def _output_target_path(runner_selector: str, dataset_name: str, subset_key: str = "") -> str:
    parts = ["output", runner_selector, dataset_name]
    if subset_key:
        parts.append(subset_key.strip("/"))
    return "/".join(part for part in parts if part)


def list_outputs(
    config_path: str | None,
    *,
    dataset: str | None = None,
    runner: str | None = None,
) -> list[dict[str, Any]]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    runner_name_filter, runner_selector_filter = _runner_filter_values(config, runner)
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in fetch_output_sample_index_rows(
        config,
        dataset=dataset,
        runner=runner_name_filter,
        runner_selector=runner_selector_filter,
    ):
        runner_selector = str(row.get("source_runner_selector") or "")
        dataset_name = str(row.get("dataset_name") or "")
        subset = str(row.get("subset_key") or "")
        if not subset:
            external_key = str(row.get("external_key") or "")
            subset = external_key.rsplit("/", 1)[0] if "/" in external_key else ""
        key = (runner_selector, dataset_name, subset)
        group = groups.setdefault(
            key,
            {
                "target": _output_target_path(runner_selector, dataset_name, subset),
                "runner": runner_selector,
                "dataset": dataset_name,
                "subset": subset,
                "samples": 0,
                "data_types": [],
                "updated_at": None,
            },
        )
        group["samples"] += 1
        for data_type in row.get("data_types_json") or []:
            normalized = str(data_type)
            if normalized not in group["data_types"]:
                group["data_types"].append(normalized)
        updated_at = _timestamp_value(row.get("updated_at_utc"))
        if updated_at and (group["updated_at"] is None or updated_at > group["updated_at"]):
            group["updated_at"] = updated_at
    return sorted(groups.values(), key=lambda item: (item["runner"], item["dataset"], item["subset"]))


def show_output(config_path: str | None, target: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    _refresh_reference_cache(config)
    rows = fetch_output_sample_index_rows(config, target=target)
    data_types: list[str] = []
    samples: list[dict[str, Any]] = []
    for row in rows:
        for data_type in row.get("data_types_json") or []:
            normalized = str(data_type)
            if normalized not in data_types:
                data_types.append(normalized)
        samples.append(
            {
                "sample": row.get("external_key"),
                "source_job_id": row.get("source_job_id"),
                "runner": row.get("source_runner_selector"),
                "dataset": row.get("dataset_name"),
                "dataset_version": row.get("dataset_version"),
                "data_types": list(row.get("data_types_json") or []),
                "outputs": dict(row.get("outputs_json") or {}),
                "metadata": dict(row.get("metadata_json") or {}),
                "updated_at": _timestamp_value(row.get("updated_at_utc")),
            }
        )
    return {
        "target": target,
        "sample_count": len(samples),
        "data_types": data_types,
        "samples": samples,
    }


def update_jobs_window_flag(
    config_path: str | None,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    allow: bool,
) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    runner_name_filter, runner_selector_filter = _runner_filter_values(config, runner)
    payload = update_jobs_allow_outside_window(
        config,
        job_ids=job_ids,
        dataset=dataset,
        runner=runner_name_filter,
        runner_selector=runner_selector_filter,
        allow=allow,
    )
    records = _job_records_by_ids(config, list(payload.get("jobs") or []))
    payload["groups"] = _group_job_records(records)
    payload["job_rows"] = _job_record_rows(records)
    return payload


def cancel_jobs_matching_filters(
    config_path: str | None,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    runner_name_filter, runner_selector_filter = _runner_filter_values(config, runner)
    payload = db_cancel_jobs(
        config,
        job_ids=job_ids,
        dataset=dataset,
        runner=runner_name_filter,
        runner_selector=runner_selector_filter,
    )
    records = _job_records_by_ids(config, list(payload.get("jobs") or []))
    payload["groups"] = _group_job_records(records)
    payload["job_rows"] = _job_record_rows(records)
    return payload


def _runner_job_timeout_seconds(runner: RunnerDefinition, override_minutes: float | None) -> int:
    timeout_minutes = (
        override_minutes
        if override_minutes is not None
        else float(runner.scheduling.get("job_timeout_minutes") or 60)
    )
    if timeout_minutes <= 0:
        raise ValueError("timeout_minutes must be greater than 0")
    return int(timeout_minutes * 60)


def add_job(
    config_path: str | None,
    *,
    dataset: str | None,
    candidate: str | None,
    runner: str | None,
    references: list[str] | None,
    settings: list[str] | None,
    timeout_minutes: float | None,
    source_job_id: str | None,
    allow_start_outside_window: bool,
    batch_id: str | None,
    job_id: str | None,
) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    resolved_runner = _resolve_runner_for_job_add(
        config,
        runner=runner,
    )
    if batch_id:
        raise ValueError("batch ids are runtime-only; job add does not accept --batch-id")
    if job_id:
        raise ValueError("job ids are generated by the durable jobs store")
    job_type = _job_type_for_runner(resolved_runner)
    parameters = _job_parameters(resolved_runner, settings)
    effective_timeout_seconds = _runner_job_timeout_seconds(resolved_runner, timeout_minutes)
    payload = insert_jobs(
        config,
        dataset=dataset,
        candidate=candidate,
        references=references or [],
        runner=resolved_runner,
        job_type=job_type,
        parameters=parameters,
        timeout_seconds=effective_timeout_seconds,
        source_job_id=source_job_id.strip() if source_job_id else None,
        allow_start_outside_window=allow_start_outside_window,
    )
    created_ids = [str(row.get("job_ref") or row.get("job_id") or "") for row in payload.get("jobs", [])]
    records = _job_records_by_ids(config, [job_id for job_id in created_ids if job_id])
    payload["groups"] = _group_job_records(records)
    payload["job_rows"] = _job_record_rows(records)
    return payload


def download_dataset(
    config_path: str | None,
    *,
    dataset_name: str,
    runner: str | None,
    settings: list[str] | None,
    timeout_minutes: float | None,
    allow_start_outside_window: bool,
) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    resolved_runner = _resolve_dataset_downloader(config, runner=runner)
    parameters = _job_parameters(resolved_runner, settings)
    effective_timeout_seconds = _runner_job_timeout_seconds(resolved_runner, timeout_minutes)
    payload = insert_dataset_download_job(
        config,
        dataset_name=dataset_name,
        runner=resolved_runner,
        parameters=parameters,
        timeout_seconds=effective_timeout_seconds,
        allow_start_outside_window=allow_start_outside_window,
    )
    created_ids = [str(row.get("job_ref") or row.get("job_id") or "") for row in payload.get("jobs", [])]
    records = _job_records_by_ids(config, [job_id for job_id in created_ids if job_id])
    payload["groups"] = _group_job_records(records)
    payload["job_rows"] = _job_record_rows(records)
    return payload


def run_script(
    config_path: str | None,
    *,
    image: str,
    script_path: str | None,
    command: list[str],
    access: list[str] | None,
    environment: list[str] | None,
    mounts: list[str] | None,
    workdir: str,
) -> int:
    return run_script_container(
        load_runtime_config(config_path),
        image=image,
        script_path=script_path,
        command=command,
        access_values=access,
        environment_values=environment,
        mount_values=mounts,
        workdir=workdir,
    ).exit_code


def _matrix_overrides(values: list[str] | None) -> dict[str, list[Any]]:
    overrides: dict[str, list[Any]] = {}
    for raw_value in values or []:
        key, separator, raw_items = raw_value.partition("=")
        key = key.strip()
        if not key or not separator:
            raise ValueError("--matrix values must use key=value1,value2")
        items = [item.strip() for item in raw_items.split(",") if item.strip()]
        if not items:
            raise ValueError(f"--matrix {key} requires at least one value")
        parsed_items: list[Any] = []
        for item in items:
            if item.lower() in {"true", "false"}:
                parsed_items.append(item.lower() == "true")
                continue
            try:
                parsed_items.append(int(item))
                continue
            except ValueError:
                pass
            try:
                parsed_items.append(float(item))
                continue
            except ValueError:
                parsed_items.append(item)
        overrides[key] = parsed_items
    return overrides


def list_pipelines(config_path: str | None) -> list[dict[str, Any]]:
    config = load_runtime_config(config_path)
    return [
        {
            "name": definition.name,
            "path": str(definition.path),
            "dataset": definition.dataset,
            "matrix_lane_count": len(matrix_lanes(definition.matrix)),
            "stage_count": len(definition.stages),
        }
        for definition in list_pipeline_definitions(config.catalogs.pipelines)
    ]


def _pipeline_row_payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    for key in (
        "created_at_utc",
        "updated_at_utc",
        "completed_at_utc",
    ):
        if key in payload:
            payload[key] = _timestamp_value(payload.get(key))
    return payload


def validate_pipeline(
    config_path: str | None,
    *,
    name: str | None,
    file_path: str | None,
) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    path = resolve_pipeline_path(
        config.catalogs.pipelines,
        name=name,
        file_path=file_path,
    )
    definition = load_pipeline(path)
    for stage in definition.stages.values():
        runner_selector = str(stage.get("runner") or "").strip()
        if runner_selector:
            resolve_runner(config, runner_selector)
    return {
        "valid": True,
        "name": definition.name,
        "path": str(definition.path),
        "dataset": definition.dataset,
        "matrix_lane_count": len(matrix_lanes(definition.matrix)),
        "stages": list(definition.stages),
    }


def add_pipeline(
    config_path: str | None,
    *,
    name: str | None,
    file_path: str | None,
    dataset: str | None,
    matrix_values: list[str] | None,
    allow_start_outside_window: bool,
) -> dict[str, Any]:
    from storage.db import ensure_schema

    config = load_runtime_config(config_path)
    ensure_schema(config)
    sync_reference_state(config)
    path = resolve_pipeline_path(
        config.catalogs.pipelines,
        name=name,
        file_path=file_path,
    )
    definition = load_pipeline(path)
    dataset_target = str(dataset or definition.dataset or "").strip()
    if not dataset_target:
        raise ValueError("pipeline dataset is required in YAML or with --dataset")
    has_dataset_downloader = any(
        (
            resolve_runner(config, str(stage["runner"])).kind
            == "dataset_downloader"
        )
        for stage in definition.stages.values()
        if stage.get("runner")
    )
    try:
        _target_sample_rows(config, dataset_target, option_name="pipeline dataset")
    except (FileNotFoundError, ValueError):
        if not has_dataset_downloader:
            raise
    matrix = dict(definition.matrix)
    for key, values in _matrix_overrides(matrix_values).items():
        if key not in matrix:
            raise ValueError(f"--matrix references undefined matrix key {key!r}")
        matrix[key] = values
    config_payload = {
        **definition.raw,
        "dataset": dataset_target,
        "matrix": matrix,
    }
    lanes = matrix_lanes(matrix)
    payload = create_pipeline_run(
        config,
        pipeline_name=definition.name,
        dataset_target=dataset_target,
        config_path=str(definition.path),
        config_payload=config_payload,
        lanes=lanes,
        allow_start_outside_window=allow_start_outside_window,
    )
    run = fetch_pipeline_run(config, str(payload["pipeline_run_id"]))
    if run is None:
        raise RuntimeError("created pipeline run could not be loaded")
    return _pipeline_row_payload(dict(run))


def show_pipeline_run(
    config_path: str | None,
    pipeline_run_id: str,
) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    run = fetch_pipeline_run(config, pipeline_run_id)
    if run is None:
        raise ValueError(f"pipeline run {pipeline_run_id!r} was not found")
    stages = []
    for row in fetch_pipeline_stage_executions(config, pipeline_run_id):
        stages.append(_pipeline_row_payload(row))
    return {
        **_pipeline_row_payload(dict(run)),
        "stages": stages,
    }


def list_pipeline_runs(config_path: str | None) -> list[dict[str, Any]]:
    return [
        _pipeline_row_payload(row)
        for row in fetch_pipeline_runs(load_runtime_config(config_path))
    ]


def cancel_pipeline(config_path: str | None, pipeline_run_id: str) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    run = fetch_pipeline_run(config, pipeline_run_id)
    if run is None:
        raise ValueError(f"pipeline run {pipeline_run_id!r} was not found")
    payload = cancel_pipeline_run(
        config,
        pipeline_run_id=pipeline_run_id,
    )
    cleanup_pipeline_outputs(config, run)
    try:
        payload["stopped_script_containers"] = remove_script_containers(
            pipeline_run_id=pipeline_run_id,
        )
    except RuntimeError:
        payload["stopped_script_containers"] = 0
    return payload


def show_config_payload(config_path: str | None) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    logger.info(
        event_message(
            "config_loaded",
            config_version=config.config_version,
            dataset_root=str(config.storage.dataset_root),
            model_cache_root=str(config.storage.model_cache_root),
            output_root=str(config.storage.output_root),
            pipeline_root=str(config.storage.pipeline_root),
            runner_count=len(config.runners),
        )
    )
    logger.info(event_message("show_config"))
    return config.raw


def config_show_sections(config_path: str | None) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    return {
        "storage": {
            "dataset_root": str(config.storage.dataset_root),
            "model_cache_root": str(config.storage.model_cache_root),
            "output_root": str(config.storage.output_root),
            "pipeline_root": str(config.storage.pipeline_root),
        },
        "catalogs": {
            "runners": str(config.catalogs.runners),
            "pipelines": str(config.catalogs.pipelines),
        },
        "database": {
            "host": config.database.host,
            "port": config.database.port,
            "name": config.database.name,
            "user": config.database.user,
        },
        "polling": {
            "startup_seconds": config.orchestrator.polling.startup_seconds,
            "post_submit_seconds": config.orchestrator.polling.post_submit_seconds,
            "running_seconds": config.orchestrator.polling.running_seconds,
        },
        "runner_env": config.raw.get("orchestrator", {}).get("runner_env", {}),
        "scheduling": config.orchestrator.scheduling,
    }


def config_validate_payload(config_path: str | None) -> dict[str, Any]:
    config = load_runtime_config(config_path)
    return {
        "valid": True,
        "config_version": config.config_version,
        "runner_count": len(config.runners),
        "dataset_root": str(config.storage.dataset_root),
        "model_cache_root": str(config.storage.model_cache_root),
        "output_root": str(config.storage.output_root),
        "pipeline_root": str(config.storage.pipeline_root),
        "db_host": config.database.host,
        "db_name": config.database.name,
    }


def _yaml_source(config_path: str | None) -> dict[str, Any]:
    path = config_path or os.getenv("PATH_CONFIG_SYSTEM")
    return _read_yaml(Path(path).resolve()) if path else {}


def config_sources_payload(config_path: str | None) -> list[dict[str, Any]]:
    yaml_source = _yaml_source(config_path)
    env_source = _env_config()
    effective = load_runtime_config(config_path)

    rows = [
        {
            "key": "storage.dataset_root",
            "value": str(effective.storage.dataset_root),
            "source": "PATH_DATASETS" if env_source.get("storage", {}).get("dataset_root") else "system.yaml" if yaml_source.get("storage", {}).get("dataset_root") else "default",
        },
        {
            "key": "storage.model_cache_root",
            "value": str(effective.storage.model_cache_root),
            "source": "PATH_MODEL_CACHE" if env_source.get("storage", {}).get("model_cache_root") else "system.yaml" if yaml_source.get("storage", {}).get("model_cache_root") else "default",
        },
        {
            "key": "storage.output_root",
            "value": str(effective.storage.output_root),
            "source": (
                "PATH_OUTPUT"
                if os.getenv("PATH_OUTPUT")
                else "system.yaml"
                if yaml_source.get("storage", {}).get("output_root")
                else "default"
            ),
        },
        {
            "key": "storage.pipeline_root",
            "value": str(effective.storage.pipeline_root),
            "source": (
                "PATH_PIPELINES"
                if os.getenv("PATH_PIPELINES")
                else "system.yaml"
                if yaml_source.get("storage", {}).get("pipeline_root")
                else "default"
            ),
        },
        {
            "key": "catalogs.runners",
            "value": str(effective.catalogs.runners),
            "source": "system.yaml" if yaml_source.get("catalogs", {}).get("runners") else "default",
        },
        {
            "key": "catalogs.pipelines",
            "value": str(effective.catalogs.pipelines),
            "source": "system.yaml" if yaml_source.get("catalogs", {}).get("pipelines") else "default",
        },
        {
            "key": "database.host",
            "value": effective.database.host,
            "source": "PG_DB_HOST" if env_source.get("database", {}).get("host") else "system.yaml" if yaml_source.get("database", {}).get("host") else "default",
        },
        {
            "key": "database.port",
            "value": effective.database.port,
            "source": "PG_DB_PORT" if env_source.get("database", {}).get("port") is not None else "system.yaml" if yaml_source.get("database", {}).get("port") is not None else "default",
        },
        {
            "key": "database.name",
            "value": effective.database.name,
            "source": "PG_DB_NAME" if env_source.get("database", {}).get("name") else "system.yaml" if yaml_source.get("database", {}).get("name") else "default",
        },
        {
            "key": "database.user",
            "value": effective.database.user,
            "source": "PG_DB_USER" if env_source.get("database", {}).get("user") else "system.yaml" if yaml_source.get("database", {}).get("user") else "default",
        },
        {
            "key": "orchestrator.polling.startup_seconds",
            "value": effective.orchestrator.polling.startup_seconds,
            "source": "POLL_STARTUP_SECONDS" if env_source.get("orchestrator", {}).get("polling", {}).get("startup_seconds") is not None else "system.yaml" if yaml_source.get("orchestrator", {}).get("polling", {}).get("startup_seconds") is not None else "default",
        },
        {
            "key": "orchestrator.polling.post_submit_seconds",
            "value": effective.orchestrator.polling.post_submit_seconds,
            "source": "POLL_POST_SUBMIT_SECONDS" if env_source.get("orchestrator", {}).get("polling", {}).get("post_submit_seconds") is not None else "system.yaml" if yaml_source.get("orchestrator", {}).get("polling", {}).get("post_submit_seconds") is not None else "default",
        },
        {
            "key": "orchestrator.polling.running_seconds",
            "value": effective.orchestrator.polling.running_seconds,
            "source": "POLL_RUNNING_SECONDS" if env_source.get("orchestrator", {}).get("polling", {}).get("running_seconds") is not None else "system.yaml" if yaml_source.get("orchestrator", {}).get("polling", {}).get("running_seconds") is not None else "default",
        },
        {
            "key": "orchestrator.scheduling.max_attempts",
            "value": effective.orchestrator.scheduling.get("max_attempts", 1),
            "source": (
                "ORCH_SCHEDULING_MAX_ATTEMPTS"
                if os.getenv("ORCH_SCHEDULING_MAX_ATTEMPTS") not in {None, ""}
                else "system.yaml"
                if yaml_source.get("orchestrator", {}).get("scheduling", {}).get("max_attempts") is not None
                else "default"
            ),
        },
        {
            "key": "orchestrator.scheduling.job_timeout_minutes",
            "value": effective.orchestrator.scheduling.get("job_timeout_minutes", 60),
            "source": "system.yaml" if yaml_source.get("orchestrator", {}).get("scheduling", {}).get("job_timeout_minutes") is not None else "default",
        },
        {
            "key": "orchestrator.scheduling.startup_timeout_minutes",
            "value": effective.orchestrator.scheduling.get("startup_timeout_minutes", 1.0),
            "source": "system.yaml" if yaml_source.get("orchestrator", {}).get("scheduling", {}).get("startup_timeout_minutes") is not None else "default",
        },
    ]
    return rows

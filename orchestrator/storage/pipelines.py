from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from app.config import OrchestratorConfig, RunnerDefinition
from storage.db import (
    Jsonb,
    connect_database,
    dict_row,
    generated_identifier,
    insert_resolved_job_row,
    output_sample_payload,
    refresh_batch_record,
    utc_now_timestamp,
)


def _id(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    return f"{prefix}_{timestamp}_{uuid4().hex[:8]}"


def _json(value: Any) -> Jsonb:
    return Jsonb(value if value is not None else {})


def create_pipeline_run(
    config: OrchestratorConfig,
    *,
    pipeline_name: str,
    dataset_target: str,
    config_path: str,
    config_payload: dict[str, Any],
    lanes: list[dict[str, Any]],
    allow_start_outside_window: bool,
) -> dict[str, Any]:
    run_id = _id("pipeline")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                  pipeline_run_id, pipeline_name, status, dataset_target,
                  config_path, config_json, lanes_json,
                  allow_start_outside_window, created_at, updated_at
                )
                VALUES (%s, %s, 'pending', %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    pipeline_name,
                    dataset_target,
                    config_path,
                    _json(config_payload),
                    _json(lanes),
                    allow_start_outside_window,
                    now,
                    now,
                ),
            )
    return {
        "pipeline_run_id": run_id,
        "pipeline_name": pipeline_name,
        "status": "pending",
        "dataset": dataset_target,
        "matrix_lane_count": len(lanes),
        "created_at": now,
    }


def fetch_pipeline_runs(
    config: OrchestratorConfig,
    *,
    active_only: bool = False,
) -> list[dict[str, Any]]:
    where = "WHERE status = 'pending'" if active_only else ""
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  pipeline_run_id, pipeline_name, status, dataset_target,
                  config_path, config_json, lanes_json,
                  allow_start_outside_window,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc,
                  completed_at AT TIME ZONE 'UTC' AS completed_at_utc,
                  failure_message
                FROM pipeline_runs
                {where}
                ORDER BY created_at DESC
                """
            )
            return list(cur.fetchall())


def fetch_pipeline_run(
    config: OrchestratorConfig,
    pipeline_run_id: str,
) -> dict[str, Any] | None:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  pipeline_run_id, pipeline_name, status, dataset_target,
                  config_path, config_json, lanes_json,
                  allow_start_outside_window,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc,
                  completed_at AT TIME ZONE 'UTC' AS completed_at_utc,
                  failure_message
                FROM pipeline_runs
                WHERE pipeline_run_id = %s
                """,
                (pipeline_run_id,),
            )
            return cur.fetchone()


def fetch_pipeline_stage_executions(
    config: OrchestratorConfig,
    pipeline_run_id: str,
) -> list[dict[str, Any]]:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  stage.pipeline_stage_execution_id, stage.pipeline_run_id,
                  stage.stage_id, stage.lane_index, stage.lane_json,
                  stage.dataset_name, stage.dataset_version,
                  stage.external_key, stage.sample_id, stage.job_id,
                  COALESCE(job.status, stage.status) AS status,
                  COALESCE(job.result_json, stage.result_json) AS result_json,
                  job.output_dir,
                  stage.created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  COALESCE(job.updated_at, stage.updated_at)
                    AT TIME ZONE 'UTC' AS updated_at_utc,
                  COALESCE(job.completed_at, stage.completed_at)
                    AT TIME ZONE 'UTC' AS completed_at_utc,
                  COALESCE(job.failure_message, stage.failure_message)
                    AS failure_message
                FROM pipeline_stage_executions AS stage
                LEFT JOIN jobs AS job ON job.job_id = stage.job_id
                WHERE stage.pipeline_run_id = %s
                ORDER BY stage.lane_index, stage.created_at,
                         stage.stage_id, stage.external_key
                """,
                (pipeline_run_id,),
            )
            rows = list(cur.fetchall())
    for row in rows:
        result = dict(row.get("result_json") or {})
        output_dir = str(row.get("output_dir") or "")
        if output_dir:
            outputs, _ = output_sample_payload(
                output_dir,
                result.get("output_files"),
            )
            if outputs:
                result["output_files"] = outputs
        row["result_json"] = result
        row.pop("output_dir", None)
    return rows


def fetch_pipeline_job_outputs(
    config: OrchestratorConfig,
    pipeline_run_id: str,
) -> list[dict[str, Any]]:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  stage.stage_id,
                  stage.lane_index,
                  job.job_id,
                  job.output_dir,
                  job.result_json,
                  job.artifacts_json
                FROM pipeline_stage_executions AS stage
                JOIN jobs AS job ON job.job_id = stage.job_id
                WHERE stage.pipeline_run_id = %s
                ORDER BY stage.created_at
                """,
                (pipeline_run_id,),
            )
            return list(cur.fetchall())


def mark_pipeline_job_outputs_removed(
    config: OrchestratorConfig,
    job_ids: list[str],
) -> None:
    if not job_ids:
        return
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM output_samples WHERE source_job_id = ANY(%s)",
                (job_ids,),
            )
            cur.execute(
                """
                UPDATE jobs
                SET result_json = (result_json - 'output_files' - 'artifacts')
                    || '{"outputs_removed": true}'::jsonb,
                    artifacts_json = '[]'::jsonb,
                    artifact_count = 0
                WHERE job_id = ANY(%s)
                """,
                (job_ids,),
            )


def insert_pipeline_stage_execution(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
    stage_id: str,
    lane_index: int,
    lane: dict[str, Any],
    identity: dict[str, Any] | None,
    status: str,
) -> dict[str, Any] | None:
    stage_execution_id = _id("stage")
    identity = identity or {}
    external_key = str(identity.get("external_key") or "__empty__")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO pipeline_stage_executions (
                  pipeline_stage_execution_id, pipeline_run_id, stage_id, lane_index,
                  lane_json, dataset_name,
                  dataset_version, external_key, sample_id, status,
                  created_at, updated_at
                )
                VALUES (
                  %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (pipeline_run_id, stage_id, lane_index, external_key)
                DO NOTHING
                RETURNING *
                """,
                (
                    stage_execution_id,
                    pipeline_run_id,
                    stage_id,
                    lane_index,
                    _json(lane),
                    identity.get("dataset_name"),
                    identity.get("dataset_version"),
                    external_key,
                    identity.get("sample_id"),
                    status,
                    now,
                    now,
                ),
            )
            return cur.fetchone()


def claim_pipeline_script_stage_execution(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
    stage_id: str,
    lane_index: int,
    lane: dict[str, Any],
    dataset_name: str,
    dataset_version: str,
) -> dict[str, Any] | None:
    """Claim one non-job script stage, including a retry after restart."""
    stage_execution_id = _id("stage")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO pipeline_stage_executions (
                  pipeline_stage_execution_id, pipeline_run_id, stage_id, lane_index,
                  lane_json, dataset_name, dataset_version,
                  external_key, sample_id, status,
                  created_at, updated_at
                )
                VALUES (
                  %s, %s, %s, %s, %s, %s, %s, '__script__', '__script__',
                  'running', %s, %s
                )
                ON CONFLICT (
                  pipeline_run_id, stage_id, lane_index, external_key
                )
                DO NOTHING
                RETURNING *
                """,
                (
                    stage_execution_id,
                    pipeline_run_id,
                    stage_id,
                    lane_index,
                    _json(lane),
                    dataset_name,
                    dataset_version,
                    now,
                    now,
                ),
            )
            claimed = cur.fetchone()
            if claimed is not None:
                return claimed
            cur.execute(
                """
                UPDATE pipeline_stage_executions
                SET status = 'running', result_json = '{}'::jsonb,
                    updated_at = %s, completed_at = NULL,
                    failure_message = NULL
                WHERE pipeline_run_id = %s
                  AND stage_id = %s
                  AND lane_index = %s
                  AND external_key = '__script__'
                  AND job_id IS NULL
                  AND status = 'pending'
                RETURNING *
                """,
                (now, pipeline_run_id, stage_id, lane_index),
            )
            return cur.fetchone()


def finish_pipeline_script_stage_execution(
    config: OrchestratorConfig,
    *,
    pipeline_stage_execution_id: str,
    status: str,
    result: dict[str, Any],
    failure_message: str | None = None,
) -> bool:
    if status not in {"completed", "failed"}:
        raise ValueError("script stage status must be completed or failed")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_stage_executions
                SET status = %s, result_json = %s,
                    updated_at = %s, completed_at = %s,
                    failure_message = %s
                WHERE pipeline_stage_execution_id = %s AND status = 'running'
                """,
                (
                    status,
                    _json(result),
                    now,
                    now,
                    failure_message,
                    pipeline_stage_execution_id,
                ),
            )
            return cur.rowcount == 1


def insert_pipeline_stage_job(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
    stage_id: str,
    lane_index: int,
    lane: dict[str, Any],
    runner: RunnerDefinition,
    identity: dict[str, Any],
    inputs: dict[str, Any],
    parameters: dict[str, Any],
    timeout_seconds: int,
    allow_start_outside_window: bool,
    job_type: str,
    source_job_id: str | None,
) -> str | None:
    """Create the pipeline stage record and ordinary job in one transaction."""
    stage_execution_id = _id("stage")
    job_id = generated_identifier("job")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                INSERT INTO pipeline_stage_executions (
                  pipeline_stage_execution_id, pipeline_run_id, stage_id, lane_index,
                  lane_json, dataset_name, dataset_version, external_key,
                  sample_id, created_at, updated_at
                )
                VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s
                )
                ON CONFLICT (
                  pipeline_run_id, stage_id, lane_index, external_key
                )
                DO NOTHING
                RETURNING pipeline_stage_execution_id
                """,
                (
                    stage_execution_id,
                    pipeline_run_id,
                    stage_id,
                    lane_index,
                    _json(lane),
                    identity.get("dataset_name"),
                    identity.get("dataset_version"),
                    str(identity.get("external_key") or "__empty__"),
                    identity.get("sample_id"),
                    now,
                    now,
                ),
            )
            if cur.fetchone() is None:
                return None
            insert_resolved_job_row(
                cur,
                job_id=job_id,
                runner=runner,
                identity=identity,
                inputs=inputs,
                parameters=parameters,
                timeout_seconds=timeout_seconds,
                job_type=job_type,
                source_job_id=source_job_id,
                allow_start_outside_window=allow_start_outside_window,
                pipeline_run_id=pipeline_run_id,
                pipeline_stage_execution_id=stage_execution_id,
                now=now,
            )
    return job_id


def mark_pipeline_run_terminal(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
    status: str,
    failure_message: str | None = None,
) -> None:
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = %s, failure_message = %s,
                    updated_at = %s, completed_at = %s
                WHERE pipeline_run_id = %s AND status = 'pending'
                """,
                (status, failure_message, now, now, pipeline_run_id),
            )


def _stop_pipeline_run(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
    status: str,
    run_message: str,
    job_code: str,
    job_message: str,
) -> dict[str, Any]:
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT DISTINCT batch_id
                FROM jobs
                WHERE pipeline_run_id = %s AND batch_id IS NOT NULL
                """,
                (pipeline_run_id,),
            )
            batch_ids = [
                str(row["batch_id"])
                for row in cur.fetchall()
                if row.get("batch_id")
            ]
            cur.execute(
                """
                UPDATE jobs
                SET status = 'cancelled', updated_at = %s, completed_at = %s,
                    failure_code = %s, failure_message = %s
                WHERE pipeline_run_id = %s
                  AND status IN ('pending', 'running')
                """,
                (now, now, job_code, job_message, pipeline_run_id),
            )
            cancelled_jobs = cur.rowcount
            cur.execute(
                """
                UPDATE pipeline_stage_executions
                SET status = 'cancelled', updated_at = %s, completed_at = %s,
                    failure_message = %s
                WHERE pipeline_run_id = %s
                  AND job_id IS NULL
                  AND status IN ('pending', 'running')
                """,
                (now, now, job_message, pipeline_run_id),
            )
            cancelled_stage_executions = cur.rowcount
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = %s, updated_at = %s, completed_at = %s,
                    failure_message = %s
                WHERE pipeline_run_id = %s AND status = 'pending'
                """,
                (status, now, now, run_message, pipeline_run_id),
            )
            if cur.rowcount == 0:
                raise ValueError(f"active pipeline run {pipeline_run_id!r} was not found")
            for batch_id in batch_ids:
                refresh_batch_record(cur, batch_id, now=now)
    return {
        "pipeline_run_id": pipeline_run_id,
        "status": status,
        "cancelled_jobs": cancelled_jobs,
        "cancelled_stage_executions": cancelled_stage_executions,
    }


def cancel_pipeline_run(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
) -> dict[str, Any]:
    return _stop_pipeline_run(
        config,
        pipeline_run_id=pipeline_run_id,
        status="cancelled",
        run_message="cancelled by user",
        job_code="PIPELINE_CANCELLED",
        job_message="pipeline run was cancelled",
    )


def fail_pipeline_run(
    config: OrchestratorConfig,
    *,
    pipeline_run_id: str,
    failure_message: str,
) -> dict[str, Any]:
    return _stop_pipeline_run(
        config,
        pipeline_run_id=pipeline_run_id,
        status="failed",
        run_message=failure_message,
        job_code="PIPELINE_FAILED",
        job_message="pipeline stopped after a pipeline error",
    )

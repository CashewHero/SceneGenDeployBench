from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from app.config import OrchestratorConfig, RunnerDefinition
from domain.datasets import SampleRecord, discover_samples
from execution.runner_client import get_status
from domain.scheduling import evaluate_window_state
from domain.targets import DatasetTarget

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ModuleNotFoundError:
    psycopg = None
    dict_row = None
    Jsonb = None


PENDING_CANDIDATE_LIMIT = 1000


class DatabaseUnavailableError(RuntimeError):
    pass


def _runner_input_datatypes(
    runner: RunnerDefinition,
    role: str,
    sample_requirement: str,
    datatype_requirement: str,
) -> list[str]:
    return runner.inputs[role][sample_requirement][datatype_requirement]


def _select_contract_data(
    data: dict[str, Any],
    *,
    required_datatypes: list[str],
    optional_datatypes: list[str],
    field_name: str,
) -> dict[str, Any]:
    missing = [
        data_type for data_type in required_datatypes if data_type not in data
    ]
    if missing:
        raise ValueError(
            f"{field_name} is missing required data types: {', '.join(missing)}"
        )
    declared = list(dict.fromkeys(required_datatypes + optional_datatypes))
    return {
        data_type: data[data_type]
        for data_type in declared
        if data_type in data
    }


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS samples (
  id BIGSERIAL PRIMARY KEY,
  dataset_name TEXT NOT NULL,
  dataset_version TEXT NOT NULL,
  external_key TEXT NOT NULL,
  sample_id TEXT,
  subset_key TEXT NOT NULL DEFAULT '',
  inputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  data_types_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  dataset_data_types_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  missing BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  missing_at TIMESTAMPTZ,
  UNIQUE (dataset_name, dataset_version, external_key)
);

CREATE INDEX IF NOT EXISTS idx_samples_dataset_missing
  ON samples (dataset_name, missing);

CREATE TABLE IF NOT EXISTS runners (
  selector TEXT PRIMARY KEY,
  runner_name TEXT NOT NULL,
  runner_type TEXT NOT NULL,
  version TEXT NOT NULL,
  latest BOOLEAN NOT NULL DEFAULT FALSE,
  contract_version INTEGER NOT NULL,
  inputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  launcher_driver TEXT,
  container_image TEXT,
  config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  missing BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  missing_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_runners_missing
  ON runners (missing, runner_name, version);

CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  runner_selector TEXT NOT NULL,
  runner_name TEXT NOT NULL,
  runner_version TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  dataset_version TEXT NOT NULL,
  external_key TEXT NOT NULL,
  subset_key TEXT NOT NULL DEFAULT '',
  sample_id TEXT,
  job_type TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 1,
  source_job_id TEXT,
  config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  sample_metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  artifacts_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifact_count INTEGER NOT NULL DEFAULT 0,
  metric_count INTEGER NOT NULL DEFAULT 0,
  allow_start_outside_window BOOLEAN NOT NULL DEFAULT FALSE,
  batch_id TEXT,
  output_dir TEXT,
  pipeline_run_id TEXT,
  pipeline_stage_execution_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  failure_code TEXT,
  failure_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_created
  ON jobs (status, created_at);

CREATE INDEX IF NOT EXISTS idx_jobs_dataset
  ON jobs (dataset_name, external_key);

CREATE INDEX IF NOT EXISTS idx_jobs_runner
  ON jobs (runner_selector, status);

CREATE INDEX IF NOT EXISTS idx_jobs_updated
  ON jobs (updated_at);

CREATE INDEX IF NOT EXISTS idx_jobs_pipeline_stage_execution
  ON jobs (pipeline_stage_execution_id);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  runner_selector TEXT NOT NULL,
  runner_name TEXT NOT NULL,
  runner_type TEXT NOT NULL,
  runner_version TEXT NOT NULL,
  runner_endpoint TEXT,
  job_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  job_count INTEGER NOT NULL DEFAULT 0,
  pending_job_count INTEGER NOT NULL DEFAULT 0,
  completed_job_count INTEGER NOT NULL DEFAULT 0,
  failed_job_count INTEGER NOT NULL DEFAULT 0,
  cancelled_job_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  closed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_batches_runner_updated
  ON batches (runner_selector, updated_at);

CREATE TABLE IF NOT EXISTS job_metrics (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
  metric_namespace TEXT NOT NULL,
  metric_name TEXT NOT NULL,
  metric_type TEXT NOT NULL,
  numeric_value DOUBLE PRECISION,
  text_value TEXT,
  unit TEXT,
  source TEXT,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_job_metrics_job
  ON job_metrics (job_id);

CREATE TABLE IF NOT EXISTS output_samples (
  source_job_id TEXT PRIMARY KEY REFERENCES jobs(job_id) ON DELETE CASCADE,
  source_runner_selector TEXT NOT NULL,
  source_runner_name TEXT NOT NULL,
  source_runner_version TEXT NOT NULL,
  dataset_name TEXT NOT NULL,
  dataset_version TEXT NOT NULL,
  external_key TEXT NOT NULL,
  subset_key TEXT NOT NULL DEFAULT '',
  sample_id TEXT,
  outputs_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  data_types_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_output_samples_selector_dataset
  ON output_samples (source_runner_selector, dataset_name, external_key);

CREATE TABLE IF NOT EXISTS pipeline_runs (
  pipeline_run_id TEXT PRIMARY KEY,
  pipeline_name TEXT NOT NULL,
  status TEXT NOT NULL,
  dataset_target TEXT NOT NULL,
  config_path TEXT NOT NULL,
  config_json JSONB NOT NULL,
  lanes_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  allow_start_outside_window BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMPTZ,
  failure_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status_created
  ON pipeline_runs (status, created_at);

UPDATE pipeline_runs
SET status = 'pending'
WHERE status = 'running';

CREATE TABLE IF NOT EXISTS pipeline_stage_executions (
  pipeline_stage_execution_id TEXT
    CONSTRAINT pipeline_stage_executions_pkey PRIMARY KEY,
  pipeline_run_id TEXT NOT NULL
    CONSTRAINT pipeline_stage_executions_run_fkey
    REFERENCES pipeline_runs(pipeline_run_id) ON DELETE CASCADE,
  stage_id TEXT NOT NULL,
  lane_index INTEGER NOT NULL,
  lane_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  dataset_name TEXT,
  dataset_version TEXT,
  external_key TEXT,
  sample_id TEXT,
  status TEXT,
  job_id TEXT,
  result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMPTZ,
  failure_message TEXT,
  CONSTRAINT pipeline_stage_executions_identity_key
    UNIQUE (pipeline_run_id, stage_id, lane_index, external_key)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_stage_executions_pipeline
  ON pipeline_stage_executions (pipeline_run_id, stage_id, lane_index, status);

-- Script stages run synchronously in the orchestrator. If it restarted while
-- one was running, make that stage claimable again after orphan cleanup.
UPDATE pipeline_stage_executions
SET status = 'pending',
    updated_at = CURRENT_TIMESTAMP,
    completed_at = NULL,
    failure_message = NULL
WHERE status = 'running' AND job_id IS NULL;
"""


def _require_psycopg() -> None:
    if psycopg is None or dict_row is None or Jsonb is None:
        raise RuntimeError("psycopg is required to use the PostgreSQL-backed orchestrator")


def utc_now_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generated_identifier(prefix: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    return f"{prefix}_{timestamp}_{uuid4().hex[:8]}"


def _normalized_max_attempts(scheduling: dict[str, Any] | None) -> int:
    if scheduling is None:
        return 1
    return max(int(scheduling.get("max_attempts") or 1), 1)


def _max_attempts_for_runner(config: OrchestratorConfig, runner_selector: str | None) -> int:
    if runner_selector:
        runner = config.runners.get(runner_selector)
        if runner is not None:
            return _normalized_max_attempts(runner.scheduling)
    return _normalized_max_attempts(config.orchestrator.scheduling)


def _json_object(value: Any) -> Jsonb:
    return Jsonb(value if value is not None else {})


def _json_array(value: Any) -> Jsonb:
    return Jsonb(list(value or []))


def _batch_runner_online(endpoint: str | None, *, batch_id: str, runner_type: str) -> bool:
    if not endpoint:
        return False
    try:
        status = get_status(endpoint)
    except RuntimeError:
        return False
    if str(status.get("batch_id") or "").strip() != batch_id:
        return False
    if str(status.get("runner_type") or "").strip() != runner_type:
        return False
    return str(status.get("state") or "").strip() != "shutting_down"


def _sample_subset_key(sample: SampleRecord) -> str:
    subset_key = sample.metadata.get("subset_key")
    if isinstance(subset_key, str) and subset_key.strip():
        return subset_key.strip()
    if "/" in sample.external_key:
        return sample.external_key.rsplit("/", 1)[0]
    return ""


def _normalized_output_parts(value: Any) -> tuple[str, ...]:
    normalized = str(value or "").strip().replace("\\", "/")
    if not normalized:
        return ()
    parts: list[str] = []
    for part in PurePosixPath(normalized).parts:
        token = str(part).strip()
        if token in {"", ".", "/"}:
            continue
        if token == "..":
            parts.append("__parent__")
            continue
        parts.append(token)
    return tuple(parts)


def _job_output_dir(config: OrchestratorConfig, row: dict[str, Any]) -> Path:
    # This path is runner-owned shared storage. The orchestrator assigns it in
    # the request payload and stores the path string in PostgreSQL, but it does
    # not inspect or mutate the directory contents.
    runner_selector = f"{row['runner_name']}@{row['runner_version']}"
    dataset_name = str(row["dataset_name"]).strip() or "dataset"
    sample_metadata = dict(row.get("sample_metadata_json") or {})
    source_runner_selector = str(sample_metadata.get("source_runner_selector") or "").strip()
    subset_parts = _normalized_output_parts(row.get("subset_key"))
    if not subset_parts:
        external_parts = _normalized_output_parts(row.get("external_key"))
        subset_parts = external_parts[:-1]
    sample_parts = _normalized_output_parts(row.get("external_key"))
    sample_name = sample_parts[-1] if sample_parts else str(row.get("sample_id") or row["job_id"]).strip()
    if not sample_name:
        sample_name = str(row["job_id"])
    output_parts: list[str] = [runner_selector]
    if source_runner_selector:
        output_parts.append(source_runner_selector)
    output_parts.append(dataset_name)
    if str(row.get("job_type") or "") == "dataset_download":
        return config.storage.output_root / Path(*output_parts)
    return config.storage.output_root / Path(*output_parts) / Path(*subset_parts) / sample_name


def _list_dataset_names(config: OrchestratorConfig) -> list[str]:
    dataset_root = config.storage.dataset_root
    if not dataset_root.exists():
        return []
    return sorted(
        path.name
        for path in dataset_root.iterdir()
        if path.is_dir() and not path.name.startswith(".")
    )


def _is_output_dataset_target(target: str) -> bool:
    prefix, _, _ = target.strip().strip("/").partition("/")
    return prefix in {"output", "outputs"}


def _preferred_runner_for_name(config: OrchestratorConfig, runner_name: str) -> RunnerDefinition:
    candidates = config.runners_by_name.get(runner_name, ())
    if not candidates:
        available = ", ".join(sorted(config.runners))
        raise ValueError(f"unknown runner {runner_name!r}; available runners: {available}")
    latest_selector = config.latest_runners.get(runner_name)
    if latest_selector:
        return config.runners[latest_selector]
    return candidates[0]


def _resolve_output_runner(config: OrchestratorConfig, runner_reference: str) -> RunnerDefinition:
    normalized = runner_reference.strip()
    if not normalized:
        raise ValueError("output dataset target requires a source runner")
    if normalized in config.runners:
        return config.runners[normalized]
    if "@" in normalized:
        runner_name, version = normalized.split("@", 1)
        if runner_name and version.lower() == "latest":
            return _preferred_runner_for_name(config, runner_name)
    if normalized in config.runners_by_name:
        return _preferred_runner_for_name(config, normalized)
    available = ", ".join(sorted(config.runners))
    raise ValueError(f"unknown output runner {runner_reference!r}; available runners: {available}")


def _split_output_dataset_target(
    config: OrchestratorConfig,
    target: str,
) -> tuple[RunnerDefinition, str | None, str]:
    normalized = target.strip().strip("/")
    prefix, _, remainder = normalized.partition("/")
    if prefix not in {"output", "outputs"}:
        raise ValueError(f"not an output dataset target: {target!r}")
    runner_reference, _, dataset_target = remainder.partition("/")
    runner = _resolve_output_runner(config, runner_reference)
    if not dataset_target:
        return runner, None, ""
    parsed_dataset_target = DatasetTarget.parse(dataset_target)
    return runner, parsed_dataset_target.dataset_name, parsed_dataset_target.subset_prefix


def _sample_key(row: dict[str, Any]) -> str:
    return str(row.get("sample_id") or row.get("external_key") or "").strip()


def _output_absolute_path(output_dir: str, raw_value: Any) -> str | None:
    raw_path = str(raw_value or "").strip()
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return str(path)
    return str(Path(output_dir) / path)


def output_sample_payload(
    output_dir: str,
    output_files: Any,
) -> tuple[dict[str, dict[str, str]], list[str]]:
    if not isinstance(output_files, dict):
        return {}, []

    outputs: dict[str, dict[str, str]] = {}
    data_types: list[str] = []
    for raw_sample_id, raw_sample_outputs in output_files.items():
        sample_id = str(raw_sample_id).strip()
        if not sample_id or not isinstance(raw_sample_outputs, dict):
            continue
        sample_outputs: dict[str, str] = {}
        for raw_data_type, raw_path in raw_sample_outputs.items():
            data_type = str(raw_data_type).strip()
            if not data_type:
                continue
            absolute_path = _output_absolute_path(output_dir, raw_path)
            if absolute_path is None:
                continue
            sample_outputs[data_type] = absolute_path
            if data_type not in data_types:
                data_types.append(data_type)
        if sample_outputs:
            outputs[sample_id] = sample_outputs
    return outputs, data_types


def _upsert_output_sample(
    cur,
    *,
    row: dict[str, Any],
    output_dir: str,
    output_files: Any,
    now: str,
) -> None:
    outputs, data_types = output_sample_payload(output_dir, output_files)
    if not outputs:
        cur.execute("DELETE FROM output_samples WHERE source_job_id = %s", (row["job_id"],))
        return
    metadata = dict(row.get("sample_metadata_json") or {})
    metadata.update(
        {
            "source_job_id": row["job_id"],
            "source_runner_selector": row["runner_selector"],
            "source_runner_name": row["runner_name"],
            "source_runner_version": row["runner_version"],
            "source_output_dir": output_dir,
        }
    )
    cur.execute(
        """
        INSERT INTO output_samples (
          source_job_id, source_runner_selector, source_runner_name, source_runner_version,
          dataset_name, dataset_version, external_key, subset_key, sample_id,
          outputs_json, data_types_json, metadata_json, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (source_job_id) DO UPDATE SET
          source_runner_selector = EXCLUDED.source_runner_selector,
          source_runner_name = EXCLUDED.source_runner_name,
          source_runner_version = EXCLUDED.source_runner_version,
          dataset_name = EXCLUDED.dataset_name,
          dataset_version = EXCLUDED.dataset_version,
          external_key = EXCLUDED.external_key,
          subset_key = EXCLUDED.subset_key,
          sample_id = EXCLUDED.sample_id,
          outputs_json = EXCLUDED.outputs_json,
          data_types_json = EXCLUDED.data_types_json,
          metadata_json = EXCLUDED.metadata_json,
          updated_at = EXCLUDED.updated_at
        """,
        (
            row["job_id"],
            row["runner_selector"],
            row["runner_name"],
            row["runner_version"],
            row["dataset_name"],
            row["dataset_version"],
            row["external_key"],
            row.get("subset_key") or "",
            row.get("sample_id"),
            _json_object(outputs),
            _json_array(data_types),
            _json_object(metadata),
            now,
            now,
        ),
    )


def _delete_output_sample(cur, job_id: str) -> None:
    cur.execute("DELETE FROM output_samples WHERE source_job_id = %s", (job_id,))


@contextmanager
def connect_database(config: OrchestratorConfig):
    _require_psycopg()
    try:
        conn = psycopg.connect(
            host=config.database.host,
            port=config.database.port,
            dbname=config.database.name,
            user=config.database.user,
            password=config.database.password,
            row_factory=dict_row,
        )
    except psycopg.OperationalError as exc:
        raise DatabaseUnavailableError(
            "Database unavailable at "
            f"{config.database.host}:{config.database.port}/{config.database.name}. "
            "Check that PostgreSQL is running and reachable, then retry."
        ) from exc
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_schema(config: OrchestratorConfig) -> None:
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)


def sync_runner_state(config: OrchestratorConfig) -> dict[str, int]:
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('scenegendeploybench:sync_runner_state'))")
            cur.execute(SCHEMA_SQL)
            runner_count = _sync_runners(cur, config)
    return {"runner_count": runner_count}


def sync_dataset_state(
    config: OrchestratorConfig,
    *,
    dataset_name: str | None = None,
) -> dict[str, int | str | None]:
    normalized_dataset_name = (
        dataset_name.strip().strip("/") if dataset_name is not None else None
    )
    if normalized_dataset_name == "":
        raise ValueError("dataset name cannot be empty")
    if normalized_dataset_name and "/" in normalized_dataset_name:
        raise ValueError("dataset rescan accepts a dataset name, not a subset path")
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext('scenegendeploybench:sync_dataset_state'))")
            cur.execute(SCHEMA_SQL)
            dataset_count, sample_count = _sync_samples(
                cur,
                config,
                dataset_name=normalized_dataset_name,
            )
    return {
        "dataset": normalized_dataset_name,
        "dataset_count": dataset_count,
        "sample_count": sample_count,
    }


def _sync_runners(cur, config: OrchestratorConfig) -> int:
    now = utc_now_timestamp()
    selectors: set[str] = set()
    for runner in config.runners.values():
        selectors.add(runner.selector)
        cur.execute(
            """
            INSERT INTO runners (
              selector, runner_name, runner_type, version, latest, contract_version,
              inputs_json, launcher_driver,
              container_image, config_json, missing, updated_at, missing_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s, NULL)
            ON CONFLICT (selector) DO UPDATE SET
              runner_name = EXCLUDED.runner_name,
              runner_type = EXCLUDED.runner_type,
              version = EXCLUDED.version,
              latest = EXCLUDED.latest,
              contract_version = EXCLUDED.contract_version,
              inputs_json = EXCLUDED.inputs_json,
              launcher_driver = EXCLUDED.launcher_driver,
              container_image = EXCLUDED.container_image,
              config_json = EXCLUDED.config_json,
              missing = FALSE,
              updated_at = EXCLUDED.updated_at,
              missing_at = NULL
            """,
            (
                runner.selector,
                runner.runner,
                runner.kind,
                runner.version,
                runner.latest,
                runner.contract_version,
                _json_object(runner.inputs),
                runner.launcher.get("driver"),
                runner.launcher.get("image"),
                _json_object(runner.raw),
                now,
            ),
        )

    cur.execute("SELECT selector FROM runners")
    existing = {row["selector"] for row in cur.fetchall()}
    stale = existing - selectors
    if stale:
        cur.executemany(
            "UPDATE runners SET missing = TRUE, missing_at = %s, updated_at = %s WHERE selector = %s",
            [(now, now, selector) for selector in stale],
        )
    return len(selectors)


def _sync_samples(
    cur,
    config: OrchestratorConfig,
    *,
    dataset_name: str | None = None,
) -> tuple[int, int]:
    now = utc_now_timestamp()
    sample_keys: set[tuple[str, str, str]] = set()
    available_dataset_names = _list_dataset_names(config)
    if dataset_name is None:
        dataset_names = available_dataset_names
    elif dataset_name in available_dataset_names:
        dataset_names = [dataset_name]
    else:
        dataset_names = []
    for current_dataset_name in dataset_names:
        for sample in discover_samples(config, dataset_name=current_dataset_name):
            key = (sample.dataset_name, sample.dataset_version, sample.external_key)
            sample_keys.add(key)
            cur.execute(
                """
                INSERT INTO samples (
                  dataset_name, dataset_version, external_key, sample_id, subset_key,
                  inputs_json, data_types_json, dataset_data_types_json, metadata_json,
                  missing, updated_at, missing_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, FALSE, %s, NULL)
                ON CONFLICT (dataset_name, dataset_version, external_key) DO UPDATE SET
                  sample_id = EXCLUDED.sample_id,
                  subset_key = EXCLUDED.subset_key,
                  inputs_json = EXCLUDED.inputs_json,
                  data_types_json = EXCLUDED.data_types_json,
                  dataset_data_types_json = EXCLUDED.dataset_data_types_json,
                  metadata_json = EXCLUDED.metadata_json,
                  missing = FALSE,
                  updated_at = EXCLUDED.updated_at,
                  missing_at = NULL
                """,
                (
                    sample.dataset_name,
                    sample.dataset_version,
                    sample.external_key,
                    sample.sample_id,
                    _sample_subset_key(sample),
                    _json_object(sample.data),
                    _json_array(sample.data_types),
                    _json_array(sample.dataset_data_types),
                    _json_object(sample.metadata),
                    now,
                ),
            )

    if dataset_name is None:
        cur.execute("SELECT dataset_name, dataset_version, external_key FROM samples")
    else:
        cur.execute(
            """
            SELECT dataset_name, dataset_version, external_key
            FROM samples
            WHERE dataset_name = %s
            """,
            (dataset_name,),
        )
    existing = {(row["dataset_name"], row["dataset_version"], row["external_key"]) for row in cur.fetchall()}
    stale = existing - sample_keys
    if stale:
        cur.executemany(
            """
            UPDATE samples
            SET missing = TRUE, missing_at = %s, updated_at = %s
            WHERE dataset_name = %s AND dataset_version = %s AND external_key = %s
            """,
            [(now, now, dataset_name, dataset_version, external_key) for dataset_name, dataset_version, external_key in stale],
        )
    return len(dataset_names), len(sample_keys)


def _job_where_clauses(
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    batch_id: str | None = None,
    states: list[str] | None = None,
    failed: bool = False,
    active: bool = False,
    completed: bool = False,
    created_since: str | None = None,
    created_until: str | None = None,
    updated_since: str | None = None,
    updated_until: str | None = None,
    finished_since: str | None = None,
    finished_until: str | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if job_ids:
        clauses.append("job_id = ANY(%s)")
        params.append(job_ids)
    if dataset:
        dataset_sql, dataset_params = DatasetTarget.parse(dataset).sql_filter()
        clauses.append(dataset_sql)
        params.extend(dataset_params)
    if runner:
        clauses.append("runner_name = %s")
        params.append(runner)
    if runner_selector:
        clauses.append("runner_selector = %s")
        params.append(runner_selector)
    if batch_id:
        clauses.append("batch_id = %s")
        params.append(batch_id)
    if states:
        clauses.append("status = ANY(%s)")
        params.append(states)
    if failed:
        clauses.append("status = ANY(%s)")
        params.append(["failed"])
    if active:
        clauses.append("status = ANY(%s)")
        params.append(["pending"])
    if completed:
        clauses.append("status = ANY(%s)")
        params.append(["completed"])
    if created_since:
        clauses.append("created_at >= %s::timestamptz")
        params.append(created_since)
    if created_until:
        clauses.append("created_at <= %s::timestamptz")
        params.append(created_until)
    if updated_since:
        clauses.append("updated_at >= %s::timestamptz")
        params.append(updated_since)
    if updated_until:
        clauses.append("updated_at <= %s::timestamptz")
        params.append(updated_until)
    if finished_since:
        clauses.append("completed_at >= %s::timestamptz")
        params.append(finished_since)
    if finished_until:
        clauses.append("completed_at <= %s::timestamptz")
        params.append(finished_until)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _job_order_sql(sort: str, desc: bool, *, grouped: bool = False) -> str:
    direction = "DESC" if desc else "ASC"
    if grouped:
        order_field = {
            "updated_at": "last_update_utc",
            "created_at": "last_created_utc",
            "completed_at": "last_completed_utc",
            "dataset": "dataset_name",
            "runner": "runner_selector",
        }.get(sort, "last_update_utc")
        if order_field == "dataset_name":
            return (
                f"ORDER BY dataset_name {direction}, "
                f"subset_key {direction}, runner_selector {direction}"
            )
        if order_field == "runner_selector":
            return (
                f"ORDER BY runner_selector {direction}, "
                f"dataset_name {direction}, subset_key {direction}"
            )
        return (
            f"ORDER BY {order_field} {direction} NULLS LAST, "
            f"dataset_name ASC, subset_key ASC, runner_selector ASC"
        )
    order_field = {
        "updated_at": "updated_at",
        "created_at": "created_at",
        "completed_at": "completed_at",
        "dataset": "dataset_name",
        "runner": "runner_selector",
    }.get(sort, "updated_at")
    if order_field in {"dataset_name", "runner_selector"}:
        return (
            f"ORDER BY {order_field} {direction}, "
            f"external_key {direction}, job_id {direction}"
        )
    return (
        f"ORDER BY {order_field} {direction} NULLS LAST, "
        f"created_at DESC, job_id DESC"
    )


def fetch_job_rows(
    config: OrchestratorConfig,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    batch_id: str | None = None,
    states: list[str] | None = None,
    failed: bool = False,
    active: bool = False,
    completed: bool = False,
    created_since: str | None = None,
    created_until: str | None = None,
    updated_since: str | None = None,
    updated_until: str | None = None,
    finished_since: str | None = None,
    finished_until: str | None = None,
    sort: str = "updated_at",
    desc: bool = True,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where_sql, params = _job_where_clauses(
        job_ids=job_ids,
        dataset=dataset,
        runner=runner,
        runner_selector=runner_selector,
        batch_id=batch_id,
        states=states,
        failed=failed,
        active=active,
        completed=completed,
        created_since=created_since,
        created_until=created_until,
        updated_since=updated_since,
        updated_until=updated_until,
        finished_since=finished_since,
        finished_until=finished_until,
    )
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        params = [*params, max(int(limit), 0)]
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  job_id,
                  runner_selector,
                  runner_name,
                  runner_version,
                  dataset_name,
                  dataset_version,
                  external_key,
                  subset_key,
                  sample_id,
                  job_type,
                  status,
                  attempt_count,
                  source_job_id,
                  config_json,
                  request_json,
                  result_json,
                  sample_metadata_json,
                  artifacts_json,
                  artifact_count,
                  metric_count,
                  allow_start_outside_window,
                  batch_id,
                  output_dir,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc,
                  completed_at AT TIME ZONE 'UTC' AS completed_at_utc,
                  failure_code,
                  failure_message
                FROM jobs
                {where_sql}
                {_job_order_sql(sort, desc)}
                {limit_sql}
                """,
                params,
            )
            return list(cur.fetchall())


def fetch_job_summary(
    config: OrchestratorConfig,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    batch_id: str | None = None,
    states: list[str] | None = None,
    failed: bool = False,
    active: bool = False,
    completed: bool = False,
    created_since: str | None = None,
    created_until: str | None = None,
    updated_since: str | None = None,
    updated_until: str | None = None,
    finished_since: str | None = None,
    finished_until: str | None = None,
) -> dict[str, int]:
    where_sql, params = _job_where_clauses(
        job_ids=job_ids,
        dataset=dataset,
        runner=runner,
        runner_selector=runner_selector,
        batch_id=batch_id,
        states=states,
        failed=failed,
        active=active,
        completed=completed,
        created_since=created_since,
        created_until=created_until,
        updated_since=updated_since,
        updated_until=updated_until,
        finished_since=finished_since,
        finished_until=finished_until,
    )
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  COUNT(*)::int AS job_count,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled
                FROM jobs
                {where_sql}
                """,
                params,
            )
            row = cur.fetchone() or {}
    return {
        "job_count": int(row.get("job_count") or 0),
        "completed": int(row.get("completed") or 0),
        "pending": int(row.get("pending") or 0),
        "failed": int(row.get("failed") or 0),
        "cancelled": int(row.get("cancelled") or 0),
    }


def fetch_job_group_rows(
    config: OrchestratorConfig,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    batch_id: str | None = None,
    states: list[str] | None = None,
    failed: bool = False,
    active: bool = False,
    completed: bool = False,
    created_since: str | None = None,
    created_until: str | None = None,
    updated_since: str | None = None,
    updated_until: str | None = None,
    finished_since: str | None = None,
    finished_until: str | None = None,
    sort: str = "updated_at",
    desc: bool = True,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    where_sql, params = _job_where_clauses(
        job_ids=job_ids,
        dataset=dataset,
        runner=runner,
        runner_selector=runner_selector,
        batch_id=batch_id,
        states=states,
        failed=failed,
        active=active,
        completed=completed,
        created_since=created_since,
        created_until=created_until,
        updated_since=updated_since,
        updated_until=updated_until,
        finished_since=finished_since,
        finished_until=finished_until,
    )
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        params = [*params, max(int(limit), 0)]
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  dataset_name,
                  subset_key,
                  runner_selector,
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
                  MAX(updated_at) AT TIME ZONE 'UTC' AS last_update_utc,
                  MAX(created_at) AT TIME ZONE 'UTC' AS last_created_utc,
                  MAX(completed_at) AT TIME ZONE 'UTC' AS last_completed_utc
                FROM jobs
                {where_sql}
                GROUP BY dataset_name, subset_key, runner_selector
                {_job_order_sql(sort, desc, grouped=True)}
                {limit_sql}
                """,
                params,
            )
            return list(cur.fetchall())


def fetch_job_group_count(
    config: OrchestratorConfig,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    batch_id: str | None = None,
    states: list[str] | None = None,
    failed: bool = False,
    active: bool = False,
    completed: bool = False,
    created_since: str | None = None,
    created_until: str | None = None,
    updated_since: str | None = None,
    updated_until: str | None = None,
    finished_since: str | None = None,
    finished_until: str | None = None,
) -> int:
    where_sql, params = _job_where_clauses(
        job_ids=job_ids,
        dataset=dataset,
        runner=runner,
        runner_selector=runner_selector,
        batch_id=batch_id,
        states=states,
        failed=failed,
        active=active,
        completed=completed,
        created_since=created_since,
        created_until=created_until,
        updated_since=updated_since,
        updated_until=updated_until,
        finished_since=finished_since,
        finished_until=finished_until,
    )
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT COUNT(*)::int AS group_count
                FROM (
                  SELECT 1
                  FROM jobs
                  {where_sql}
                  GROUP BY dataset_name, subset_key, runner_selector
                ) grouped_jobs
                """,
                params,
            )
            row = cur.fetchone() or {}
    return int(row.get("group_count") or 0)


def fetch_job_status(config: OrchestratorConfig, job_id: str) -> str | None:
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM jobs WHERE job_id = %s", (job_id,))
            row = cur.fetchone()
    if row is None:
        return None
    return str(row["status"] if isinstance(row, dict) else row[0])


def refresh_batch_record(cur, batch_id: str, *, now: str) -> None:
    cur.execute(
        """
        SELECT job_id, status
        FROM jobs
        WHERE batch_id = %s
        ORDER BY created_at
        """,
        (batch_id,),
    )
    raw_rows = list(cur.fetchall())
    rows = [
        row if isinstance(row, dict) else {"job_id": row[0], "status": row[1]}
        for row in raw_rows
    ]
    pending = sum(1 for row in rows if row["status"] == "pending")
    completed = sum(1 for row in rows if row["status"] == "completed")
    failed = sum(1 for row in rows if row["status"] == "failed")
    cancelled = sum(1 for row in rows if row["status"] == "cancelled")
    # Empty batches happen when a failed dispatch releases or reassigns every
    # pending job. Close them instead of leaving stale "open" rows behind.
    closed_at = now if pending == 0 else None
    cur.execute(
        """
        UPDATE batches
        SET job_ids_json = %s,
            job_count = %s,
            pending_job_count = %s,
            completed_job_count = %s,
            failed_job_count = %s,
            cancelled_job_count = %s,
            updated_at = %s,
            closed_at = %s
        WHERE batch_id = %s
        """,
        (
            _json_array([row["job_id"] for row in rows]),
            len(rows),
            pending,
            completed,
            failed,
            cancelled,
            now,
            closed_at,
            batch_id,
        ),
    )


def _upsert_batch_record(
    cur,
    *,
    batch_id: str,
    runner: RunnerDefinition,
    runner_endpoint: str | None,
    now: str,
) -> None:
    cur.execute(
        """
        INSERT INTO batches (
          batch_id, runner_selector, runner_name, runner_type, runner_version,
          runner_endpoint, created_at, updated_at, closed_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
        ON CONFLICT (batch_id) DO UPDATE SET
          runner_selector = EXCLUDED.runner_selector,
          runner_name = EXCLUDED.runner_name,
          runner_type = EXCLUDED.runner_type,
          runner_version = EXCLUDED.runner_version,
          runner_endpoint = COALESCE(EXCLUDED.runner_endpoint, batches.runner_endpoint),
          updated_at = EXCLUDED.updated_at
        """,
        (
            batch_id,
            runner.selector,
            runner.runner,
            runner.kind,
            runner.version,
            runner_endpoint,
            now,
            now,
        ),
    )
    refresh_batch_record(cur, batch_id, now=now)


def fetch_batch_row(config: OrchestratorConfig, batch_id: str) -> dict[str, Any] | None:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  batch_id,
                  runner_selector,
                  runner_name,
                  runner_type,
                  runner_version,
                  runner_endpoint,
                  job_ids_json,
                  job_count,
                  pending_job_count,
                  completed_job_count,
                  failed_job_count,
                  cancelled_job_count,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc,
                  closed_at AT TIME ZONE 'UTC' AS closed_at_utc
                FROM batches
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            return cur.fetchone()


def fetch_batch_rows(config: OrchestratorConfig) -> list[dict[str, Any]]:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  batch_id,
                  runner_selector,
                  runner_name,
                  runner_type,
                  runner_version,
                  runner_endpoint,
                  job_ids_json,
                  job_count,
                  pending_job_count,
                  completed_job_count,
                  failed_job_count,
                  cancelled_job_count,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc,
                  closed_at AT TIME ZONE 'UTC' AS closed_at_utc
                FROM batches
                ORDER BY updated_at DESC, created_at DESC, batch_id DESC
                """
            )
            return list(cur.fetchall())


def update_batch_runner_endpoint(config: OrchestratorConfig, *, batch_id: str, runner_endpoint: str) -> None:
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE batches
                SET runner_endpoint = %s,
                    updated_at = %s
                WHERE batch_id = %s
                """,
                (runner_endpoint, now, batch_id),
            )
            refresh_batch_record(cur, batch_id, now=now)


def _target_sample_rows(
    config: OrchestratorConfig,
    target: str,
    *,
    option_name: str,
) -> tuple[bool, str, list[dict[str, Any]]]:
    normalized = target.strip()
    if not normalized:
        raise ValueError(f"{option_name} cannot be empty")
    is_output = _is_output_dataset_target(normalized)
    if is_output:
        _, dataset_name, _ = _split_output_dataset_target(config, normalized)
        rows = fetch_output_sample_rows(config, dataset=normalized)
    else:
        parsed = DatasetTarget.parse(normalized)
        dataset_name = parsed.dataset_name
        rows = [
            row
            for row in fetch_sample_rows(config, dataset=normalized)
            if parsed.matches(row["dataset_name"], row["external_key"])
        ]
    if not rows:
        if is_output:
            raise ValueError(f"{option_name} {target!r} matched no output samples")
        raise ValueError(
            f"{option_name} {target!r} matched no indexed samples; "
            f"run 'deploybench dataset rescan {dataset_name}'"
        )
    return is_output, dataset_name or "", rows


def _sample_identity(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row["dataset_name"]),
        str(row["dataset_version"]),
        str(row["external_key"]),
    )


def _primary_role_data(
    row: dict[str, Any],
    *,
    from_output: bool,
    role: str,
) -> dict[str, Any]:
    if not from_output:
        return dict(row.get("inputs_json") or {})
    sample_key = _sample_key(row)
    raw_data = dict(row.get("outputs_json") or {}).get(sample_key)
    if not isinstance(raw_data, dict):
        raise ValueError(
            f"{role} output sample {sample_key!r} is missing for "
            f"{row['external_key']!r}"
        )
    return dict(raw_data)


def _reference_candidates(
    config: OrchestratorConfig,
    targets: list[str],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for target in targets:
        from_output, _, rows = _target_sample_rows(
            config,
            target,
            option_name="--reference",
        )
        for row in rows:
            role_samples = (
                dict(row.get("outputs_json") or {})
                if from_output
                else {_sample_key(row): dict(row.get("inputs_json") or {})}
            )
            for sample_key, raw_data in role_samples.items():
                if not isinstance(raw_data, dict):
                    continue
                identity = (
                    "output" if from_output else "data",
                    str(row["dataset_name"]),
                    str(row["dataset_version"]),
                    str(row["external_key"]),
                    str(sample_key),
                )
                if identity in seen:
                    continue
                seen.add(identity)
                candidates.append(
                    {
                        "identity": identity,
                        "dataset_name": str(row["dataset_name"]),
                        "dataset_version": str(row["dataset_version"]),
                        "external_key": str(row["external_key"]),
                        "sample_key": str(sample_key),
                        "data": dict(raw_data),
                    }
                )
    return candidates


def insert_resolved_job_row(
    cur,
    *,
    job_id: str,
    runner: RunnerDefinition,
    identity: dict[str, Any],
    inputs: dict[str, Any],
    parameters: dict[str, Any],
    timeout_seconds: int,
    job_type: str,
    source_job_id: str | None,
    allow_start_outside_window: bool,
    now: str,
    pipeline_run_id: str | None = None,
    pipeline_stage_execution_id: str | None = None,
) -> dict[str, Any]:
    metadata = dict(identity.get("metadata_json") or {})
    primary_sample = str(
        identity.get("primary_sample")
        or identity.get("sample_id")
        or identity.get("external_key")
        or ""
    ).strip()
    job_payload: dict[str, Any] = {
        "job_id": job_id,
        "batch_id": None,
        "job_type": job_type,
        "source_job_id": source_job_id,
        "attempt": 1,
        "timeout_seconds": timeout_seconds,
        "parameters": dict(parameters),
    }
    if primary_sample:
        job_payload["primary_sample"] = primary_sample
    if metadata:
        job_payload["primary_sample_metadata"] = metadata
    request_payload = {
        "contract_version": runner.contract_version,
        "job": job_payload,
        "inputs": inputs,
        "runtime": {},
    }
    cur.execute(
        """
        INSERT INTO jobs (
          job_id, runner_selector, runner_name, runner_version,
          dataset_name, dataset_version, external_key, subset_key,
          sample_id, job_type, status, attempt_count, source_job_id,
          config_json, request_json, result_json, sample_metadata_json,
          artifacts_json, artifact_count, metric_count,
          allow_start_outside_window, batch_id, output_dir,
          pipeline_run_id, pipeline_stage_execution_id,
          created_at, updated_at
        )
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s,
          %s, %s, 'pending', 1, %s, %s, %s, '{}'::jsonb, %s,
          '[]'::jsonb, 0, 0, %s, NULL, NULL, %s, %s, %s, %s
        )
        """,
        (
            job_id,
            runner.selector,
            runner.runner,
            runner.version,
            identity["dataset_name"],
            identity["dataset_version"],
            identity["external_key"],
            identity.get("subset_key") or "",
            identity.get("sample_id"),
            job_type,
            source_job_id,
            _json_object(parameters),
            _json_object(request_payload),
            _json_object(metadata),
            allow_start_outside_window,
            pipeline_run_id,
            pipeline_stage_execution_id,
            now,
            now,
        ),
    )
    if pipeline_stage_execution_id:
        cur.execute(
            """
            UPDATE pipeline_stage_executions
            SET job_id = %s, updated_at = %s
            WHERE pipeline_stage_execution_id = %s
            """,
            (
                job_id,
                now,
                pipeline_stage_execution_id,
            ),
        )
    return request_payload


def insert_jobs(
    config: OrchestratorConfig,
    *,
    dataset: str | None,
    candidate: str | None,
    references: list[str],
    runner: RunnerDefinition,
    job_type: str,
    parameters: dict[str, Any],
    timeout_seconds: int,
    source_job_id: str | None,
    allow_start_outside_window: bool,
) -> dict[str, Any]:
    sync_runner_state(config)
    dataset_target = (dataset or "").strip()
    candidate_target = (candidate or "").strip()
    if not dataset_target and not candidate_target:
        raise ValueError("job add requires --dataset or --candidate")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")

    data_from_output = False
    data_rows: list[dict[str, Any]] = []
    dataset_name = ""
    if dataset_target:
        data_from_output, dataset_name, data_rows = _target_sample_rows(
            config,
            dataset_target,
            option_name="--dataset",
        )

    candidate_from_output = False
    candidate_rows: list[dict[str, Any]] = []
    if candidate_target:
        candidate_from_output, candidate_dataset_name, candidate_rows = _target_sample_rows(
            config,
            candidate_target,
            option_name="--candidate",
        )
        dataset_name = candidate_dataset_name

    data_required = _runner_input_datatypes(
        runner, "data", "required_sample", "required_datatype"
    )
    data_optional = _runner_input_datatypes(
        runner, "data", "required_sample", "optional_datatype"
    )
    candidate_required = _runner_input_datatypes(
        runner, "candidate", "required_sample", "required_datatype"
    )
    candidate_optional = _runner_input_datatypes(
        runner, "candidate", "required_sample", "optional_datatype"
    )
    candidate_extra_required = _runner_input_datatypes(
        runner, "candidate", "optional_sample", "required_datatype"
    )
    candidate_extra_optional = _runner_input_datatypes(
        runner, "candidate", "optional_sample", "optional_datatype"
    )
    candidate_declared = (
        candidate_required
        or candidate_optional
        or candidate_extra_required
        or candidate_extra_optional
    )
    if candidate_target and not candidate_declared:
        raise ValueError(f"runner {runner.selector} does not declare inputs.candidate")
    if candidate_required and not candidate_target:
        raise ValueError(f"runner {runner.selector} requires --candidate")

    reference_required = _runner_input_datatypes(
        runner, "references", "optional_sample", "required_datatype"
    )
    reference_optional = _runner_input_datatypes(
        runner, "references", "optional_sample", "optional_datatype"
    )
    if references and not (reference_required or reference_optional):
        raise ValueError(
            f"runner {runner.selector} does not declare inputs.references.optional_sample"
        )
    reference_candidates = _reference_candidates(config, references)

    primary_rows = candidate_rows if candidate_target else data_rows
    data_rows_by_identity: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in data_rows:
        data_rows_by_identity.setdefault(_sample_identity(row), []).append(row)

    now = utc_now_timestamp()
    created_jobs: list[dict[str, Any]] = []
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            for sample_row in primary_rows:
                data_row = sample_row
                if dataset_target and candidate_target:
                    matching_data_rows = data_rows_by_identity.get(_sample_identity(sample_row), [])
                    if not matching_data_rows:
                        raise ValueError(
                            f"--dataset has no sample matching --candidate "
                            f"{sample_row['external_key']!r}"
                        )
                    if len(matching_data_rows) > 1:
                        raise ValueError(
                            f"--dataset matches multiple outputs for candidate "
                            f"{sample_row['external_key']!r}; select a narrower output target"
                        )
                    data_row = matching_data_rows[0]

                sample_inputs = dict(sample_row["inputs_json"] or {})
                sample_key = _sample_key(sample_row)
                sample_metadata = dict(sample_row["metadata_json"] or {})
                upstream_row = (
                    sample_row
                    if candidate_from_output
                    else data_row if data_from_output else sample_row
                )
                upstream_metadata = dict(upstream_row.get("output_metadata_json") or {})
                metadata_source_job_id = upstream_row.get(
                    "source_job_id"
                ) or upstream_metadata.get("source_job_id")
                sample_source_job_id = source_job_id or (
                    str(metadata_source_job_id) if metadata_source_job_id else None
                )
                data_source = (
                    _primary_role_data(
                        data_row,
                        from_output=data_from_output,
                        role="data",
                    )
                    if dataset_target
                    else sample_inputs
                )
                selected_data = _select_contract_data(
                    data_source,
                    required_datatypes=data_required,
                    optional_datatypes=data_optional,
                    field_name=f"data sample {sample_row['external_key']!r}",
                )

                selected_candidates: dict[str, dict[str, Any]] = {}
                if candidate_required or candidate_optional:
                    selected_candidates[sample_key] = _select_contract_data(
                        _primary_role_data(
                            sample_row,
                            from_output=candidate_from_output,
                            role="candidate",
                        ),
                        required_datatypes=candidate_required,
                        optional_datatypes=candidate_optional,
                        field_name=f"candidate sample {sample_key!r}",
                    )
                if candidate_extra_required or candidate_extra_optional:
                    extra_samples = (
                        dict(sample_row.get("outputs_json") or {})
                        if candidate_from_output
                        else {}
                    )
                    for output_sample_key, raw_output_data in extra_samples.items():
                        output_sample_key = str(output_sample_key)
                        if output_sample_key == sample_key:
                            continue
                        if not isinstance(raw_output_data, dict):
                            raise ValueError(
                                f"candidate sample {output_sample_key!r} must be a mapping"
                            )
                        selected_candidates[output_sample_key] = _select_contract_data(
                            dict(raw_output_data),
                            required_datatypes=candidate_extra_required,
                            optional_datatypes=candidate_extra_optional,
                            field_name=f"optional candidate sample {output_sample_key!r}",
                        )

                selected_references: dict[str, dict[str, Any]] = {}
                primary_identity = (
                    str(sample_row["dataset_name"]),
                    str(sample_row["dataset_version"]),
                    str(sample_row["external_key"]),
                )
                for candidate in reference_candidates:
                    candidate_identity = (
                        candidate["dataset_name"],
                        candidate["dataset_version"],
                        candidate["external_key"],
                    )
                    if candidate_identity == primary_identity:
                        continue
                    reference_key = str(candidate["sample_key"]).strip()
                    if not reference_key:
                        raise ValueError(
                            f"reference sample {candidate['external_key']!r} has no sample key"
                        )
                    if reference_key in selected_references:
                        raise ValueError(
                            f"reference selections contain duplicate sample key {reference_key!r}; "
                            "select a narrower target"
                        )
                    selected_references[reference_key] = _select_contract_data(
                        dict(candidate["data"]),
                        required_datatypes=reference_required,
                        optional_datatypes=reference_optional,
                        field_name=f"reference sample {candidate['external_key']!r}",
                    )
                job_id = generated_identifier("job")
                inputs: dict[str, dict[str, dict[str, Any]]] = {}
                if selected_data and sample_key:
                    inputs["data"] = {sample_key: selected_data}
                if selected_candidates:
                    inputs["candidate"] = selected_candidates
                if selected_references:
                    inputs["references"] = selected_references

                insert_resolved_job_row(
                    cur,
                    job_id=job_id,
                    runner=runner,
                    identity={
                        "dataset_name": sample_row["dataset_name"],
                        "dataset_version": sample_row["dataset_version"],
                        "external_key": sample_row["external_key"],
                        "subset_key": sample_row["subset_key"] or "",
                        "sample_id": sample_row["sample_id"],
                        "primary_sample": sample_key,
                        "metadata_json": sample_metadata,
                    },
                    inputs=inputs,
                    parameters=parameters,
                    timeout_seconds=timeout_seconds,
                    job_type=job_type,
                    source_job_id=sample_source_job_id,
                    allow_start_outside_window=allow_start_outside_window,
                    now=now,
                )
                created_jobs.append(
                    {
                        "job_id": job_id,
                        "job_ref": job_id,
                        "sample": sample_row["external_key"],
                        "state": "pending",
                    }
                )
    return {
        "job_count": len(created_jobs),
        "created_at": now,
        "dataset": dataset_name,
        "dataset_version": primary_rows[0]["dataset_version"],
        "runner": runner.selector,
        "job_type": job_type,
        "timeout_seconds": timeout_seconds,
        "allow_start_outside_window": allow_start_outside_window,
        "parameters": dict(parameters),
        "jobs": created_jobs,
    }


def insert_dataset_download_job(
    config: OrchestratorConfig,
    *,
    dataset_name: str,
    runner: RunnerDefinition,
    parameters: dict[str, Any],
    timeout_seconds: int,
    allow_start_outside_window: bool,
) -> dict[str, Any]:
    sync_runner_state(config)
    normalized_dataset_name = dataset_name.strip().strip("/")
    if not normalized_dataset_name:
        raise ValueError("dataset name is required")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")

    job_id = generated_identifier("job")
    now = utc_now_timestamp()
    job_parameters = {
        **dict(parameters),
        "dataset_name": normalized_dataset_name,
    }
    identity = {
        "dataset_name": normalized_dataset_name,
        "dataset_version": "unversioned",
        "external_key": "",
        "subset_key": "",
        "sample_id": None,
        "metadata_json": {"dataset_download": True},
    }
    with connect_database(config) as conn:
        with conn.cursor() as cur:
            insert_resolved_job_row(
                cur,
                job_id=job_id,
                runner=runner,
                identity=identity,
                inputs={},
                parameters=job_parameters,
                timeout_seconds=timeout_seconds,
                job_type="dataset_download",
                source_job_id=None,
                allow_start_outside_window=allow_start_outside_window,
                now=now,
            )
    return {
        "job_count": 1,
        "created_at": now,
        "dataset": normalized_dataset_name,
        "dataset_version": "unversioned",
        "runner": runner.selector,
        "job_type": "dataset_download",
        "timeout_seconds": timeout_seconds,
        "allow_start_outside_window": allow_start_outside_window,
        "parameters": dict(parameters),
        "jobs": [
            {
                "job_id": job_id,
                "job_ref": job_id,
                "state": "pending",
            }
        ],
}


def update_jobs_allow_outside_window(
    config: OrchestratorConfig,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    allow: bool,
) -> dict[str, Any]:
    where_sql, params = _job_where_clauses(
        job_ids=job_ids,
        dataset=dataset,
        runner=runner,
        runner_selector=runner_selector,
    )
    if not where_sql:
        raise ValueError("job update requires at least one job filter")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT job_id
                FROM jobs
                {where_sql}
                """,
                params,
            )
            rows = list(cur.fetchall())
            if not rows:
                return {"matched": 0, "updated": 0, "jobs": []}
            updated_ids: list[str] = []
            for row in rows:
                cur.execute(
                    """
                    UPDATE jobs
                    SET allow_start_outside_window = %s,
                        updated_at = %s
                    WHERE job_id = %s
                    """,
                    (
                        allow,
                        now,
                        row["job_id"],
                    ),
                )
                updated_ids.append(row["job_id"])
    return {"matched": len(rows), "updated": len(updated_ids), "jobs": updated_ids, "allow_start_outside_window": allow}


def cancel_jobs(
    config: OrchestratorConfig,
    *,
    job_ids: list[str] | None = None,
    dataset: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
) -> dict[str, Any]:
    where_sql, params = _job_where_clauses(
        job_ids=job_ids,
        dataset=dataset,
        runner=runner,
        runner_selector=runner_selector,
    )
    if not where_sql:
        raise ValueError("job cancel requires at least one job filter")
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT job_id, status
                FROM jobs
                {where_sql}
                """,
                params,
            )
            rows = list(cur.fetchall())
            cancellable = [row["job_id"] for row in rows if row["status"] == "pending"]
            if cancellable:
                cur.execute(
                    """
                    SELECT DISTINCT batch_id
                    FROM jobs
                    WHERE job_id = ANY(%s) AND batch_id IS NOT NULL
                    """,
                    (cancellable,),
                )
                impacted_batch_ids = [str(row["batch_id"]) for row in cur.fetchall() if row.get("batch_id")]
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'cancelled',
                        updated_at = %s,
                        completed_at = %s,
                        failure_code = 'JOB_CANCELLED',
                        failure_message = 'cancelled by operator'
                    WHERE job_id = ANY(%s)
                    """,
                    (now, now, cancellable),
                )
                for batch_id in impacted_batch_ids:
                    refresh_batch_record(cur, batch_id, now=now)
    return {
        "matched": len(rows),
        "cancelled": len(cancellable),
        "skipped": len(rows) - len(cancellable),
        "jobs": cancellable,
    }


def fetch_runner_rows(config: OrchestratorConfig, *, selector: str | None = None) -> list[dict[str, Any]]:
    where_sql = ""
    params: list[Any] = []
    if selector:
        where_sql = "WHERE selector = %s"
        params.append(selector)
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  selector,
                  runner_name,
                  runner_type,
                  version,
                  latest,
                  contract_version,
                  inputs_json,
                  launcher_driver,
                  container_image,
                  config_json,
                  missing,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc
                FROM runners
                {where_sql}
                ORDER BY selector
                """,
                params,
            )
            return list(cur.fetchall())


def fetch_sample_rows(config: OrchestratorConfig, *, dataset: str | None = None) -> list[dict[str, Any]]:
    where_sql = "WHERE missing = FALSE"
    params: list[Any] = []
    if dataset:
        dataset_sql, dataset_params = DatasetTarget.parse(dataset).sql_filter()
        where_sql += f" AND {dataset_sql}"
        params.extend(dataset_params)
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  dataset_name,
                  dataset_version,
                  external_key,
                  sample_id,
                  subset_key,
                  inputs_json,
                  data_types_json,
                  dataset_data_types_json,
                  metadata_json,
                  missing
                FROM samples
                {where_sql}
                ORDER BY dataset_name, external_key
                """,
                params,
            )
            return list(cur.fetchall())


def fetch_output_sample_rows(config: OrchestratorConfig, *, dataset: str) -> list[dict[str, Any]]:
    source_runner, dataset_name, subset_prefix = _split_output_dataset_target(config, dataset)
    where_sql = "WHERE output_samples.source_runner_selector = %s"
    params: list[Any] = [source_runner.selector]
    if dataset_name:
        where_sql += " AND output_samples.dataset_name = %s"
        params.append(dataset_name)
    if subset_prefix:
        where_sql += " AND (output_samples.external_key = %s OR output_samples.external_key LIKE %s)"
        params.extend([subset_prefix, f"{subset_prefix}/%"])
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  output_samples.dataset_name,
                  output_samples.dataset_version,
                  output_samples.external_key,
                  output_samples.sample_id,
                  output_samples.subset_key,
                  COALESCE(
                    jsonb_extract_path(
                      producer_jobs.request_json,
                      'inputs',
                      'data',
                      output_samples.sample_id
                    ),
                    samples.inputs_json,
                    '{{}}'::jsonb
                  ) AS inputs_json,
                  COALESCE(
                    samples.dataset_data_types_json,
                    '[]'::jsonb
                  ) AS dataset_data_types_json,
                  COALESCE(samples.metadata_json, '{{}}'::jsonb) AS metadata_json,
                  output_samples.outputs_json,
                  output_samples.metadata_json AS output_metadata_json,
                  output_samples.source_job_id,
                  FALSE AS missing
                FROM output_samples
                LEFT JOIN samples
                  ON samples.dataset_name = output_samples.dataset_name
                 AND samples.dataset_version = output_samples.dataset_version
                 AND samples.external_key = output_samples.external_key
                 AND samples.missing = FALSE
                LEFT JOIN jobs AS producer_jobs
                  ON producer_jobs.job_id = output_samples.source_job_id
                {where_sql}
                ORDER BY
                  output_samples.dataset_name,
                  output_samples.external_key,
                  output_samples.source_job_id
                """,
                params,
            )
            return list(cur.fetchall())


def fetch_output_sample_index_rows(
    config: OrchestratorConfig,
    *,
    target: str | None = None,
    runner: str | None = None,
    runner_selector: str | None = None,
    dataset: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if target:
        source_runner, dataset_name, subset_prefix = _split_output_dataset_target(config, target)
        clauses.append("source_runner_selector = %s")
        params.append(source_runner.selector)
        if dataset_name:
            clauses.append("dataset_name = %s")
            params.append(dataset_name)
        if subset_prefix:
            clauses.append("(external_key = %s OR external_key LIKE %s)")
            params.extend([subset_prefix, f"{subset_prefix}/%"])
    if runner:
        clauses.append("source_runner_name = %s")
        params.append(runner)
    if runner_selector:
        clauses.append("source_runner_selector = %s")
        params.append(runner_selector)
    if dataset:
        dataset_sql, dataset_params = DatasetTarget.parse(dataset).sql_filter()
        clauses.append(dataset_sql)
        params.extend(dataset_params)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  source_job_id,
                  source_runner_selector,
                  source_runner_name,
                  source_runner_version,
                  dataset_name,
                  dataset_version,
                  external_key,
                  subset_key,
                  sample_id,
                  outputs_json,
                  data_types_json,
                  metadata_json,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc
                FROM output_samples
                {where_sql}
                ORDER BY source_runner_selector, dataset_name, external_key, source_job_id
                """,
                params,
            )
            return list(cur.fetchall())


def fetch_runner_usage_rows(config: OrchestratorConfig) -> list[dict[str, Any]]:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  runner_selector,
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
                  MAX(COALESCE(updated_at, created_at)) AT TIME ZONE 'UTC' AS last_seen_utc
                FROM jobs
                GROUP BY runner_selector
                """
            )
            return list(cur.fetchall())


def fetch_dataset_usage_rows(config: OrchestratorConfig) -> list[dict[str, Any]]:
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  dataset_name,
                  COUNT(*)::int AS total,
                  COUNT(*) FILTER (WHERE status = 'completed')::int AS completed,
                  COUNT(*) FILTER (WHERE status = 'pending')::int AS pending,
                  COUNT(*) FILTER (WHERE status = 'failed')::int AS failed,
                  COUNT(*) FILTER (WHERE status = 'cancelled')::int AS cancelled,
                  MAX(COALESCE(updated_at, created_at)) AT TIME ZONE 'UTC' AS last_job_utc
                FROM jobs
                GROUP BY dataset_name
                """
            )
            return list(cur.fetchall())


def fetch_latest_job_rows_by_sample(
    config: OrchestratorConfig,
    *,
    dataset: str | None = None,
) -> list[dict[str, Any]]:
    where_sql, params = _job_where_clauses(dataset=dataset)
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT DISTINCT ON (dataset_name, external_key)
                  job_id,
                  runner_selector,
                  runner_name,
                  runner_version,
                  dataset_name,
                  dataset_version,
                  external_key,
                  subset_key,
                  sample_id,
                  job_type,
                  status,
                  attempt_count,
                  source_job_id,
                  config_json,
                  request_json,
                  result_json,
                  sample_metadata_json,
                  artifacts_json,
                  artifact_count,
                  metric_count,
                  allow_start_outside_window,
                  batch_id,
                  output_dir,
                  created_at AT TIME ZONE 'UTC' AS created_at_utc,
                  updated_at AT TIME ZONE 'UTC' AS updated_at_utc,
                  completed_at AT TIME ZONE 'UTC' AS completed_at_utc,
                  failure_code,
                  failure_message
                FROM jobs
                {where_sql}
                ORDER BY dataset_name, external_key, updated_at DESC NULLS LAST, created_at DESC, job_id DESC
                """,
                params,
            )
            return list(cur.fetchall())


def _resolve_target_batch(
    config: OrchestratorConfig,
    *,
    row: dict[str, Any],
    runner: RunnerDefinition,
) -> tuple[str, str | None, bool, str]:
    source_batch_id = str(row.get("batch_id") or "").strip()
    if source_batch_id:
        batch_row = fetch_batch_row(config, source_batch_id)
        if batch_row is not None:
            runner_endpoint = str(batch_row.get("runner_endpoint") or "").strip() or None
            if _batch_runner_online(runner_endpoint, batch_id=source_batch_id, runner_type=runner.kind):
                return source_batch_id, runner_endpoint, True, source_batch_id
        # Reuse the existing durable batch id when the prior runner is gone so
        # retries do not create a new batch row on every failed startup.
        return source_batch_id, None, False, source_batch_id

    return generated_identifier("batch"), None, False, ""


def _claim_candidate_rows(
    config: OrchestratorConfig,
    *,
    excluded_runner_selectors: set[str] | None = None,
) -> list[dict[str, Any]]:
    known_runner_selectors = sorted(config.runners)
    active_runner_selectors = sorted(
        runner.selector
        for runner in config.runners.values()
        if evaluate_window_state(runner.scheduling or config.orchestrator.scheduling or {}).active
    )
    excluded = sorted(excluded_runner_selectors or ())
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT *
                FROM jobs
                WHERE status = 'pending'
                  AND NOT (runner_selector = ANY(%s))
                  AND (
                    runner_selector = ANY(%s)
                    OR (runner_selector = ANY(%s) AND allow_start_outside_window = TRUE)
                    OR NOT (runner_selector = ANY(%s))
                  )
                ORDER BY created_at
                LIMIT %s
                """,
                (
                    excluded,
                    active_runner_selectors,
                    known_runner_selectors,
                    known_runner_selectors,
                    PENDING_CANDIDATE_LIMIT,
                ),
            )
            return list(cur.fetchall())


def claimable_pending_runner_selectors(
    config: OrchestratorConfig,
) -> list[str]:
    known_runner_selectors = sorted(config.runners)
    active_runner_selectors = sorted(
        runner.selector
        for runner in config.runners.values()
        if evaluate_window_state(
            runner.scheduling or config.orchestrator.scheduling or {}
        ).active
    )
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT DISTINCT runner_selector
                FROM jobs
                WHERE status = 'pending'
                  AND (
                    runner_selector = ANY(%s)
                    OR (runner_selector = ANY(%s) AND allow_start_outside_window = TRUE)
                    OR NOT (runner_selector = ANY(%s))
                  )
                ORDER BY runner_selector
                """,
                (
                    active_runner_selectors,
                    known_runner_selectors,
                    known_runner_selectors,
                ),
            )
            return [
                str(row["runner_selector"])
                for row in cur.fetchall()
            ]


def claim_pending_batch(
    config: OrchestratorConfig,
    *,
    excluded_runner_selectors: set[str] | None = None,
) -> dict[str, Any] | None:
    now = utc_now_timestamp()
    candidates = _claim_candidate_rows(
        config,
        excluded_runner_selectors=excluded_runner_selectors,
    )

    for first in candidates:
        runner = config.runners.get(first["runner_selector"])
        if runner is None:
            with connect_database(config) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'failed',
                            updated_at = %s,
                            completed_at = %s,
                            failure_code = 'RUNNER_MISSING',
                            failure_message = 'runner definition is no longer available'
                        WHERE job_id = %s
                        """,
                        (now, now, first["job_id"]),
                    )
            continue
        window_state = evaluate_window_state(runner.scheduling or config.orchestrator.scheduling or {})
        if not window_state.active and not bool(first.get("allow_start_outside_window")):
            continue

        batch_id, runner_endpoint, reuse_existing_batch, source_batch_id = _resolve_target_batch(
            config,
            row=first,
            runner=runner,
        )
        max_batch_size = int(runner.scheduling.get("max_batch_size") or 1)
        with connect_database(config) as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                candidate_filters = ["status = 'pending'", "runner_selector = %s"]
                candidate_params: list[Any] = [runner.selector]
                if not window_state.active:
                    candidate_filters.append("allow_start_outside_window = TRUE")
                if reuse_existing_batch:
                    candidate_filters.append("(COALESCE(batch_id, '') = '' OR batch_id = %s)")
                    candidate_params.append(batch_id)
                elif source_batch_id:
                    candidate_filters.append("(COALESCE(batch_id, '') = '' OR batch_id = %s)")
                    candidate_params.append(source_batch_id)
                else:
                    candidate_filters.append("COALESCE(batch_id, '') = ''")
                cur.execute(
                    f"""
                    SELECT *
                    FROM jobs
                    WHERE {' AND '.join(candidate_filters)}
                    ORDER BY created_at
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    [*candidate_params, max_batch_size],
                )
                rows = list(cur.fetchall())
                if not rows:
                    continue

                jobs: list[dict[str, Any]] = []
                for row in rows:
                    request_payload = dict(row["request_json"] or {})
                    runtime_output_dir = str(_job_output_dir(config, row))
                    request_payload["job"] = {
                        **dict(request_payload.get("job") or {}),
                        "job_id": row["job_id"],
                        "batch_id": batch_id,
                        "attempt": row["attempt_count"],
                    }
                    request_payload["runtime"] = {"output_dir": runtime_output_dir}
                    cur.execute(
                        """
                        UPDATE jobs
                        SET batch_id = %s,
                            output_dir = %s,
                            request_json = %s,
                            result_json = '{}'::jsonb,
                            artifacts_json = '[]'::jsonb,
                            artifact_count = 0,
                            metric_count = 0,
                            updated_at = %s,
                            completed_at = NULL,
                            failure_code = NULL,
                            failure_message = NULL
                        WHERE job_id = %s
                        """,
                        (
                            batch_id,
                            runtime_output_dir,
                            _json_object(request_payload),
                            now,
                            row["job_id"],
                        ),
                    )
                    jobs.append(
                        {
                            "job_id": row["job_id"],
                            "request_payload": request_payload,
                            "output_dir": runtime_output_dir,
                        }
                    )
                _upsert_batch_record(
                    cur,
                    batch_id=batch_id,
                    runner=runner,
                    runner_endpoint=runner_endpoint,
                    now=now,
                )
                if source_batch_id and source_batch_id != batch_id:
                    refresh_batch_record(cur, source_batch_id, now=now)
                return {
                    "batch_id": batch_id,
                    "runner_selector": runner.selector,
                    "runner_endpoint": runner_endpoint,
                    "window_state": {
                        "active": window_state.active,
                        "start_policy": window_state.start_policy,
                        "end_policy": window_state.end_policy,
                    },
                    "jobs": jobs,
                }
    return None


def write_job_terminal_result(
    config: OrchestratorConfig,
    *,
    job_id: str,
    batch_id: str,
    output_dir: str,
    terminal_state: str,
    updated_at: str | None,
    result_payload: dict[str, Any],
    artifacts_payload: list[dict[str, Any]],
) -> dict[str, Any]:
    # Terminal handling updates durable database state only. Files under
    # output_dir remain runner-owned; the orchestrator does not read back from
    # or rewrite the output directory here.
    completed_at = result_payload.get("completed_at") or updated_at
    failure = result_payload.get("failure") or {}
    final_status = "completed" if terminal_state == "finished" and result_payload.get("status") == "completed" else "failed"
    metrics = result_payload.get("metrics") or []
    now = updated_at or utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                  job_id,
                  status,
                  attempt_count,
                  runner_selector,
                  runner_name,
                  runner_version,
                  dataset_name,
                  dataset_version,
                  external_key,
                  subset_key,
                  sample_id,
                  request_json,
                  sample_metadata_json
                FROM jobs
                WHERE job_id = %s
                FOR UPDATE
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"unknown job_id {job_id!r}")
            attempt_count = int(row["attempt_count"] or 1)
            max_attempts = _max_attempts_for_runner(config, row.get("runner_selector"))
            if row["status"] == "cancelled":
                return {
                    "job_id": job_id,
                    "status": "cancelled",
                    "retry_scheduled": False,
                    "attempt_count": attempt_count,
                    "max_attempts": max_attempts,
                    "terminal_status": "cancelled",
                }
            should_retry = final_status == "failed" and attempt_count < max_attempts
            cur.execute("DELETE FROM job_metrics WHERE job_id = %s", (job_id,))
            if should_retry:
                _delete_output_sample(cur, job_id)
                request_payload = dict(row["request_json"] or {})
                job_payload = dict(request_payload.get("job") or {})
                next_attempt = attempt_count + 1
                job_payload["attempt"] = next_attempt
                job_payload["batch_id"] = None
                request_payload["job"] = job_payload
                request_payload["runtime"] = {}
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        attempt_count = %s,
                        batch_id = NULL,
                        output_dir = NULL,
                        request_json = %s,
                        result_json = '{}'::jsonb,
                        artifacts_json = '[]'::jsonb,
                        artifact_count = 0,
                        metric_count = 0,
                        updated_at = %s,
                        completed_at = NULL,
                        failure_code = NULL,
                        failure_message = NULL
                    WHERE job_id = %s
                    """,
                    (
                        next_attempt,
                        _json_object(request_payload),
                        now,
                        job_id,
                    ),
                )
                if batch_id:
                    refresh_batch_record(cur, batch_id, now=now)
                return {
                    "job_id": job_id,
                    "status": "pending",
                    "retry_scheduled": True,
                    "attempt_count": next_attempt,
                    "max_attempts": max_attempts,
                    "terminal_status": final_status,
                }
            for metric in metrics:
                value = metric.get("value")
                numeric_value = None
                text_value = None
                if isinstance(value, (int, float)):
                    numeric_value = float(value)
                elif value is not None:
                    text_value = str(value)
                cur.execute(
                    """
                    INSERT INTO job_metrics (
                      job_id, metric_namespace, metric_name, metric_type,
                      numeric_value, text_value, unit, source, metadata_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        job_id,
                        str(metric.get("namespace", "")),
                        str(metric.get("name", "")),
                        str(metric.get("type", "")),
                        numeric_value,
                        text_value,
                        metric.get("unit"),
                        metric.get("source"),
                        _json_object(metric.get("metadata") or {}),
                    ),
                )
            cur.execute(
                """
                UPDATE jobs
                SET status = %s,
                    batch_id = %s,
                    output_dir = %s,
                    result_json = %s,
                    artifacts_json = %s,
                    artifact_count = %s,
                    metric_count = %s,
                    updated_at = %s,
                    completed_at = %s,
                    failure_code = %s,
                    failure_message = %s
                WHERE job_id = %s
                """,
                (
                    final_status,
                    batch_id,
                    output_dir,
                    _json_object(result_payload),
                    _json_array(artifacts_payload),
                    len(artifacts_payload),
                    len(metrics),
                    now,
                    completed_at,
                    failure.get("code"),
                    failure.get("message"),
                    job_id,
                ),
            )
            if final_status == "completed":
                _upsert_output_sample(
                    cur,
                    row=row,
                    output_dir=output_dir,
                    output_files=result_payload.get("output_files"),
                    now=now,
                )
            else:
                _delete_output_sample(cur, job_id)
            if batch_id:
                refresh_batch_record(cur, batch_id, now=now)
            return {
                "job_id": job_id,
                "status": final_status,
                "retry_scheduled": False,
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "terminal_status": final_status,
            }


def write_job_dispatch_failure(
    config: OrchestratorConfig,
    *,
    job_id: str,
    batch_id: str,
    output_dir: str,
    error_message: str,
) -> dict[str, Any]:
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT status, attempt_count, runner_selector, request_json
                FROM jobs
                WHERE job_id = %s
                FOR UPDATE
                """,
                (job_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise ValueError(f"unknown job_id {job_id!r}")
            attempt_count = int(row["attempt_count"] or 1)
            max_attempts = _max_attempts_for_runner(config, row.get("runner_selector"))
            if row["status"] == "cancelled":
                return {
                    "job_id": job_id,
                    "status": "cancelled",
                    "retry_scheduled": False,
                    "attempt_count": attempt_count,
                    "max_attempts": max_attempts,
                    "terminal_status": "cancelled",
                }
            if attempt_count < max_attempts:
                _delete_output_sample(cur, job_id)
                request_payload = dict(row["request_json"] or {})
                job_payload = dict(request_payload.get("job") or {})
                next_attempt = attempt_count + 1
                job_payload["attempt"] = next_attempt
                job_payload["batch_id"] = None
                request_payload["job"] = job_payload
                request_payload["runtime"] = {}
                cur.execute("DELETE FROM job_metrics WHERE job_id = %s", (job_id,))
                cur.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending',
                        attempt_count = %s,
                        batch_id = NULL,
                        output_dir = NULL,
                        request_json = %s,
                        result_json = '{}'::jsonb,
                        artifacts_json = '[]'::jsonb,
                        artifact_count = 0,
                        metric_count = 0,
                        updated_at = %s,
                        completed_at = NULL,
                        failure_code = NULL,
                        failure_message = NULL
                    WHERE job_id = %s
                    """,
                    (
                        next_attempt,
                        _json_object(request_payload),
                        now,
                        job_id,
                    ),
                )
                if batch_id:
                    refresh_batch_record(cur, batch_id, now=now)
                return {
                    "job_id": job_id,
                    "status": "pending",
                    "retry_scheduled": True,
                    "attempt_count": next_attempt,
                    "max_attempts": max_attempts,
                    "terminal_status": "failed",
                }
            cur.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    batch_id = %s,
                    output_dir = %s,
                    updated_at = %s,
                    completed_at = %s,
                    failure_code = 'DISPATCH_ERROR',
                    failure_message = %s
                WHERE job_id = %s
                """,
                (batch_id, output_dir, now, now, error_message, job_id),
            )
            _delete_output_sample(cur, job_id)
            if batch_id:
                refresh_batch_record(cur, batch_id, now=now)
            return {
                "job_id": job_id,
                "status": "failed",
                "retry_scheduled": False,
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "terminal_status": "failed",
            }


def release_batch_pending_jobs(config: OrchestratorConfig, *, batch_id: str) -> dict[str, Any]:
    now = utc_now_timestamp()
    with connect_database(config) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT job_id, request_json
                FROM jobs
                WHERE batch_id = %s
                  AND status = 'pending'
                ORDER BY created_at
                """,
                (batch_id,),
            )
            rows = list(cur.fetchall())
            released_job_ids: list[str] = []
            for row in rows:
                request_payload = dict(row["request_json"] or {})
                job_payload = dict(request_payload.get("job") or {})
                job_payload["batch_id"] = None
                request_payload["job"] = job_payload
                request_payload["runtime"] = {}
                cur.execute(
                    """
                    UPDATE jobs
                    SET batch_id = NULL,
                        output_dir = NULL,
                        request_json = %s,
                        result_json = '{}'::jsonb,
                        artifacts_json = '[]'::jsonb,
                        artifact_count = 0,
                        metric_count = 0,
                        updated_at = %s,
                        completed_at = NULL,
                        failure_code = NULL,
                        failure_message = NULL
                    WHERE job_id = %s
                    """,
                    (
                        _json_object(request_payload),
                        now,
                        row["job_id"],
                    ),
                )
                released_job_ids.append(str(row["job_id"]))
            refresh_batch_record(cur, batch_id, now=now)
    return {
        "batch_id": batch_id,
        "released": len(released_job_ids),
        "jobs": released_job_ids,
    }

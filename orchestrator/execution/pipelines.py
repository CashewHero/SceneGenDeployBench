from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.config import OrchestratorConfig, RunnerDefinition
from domain.pipelines import (
    STAGE_OUTPUT_REFERENCE_RE,
    PipelineDefinition,
    resolve_static_value,
    stage_dependencies,
)
from execution.script_run import run_script_container
from storage.db import _primary_role_data, _target_sample_rows
from storage.pipelines import (
    claim_pipeline_script_stage_execution,
    fail_pipeline_run,
    fetch_pipeline_runs,
    fetch_pipeline_stage_executions,
    fetch_pipeline_job_outputs,
    finish_pipeline_script_stage_execution,
    insert_pipeline_stage_job,
    insert_pipeline_stage_execution,
    mark_pipeline_job_outputs_removed,
    mark_pipeline_run_terminal,
)

logger = logging.getLogger("scenegendeploybench.orchestrator.pipelines")
TERMINAL = {"completed", "failed", "cancelled", "skipped"}
PIPELINE_LANE_INDEX = 0


@dataclass(frozen=True)
class SourceItem:
    identity: dict[str, Any]
    data: dict[str, Any]
    source_job_id: str | None = None


def _runner(config: OrchestratorConfig, selector: str) -> RunnerDefinition:
    normalized = selector.strip()
    if normalized in config.runners:
        return config.runners[normalized]
    if normalized in config.runners_by_name:
        latest = config.latest_runners.get(normalized)
        if latest:
            return config.runners[latest]
        return config.runners_by_name[normalized][0]
    if normalized.endswith("@latest"):
        return _runner(config, normalized.removesuffix("@latest"))
    raise ValueError(f"pipeline references unknown runner {selector!r}")


def _identity(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_name": str(row["dataset_name"]),
        "dataset_version": str(row["dataset_version"]),
        "external_key": str(row["external_key"]),
        "subset_key": str(row.get("subset_key") or ""),
        "sample_id": str(row.get("sample_id") or row["external_key"]),
        "metadata_json": dict(row.get("metadata_json") or {}),
    }


def _identity_key(identity: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(identity["dataset_name"]),
        str(identity["dataset_version"]),
        str(identity["external_key"]),
    )


def _pipeline_run_timestamp(run: dict[str, Any]) -> str:
    run_id = str(run["pipeline_run_id"])
    parts = run_id.split("_", 2)
    return parts[1] if len(parts) == 3 else run_id


def _pipeline_run_directory(run: dict[str, Any]) -> Path:
    return Path(str(run["pipeline_name"])) / _pipeline_run_timestamp(run)


def _stage_scope(stage: dict[str, Any]) -> str:
    return str(stage.get("scope") or "matrix")


def _stage_execution_lanes(
    stage: dict[str, Any],
    lanes: list[dict[str, Any]],
) -> list[tuple[int, dict[str, Any]]]:
    if _stage_scope(stage) == "pipeline":
        return [(PIPELINE_LANE_INDEX, {})]
    return [(index, dict(lane)) for index, lane in enumerate(lanes)]


def _dependency_lane_index(
    *,
    stage: dict[str, Any],
    dependency: dict[str, Any],
    lane_index: int,
) -> int | None:
    if _stage_scope(dependency) == "pipeline":
        return PIPELINE_LANE_INDEX
    if _stage_scope(stage) == "matrix":
        return lane_index
    return None


def _script_execution_directory(
    run: dict[str, Any],
    *,
    stage_id: str,
    stage: dict[str, Any],
    lane_index: int,
) -> Path:
    path = _pipeline_run_directory(run) / stage_id
    if (
        _stage_scope(stage) == "matrix"
        and len(run.get("lanes_json") or [{}]) > 1
    ):
        path /= str(lane_index)
    return path


def _effective_retention(stage: dict[str, Any]) -> str:
    retention = str(stage.get("retention") or "keep")
    if retention == "matrix" and _stage_scope(stage) == "pipeline":
        return "pipeline"
    return retention


def _target_sources(config: OrchestratorConfig, target: str) -> list[SourceItem]:
    from_output, _, rows = _target_sample_rows(
        config,
        target,
        option_name="pipeline input",
    )
    return [
        SourceItem(
            identity=_identity(row),
            data=_primary_role_data(row, from_output=from_output, role="pipeline"),
            source_job_id=str(row.get("source_job_id") or "") or None,
        )
        for row in rows
    ]


def _stage_rows(
    rows: list[dict[str, Any]],
    *,
    stage_id: str,
    lane_index: int | None,
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if row["stage_id"] == stage_id
        and (lane_index is None or int(row["lane_index"]) == lane_index)
    ]


def _stage_output_sources(rows: list[dict[str, Any]]) -> list[SourceItem]:
    sources: list[SourceItem] = []
    for row in rows:
        if row["status"] != "completed":
            continue
        output_files = dict(row.get("result_json") or {}).get("output_files")
        if not isinstance(output_files, dict) or not output_files:
            continue
        row_sample_id = str(row.get("sample_id") or "")
        for raw_sample_id, sample_data in output_files.items():
            sample_id = str(raw_sample_id).strip()
            if not sample_id or not isinstance(sample_data, dict):
                continue
            external_key = (
                str(row.get("external_key") or sample_id)
                if sample_id == row_sample_id
                else sample_id
            )
            identity = {
                "dataset_name": str(row.get("dataset_name") or ""),
                "dataset_version": str(row.get("dataset_version") or ""),
                "external_key": external_key,
                "subset_key": "",
                "sample_id": sample_id,
                "metadata_json": {},
            }
            sources.append(
                SourceItem(
                    identity=identity,
                    data=dict(sample_data),
                    source_job_id=str(row.get("job_id") or "") or None,
                )
            )
    return sources


def _resolve_sources(
    config: OrchestratorConfig,
    *,
    expression: Any,
    dataset: str,
    stage: dict[str, Any],
    stages: dict[str, dict[str, Any]],
    lane_index: int,
    stage_rows: list[dict[str, Any]],
) -> list[SourceItem]:
    if isinstance(expression, str) and not expression.lstrip().startswith("${{"):
        return _target_sources(config, expression)
    if not isinstance(expression, str):
        raise ValueError(
            f"runner input must be a dataset or stage reference, got {expression!r}"
        )
    match = STAGE_OUTPUT_REFERENCE_RE.fullmatch(expression)
    if not match:
        raise ValueError(f"unsupported runner input reference {expression!r}")
    dependency = match.group(1)
    dependency_stage = stages[dependency]
    dependency_rows = _stage_rows(
        stage_rows,
        stage_id=dependency,
        lane_index=_dependency_lane_index(
            stage=stage,
            dependency=dependency_stage,
            lane_index=lane_index,
        ),
    )
    return _stage_output_sources(dependency_rows)


def _contract_types(
    runner: RunnerDefinition,
    role: str,
    sample_requirement: str,
) -> tuple[list[str], list[str]]:
    contract = runner.inputs[role][sample_requirement]
    return (
        list(contract["required_datatype"]),
        list(contract["optional_datatype"]),
    )


def _select_data(
    data: dict[str, Any],
    *,
    required: list[str],
    optional: list[str],
    description: str,
) -> dict[str, Any]:
    missing = [data_type for data_type in required if data_type not in data]
    if missing:
        raise ValueError(f"{description} is missing data types: {', '.join(missing)}")
    declared = list(dict.fromkeys(required + optional))
    return {key: data[key] for key in declared if key in data}


def _matching_source(
    sources: list[SourceItem],
    identity: dict[str, Any],
    *,
    role: str,
) -> SourceItem:
    matches = [
        source
        for source in sources
        if _identity_key(source.identity) == _identity_key(identity)
    ]
    if not matches:
        sample_id = str(identity.get("sample_id") or "")
        matches = [
            source
            for source in sources
            if sample_id
            and str(source.identity.get("sample_id") or "") == sample_id
        ]
    if len(matches) != 1:
        raise ValueError(
            f"pipeline {role} must match one source for {identity['external_key']!r}, "
            f"found {len(matches)}"
        )
    return matches[0]


def _runner_inputs(
    config: OrchestratorConfig,
    *,
    runner: RunnerDefinition,
    stage: dict[str, Any],
    identity: dict[str, Any],
    dataset: str,
    stages: dict[str, dict[str, Any]],
    lane_index: int,
    stage_rows: list[dict[str, Any]],
) -> tuple[dict[str, Any], str | None]:
    inputs: dict[str, Any] = {}
    source_job_id: str | None = None
    configured_inputs = dict(stage.get("inputs") or {})
    for role in ("data", "candidate", "references"):
        if role not in configured_inputs:
            required, _ = _contract_types(
                runner,
                role,
                "optional_sample" if role == "references" else "required_sample",
            )
            if required and role != "references":
                raise ValueError(
                    f"runner {runner.selector} requires pipeline stage input {role}"
                )
            continue
        sources = _resolve_sources(
            config,
            expression=configured_inputs[role],
            dataset=dataset,
            stage=stage,
            stages=stages,
            lane_index=lane_index,
            stage_rows=stage_rows,
        )
        sample_requirement = "optional_sample" if role == "references" else "required_sample"
        required, optional = _contract_types(runner, role, sample_requirement)
        if role == "references":
            selected: dict[str, Any] = {}
            for source in sources:
                if _identity_key(source.identity) == _identity_key(identity):
                    continue
                sample_id = str(source.identity["sample_id"])
                selected[sample_id] = _select_data(
                    source.data,
                    required=required,
                    optional=optional,
                    description=f"reference {source.identity['external_key']!r}",
                )
            if selected:
                inputs[role] = selected
            continue
        source = _matching_source(sources, identity, role=role)
        selected_data = _select_data(
            source.data,
            required=required,
            optional=optional,
            description=f"{role} {identity['external_key']!r}",
        )
        if selected_data:
            inputs[role] = {str(identity["sample_id"]): selected_data}
        source_job_id = source_job_id or source.source_job_id
    return inputs, source_job_id


def _dependency_rows(
    all_rows: list[dict[str, Any]],
    *,
    stage: dict[str, Any],
    stages: dict[str, dict[str, Any]],
    lane_index: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for dependency in stage_dependencies(stage):
        dependency_rows = _stage_rows(
            all_rows,
            stage_id=dependency,
            lane_index=_dependency_lane_index(
                stage=stage,
                dependency=stages[dependency],
                lane_index=lane_index,
            ),
        )
        rows.extend(dependency_rows)
    return rows


def _dependencies_ready(
    all_rows: list[dict[str, Any]],
    *,
    stage: dict[str, Any],
    stages: dict[str, dict[str, Any]],
    lane_index: int,
) -> bool:
    dependencies = stage_dependencies(stage)
    if not dependencies:
        return True
    for dependency in dependencies:
        rows = _stage_rows(
            all_rows,
            stage_id=dependency,
            lane_index=_dependency_lane_index(
                stage=stage,
                dependency=stages[dependency],
                lane_index=lane_index,
            ),
        )
        if not rows or any(row["status"] not in TERMINAL for row in rows):
            return False
    return True


def _script_context(
    *,
    run: dict[str, Any],
    stage_id: str,
    stage: dict[str, Any],
    lane_index: int,
    lane: dict[str, Any],
    dependencies: list[dict[str, Any]],
) -> dict[str, Any]:
    needs: dict[str, list[dict[str, Any]]] = {}
    for row in dependencies:
        needs.setdefault(str(row["stage_id"]), []).append(
            {
                "sample": row.get("sample_id"),
                "external_key": row.get("external_key"),
                "lane_index": row.get("lane_index"),
                "matrix": dict(row.get("lane_json") or {}),
                "status": row.get("status"),
                "job_id": row.get("job_id"),
                "result": dict(row.get("result_json") or {}),
            }
        )
    return {
        "pipeline": {
            "run_id": str(run["pipeline_run_id"]),
            "name": str(run["pipeline_name"]),
            "dataset": str(run["dataset_target"]),
        },
        "stage": {
            "id": stage_id,
            "scope": _stage_scope(stage),
            "lane_index": (
                None if _stage_scope(stage) == "pipeline" else lane_index
            ),
            "matrix": lane,
        },
        "needs": needs,
    }


def _materialize_script_stage(
    config: OrchestratorConfig,
    *,
    run: dict[str, Any],
    stage_id: str,
    stage: dict[str, Any],
    stages: dict[str, dict[str, Any]],
    lane_index: int,
    lane: dict[str, Any],
    all_rows: list[dict[str, Any]],
) -> bool:
    if not _dependencies_ready(
        all_rows,
        stage=stage,
        stages=stages,
        lane_index=lane_index,
    ):
        return False
    dependencies = _dependency_rows(
        all_rows,
        stage=stage,
        stages=stages,
        lane_index=lane_index,
    )
    existing = _stage_rows(
        all_rows,
        stage_id=stage_id,
        lane_index=lane_index,
    )
    if existing and any(row["status"] != "pending" for row in existing):
        return False
    if stage["if"] == "success()" and any(
        row["status"] != "completed" for row in dependencies
    ):
        if existing:
            return False
        insert_pipeline_stage_execution(
            config,
            pipeline_run_id=str(run["pipeline_run_id"]),
            stage_id=stage_id,
            lane_index=lane_index,
            lane=lane,
            identity={"external_key": "__script__", "sample_id": "__script__"},
            status="skipped",
        )
        return True

    claimed = claim_pipeline_script_stage_execution(
        config,
        pipeline_run_id=str(run["pipeline_run_id"]),
        stage_id=stage_id,
        lane_index=lane_index,
        lane=lane,
        dataset_name=str(run["pipeline_name"]),
        dataset_version=_pipeline_run_timestamp(run),
    )
    if claimed is None:
        return False

    context = _script_context(
        run=run,
        stage_id=stage_id,
        stage=stage,
        lane_index=lane_index,
        lane=lane,
        dependencies=dependencies,
    )
    raw_run = stage.get("run")
    if isinstance(raw_run, str):
        command = ["sh", "-lc", raw_run]
    else:
        resolved_run = resolve_static_value(
            list(raw_run or []),
            dataset=str(run["dataset_target"]),
            lane=lane,
        )
        command = [str(item) for item in resolved_run]
    environment = resolve_static_value(
        dict(stage.get("env") or {}),
        dataset=str(run["dataset_target"]),
        lane=lane,
    )
    access = stage.get("access") or []
    if isinstance(access, str):
        access = [access]
    mounts = stage.get("mounts") or []
    if isinstance(mounts, str):
        mounts = [mounts]

    exit_code = 1
    script_result: dict[str, Any] = {}
    failure_message: str | None = None
    try:
        execution_result = run_script_container(
            config,
            image=str(stage["image"]),
            script_path=None,
            command=command,
            access_values=[str(value) for value in access],
            environment_values=[
                f"{key}={value}" for key, value in environment.items()
            ],
            mount_values=[str(value) for value in mounts],
            workdir=str(stage.get("workdir") or "/workspace"),
            workspace_files={
                "pipeline.json": json.dumps(context, indent=2) + "\n",
            },
            labels={
                "scenegendeploybench.pipeline_run_id": str(
                    run["pipeline_run_id"]
                ),
                "scenegendeploybench.pipeline_stage_execution_id": str(
                    claimed["pipeline_stage_execution_id"]
                ),
            },
            timeout_seconds=float(stage["timeout-minutes"]) * 60,
            publish_dir=_script_execution_directory(
                run,
                stage_id=stage_id,
                stage=stage,
                lane_index=lane_index,
            ),
            retention=str(stage["retention"]),
        )
        exit_code = execution_result.exit_code
        script_result = execution_result.result
        if exit_code != 0:
            failure_message = f"script exited with code {exit_code}"
    except Exception as exc:
        failure_message = str(exc)
    finish_pipeline_script_stage_execution(
        config,
        pipeline_stage_execution_id=str(claimed["pipeline_stage_execution_id"]),
        status="completed" if exit_code == 0 and failure_message is None else "failed",
        result={**script_result, "exit_code": exit_code},
        failure_message=failure_message,
    )
    return True


def _materialize_runner_stage(
    config: OrchestratorConfig,
    *,
    run: dict[str, Any],
    stage_id: str,
    stage: dict[str, Any],
    stages: dict[str, dict[str, Any]],
    lane_index: int,
    lane: dict[str, Any],
    all_rows: list[dict[str, Any]],
) -> bool:
    if not _dependencies_ready(
        all_rows,
        stage=stage,
        stages=stages,
        lane_index=lane_index,
    ):
        return False
    dependencies = _dependency_rows(
        all_rows,
        stage=stage,
        stages=stages,
        lane_index=lane_index,
    )
    if stage["if"] == "success()" and any(
        row["status"] != "completed" for row in dependencies
    ):
        existing = _stage_rows(
            all_rows,
            stage_id=stage_id,
            lane_index=lane_index,
        )
        if existing:
            return False
        insert_pipeline_stage_execution(
            config,
            pipeline_run_id=str(run["pipeline_run_id"]),
            stage_id=stage_id,
            lane_index=lane_index,
            lane=lane,
            identity={"external_key": "__empty__", "sample_id": "__empty__"},
            status="skipped",
        )
        return True
    runner = _runner(config, str(stage["runner"]))
    parameters = {
        **runner.job_parameters,
        **dict(
            resolve_static_value(
                stage.get("with") or {},
                dataset=str(run["dataset_target"]),
                lane=lane,
            )
        ),
    }
    existing = {
        str(row["external_key"])
        for row in _stage_rows(all_rows, stage_id=stage_id, lane_index=lane_index)
    }
    if runner.kind == "dataset_downloader":
        if existing:
            return False
        dataset_name = str(
            parameters.get("dataset_name") or run["dataset_target"]
        ).strip().strip("/")
        if not dataset_name:
            raise ValueError(
                f"dataset downloader stage {stage_id!r} requires a pipeline "
                "dataset or with.dataset_name"
            )
        parameters["dataset_name"] = dataset_name
        job_id = insert_pipeline_stage_job(
            config,
            pipeline_run_id=str(run["pipeline_run_id"]),
            stage_id=stage_id,
            lane_index=lane_index,
            lane=lane,
            runner=runner,
            identity={
                "dataset_name": dataset_name,
                "dataset_version": "unversioned",
                "external_key": "",
                "subset_key": "",
                "sample_id": None,
                "metadata_json": {"dataset_download": True},
            },
            inputs={},
            parameters=parameters,
            timeout_seconds=int(float(stage["timeout-minutes"]) * 60),
            allow_start_outside_window=bool(run["allow_start_outside_window"]),
            job_type="dataset_download",
            source_job_id=None,
        )
        return job_id is not None

    configured_inputs = dict(stage.get("inputs") or {})
    primary_role = "candidate" if "candidate" in configured_inputs else "data"
    if primary_role not in configured_inputs:
        raise ValueError(
            f"runner stage {stage_id!r} using {runner.selector} requires "
            "inputs.data or inputs.candidate"
        )
    primary_expression = resolve_static_value(
        configured_inputs[primary_role],
        dataset=str(run["dataset_target"]),
        lane=lane,
    )
    sources = _resolve_sources(
        config,
        expression=primary_expression,
        dataset=str(run["dataset_target"]),
        stage=stage,
        stages=stages,
        lane_index=lane_index,
        stage_rows=all_rows,
    )
    created = False
    for source in sources:
        identity = source.identity
        if identity["external_key"] in existing:
            continue
        inputs, source_job_id = _runner_inputs(
            config,
            runner=runner,
            stage={
                **stage,
                "inputs": {
                    role: resolve_static_value(
                        configured_inputs[role],
                        dataset=str(run["dataset_target"]),
                        lane=lane,
                    )
                    for role in ("data", "candidate", "references")
                    if role in configured_inputs
                },
            },
            identity=identity,
            dataset=str(run["dataset_target"]),
            stages=stages,
            lane_index=lane_index,
            stage_rows=all_rows,
        )
        job_id = insert_pipeline_stage_job(
            config,
            pipeline_run_id=str(run["pipeline_run_id"]),
            stage_id=stage_id,
            lane_index=lane_index,
            lane=lane,
            runner=runner,
            identity=identity,
            inputs=inputs,
            parameters=parameters,
            timeout_seconds=int(float(stage["timeout-minutes"]) * 60),
            allow_start_outside_window=bool(run["allow_start_outside_window"]),
            job_type=(
                "evaluation" if runner.kind == "evaluator" else "generation"
            ),
            source_job_id=source_job_id,
        )
        created = created or job_id is not None
    if not sources and stage_dependencies(stage) and not existing:
        insert_pipeline_stage_execution(
            config,
            pipeline_run_id=str(run["pipeline_run_id"]),
            stage_id=stage_id,
            lane_index=lane_index,
            lane=lane,
            identity={"external_key": "__empty__", "sample_id": "__empty__"},
            status="skipped",
        )
        created = True
    return created


def _remove_empty_output_directories(path: Path, *, root: Path) -> None:
    parent = path.parent
    while parent != root:
        try:
            parent.rmdir()
        except OSError:
            return
        parent = parent.parent


def _runner_output_paths(
    config: OrchestratorConfig,
    record: dict[str, Any],
) -> set[Path]:
    output_root = config.storage.output_root.resolve()
    output_dir = Path(str(record.get("output_dir") or "")).resolve()
    try:
        output_dir.relative_to(output_root)
    except ValueError:
        return set()

    raw_paths: list[Any] = []
    output_files = dict(record.get("result_json") or {}).get("output_files")
    if isinstance(output_files, dict):
        for sample_files in output_files.values():
            if isinstance(sample_files, dict):
                raw_paths.extend(sample_files.values())
    artifacts = record.get("artifacts_json")
    if isinstance(artifacts, list):
        raw_paths.extend(
            artifact.get("path")
            for artifact in artifacts
            if isinstance(artifact, dict)
        )

    paths: set[Path] = set()
    for raw_path in raw_paths:
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        path = Path(raw_path)
        path = (path if path.is_absolute() else output_dir / path).resolve()
        try:
            path.relative_to(output_dir)
        except ValueError:
            continue
        paths.add(path)
    return paths


def _remove_runner_outputs(
    config: OrchestratorConfig,
    records: list[dict[str, Any]],
) -> None:
    removed_jobs: list[str] = []
    output_root = config.storage.output_root.resolve()
    for record in records:
        for path in _runner_output_paths(config, record):
            if path.is_dir():
                shutil.rmtree(path)
            else:
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
            _remove_empty_output_directories(path, root=output_root)
        removed_jobs.append(str(record["job_id"]))
    mark_pipeline_job_outputs_removed(config, removed_jobs)


def _remove_script_output(
    config: OrchestratorConfig,
    run: dict[str, Any],
    *,
    stage_id: str,
    stage: dict[str, Any],
    lane_index: int,
) -> None:
    pipeline_root = config.storage.pipeline_root.resolve()
    run_root = (pipeline_root / _pipeline_run_directory(run)).resolve()
    path = (
        pipeline_root
        / _script_execution_directory(
            run,
            stage_id=stage_id,
            stage=stage,
            lane_index=lane_index,
        )
    ).resolve()
    try:
        path.relative_to(run_root)
    except ValueError:
        return
    shutil.rmtree(path, ignore_errors=True)
    for directory in (path.parent, run_root):
        try:
            directory.rmdir()
        except OSError:
            pass


def cleanup_pipeline_outputs(
    config: OrchestratorConfig,
    run: dict[str, Any],
    *,
    retentions: set[str] | None = None,
    lane_index: int | None = None,
    stage_ids: set[str] | None = None,
) -> None:
    stages = dict(dict(run.get("config_json") or {}).get("stages") or {})
    selected_retentions = retentions or {"pipeline", "matrix", "none"}
    transient_stages = {
        str(stage_id)
        for stage_id, stage in stages.items()
        if isinstance(stage, dict)
        and _effective_retention(stage) in selected_retentions
        and (stage_ids is None or str(stage_id) in stage_ids)
    }
    if not transient_stages:
        return
    runner_records = [
        record
        for record in fetch_pipeline_job_outputs(
            config,
            str(run["pipeline_run_id"]),
        )
        if str(record["stage_id"]) in transient_stages
        and (
            lane_index is None
            or int(record["lane_index"]) == lane_index
        )
    ]
    _remove_runner_outputs(config, runner_records)

    lanes = list(run.get("lanes_json") or [{}])
    for stage_id in transient_stages:
        stage = stages[stage_id]
        if not stage.get("image"):
            continue
        execution_lanes = _stage_execution_lanes(stage, lanes)
        for execution_lane_index, _ in execution_lanes:
            if (
                lane_index is not None
                and execution_lane_index != lane_index
            ):
                continue
            _remove_script_output(
                config,
                run,
                stage_id=stage_id,
                stage=stage,
                lane_index=execution_lane_index,
            )


def _execution_finished(
    rows: list[dict[str, Any]],
    *,
    stage_id: str,
    lane_index: int,
) -> bool:
    execution_rows = _stage_rows(
        rows,
        stage_id=stage_id,
        lane_index=lane_index,
    )
    return bool(execution_rows) and all(
        row["status"] in TERMINAL for row in execution_rows
    )


def _cleanup_none_outputs(
    config: OrchestratorConfig,
    *,
    run: dict[str, Any],
    definition: PipelineDefinition,
    rows: list[dict[str, Any]],
) -> bool:
    cleaned = False
    lanes = list(run["lanes_json"] or [{}])
    for stage_id, stage in definition.stages.items():
        if _effective_retention(stage) != "none":
            continue
        for lane_index, _ in _stage_execution_lanes(stage, lanes):
            if not _execution_finished(
                rows,
                stage_id=stage_id,
                lane_index=lane_index,
            ):
                continue
            cleanup_pipeline_outputs(
                config,
                run,
                retentions={"none"},
                lane_index=lane_index,
                stage_ids={stage_id},
            )
            cleaned = True
    return cleaned


def _cleanup_finished_matrix_lanes(
    config: OrchestratorConfig,
    *,
    run: dict[str, Any],
    definition: PipelineDefinition,
    rows: list[dict[str, Any]],
) -> bool:
    cleaned = False
    lanes = list(run["lanes_json"] or [{}])
    for lane_index in range(len(lanes)):
        if not all(
            _execution_finished(
                rows,
                stage_id=stage_id,
                lane_index=(
                    lane_index
                    if _stage_scope(stage) == "matrix"
                    else PIPELINE_LANE_INDEX
                ),
            )
            for stage_id, stage in definition.stages.items()
        ):
            continue
        cleanup_pipeline_outputs(
            config,
            run,
            retentions={"matrix"},
            lane_index=lane_index,
        )
        cleaned = True
    return cleaned


def _finish_if_terminal(
    config: OrchestratorConfig,
    *,
    run: dict[str, Any],
    definition: PipelineDefinition,
    rows: list[dict[str, Any]],
) -> bool:
    lanes = list(run["lanes_json"] or [{}])
    for stage_id, stage in definition.stages.items():
        for lane_index, _ in _stage_execution_lanes(stage, lanes):
            stage_rows = _stage_rows(
                rows,
                stage_id=stage_id,
                lane_index=lane_index,
            )
            if not stage_rows or any(row["status"] not in TERMINAL for row in stage_rows):
                return False
    failures = [
        row
        for row in rows
        if row["status"] in {"failed", "cancelled"}
    ]
    cleanup_pipeline_outputs(
        config,
        run,
        retentions={"pipeline", "matrix", "none"},
    )
    mark_pipeline_run_terminal(
        config,
        pipeline_run_id=str(run["pipeline_run_id"]),
        status="failed" if failures else "completed",
        failure_message=(
            f"{len(failures)} pipeline stage execution(s) failed or were cancelled"
            if failures
            else None
        ),
    )
    return True


def reconcile_pipeline_run(
    config: OrchestratorConfig,
    run: dict[str, Any],
) -> dict[str, Any]:
    definition = PipelineDefinition(
        name=str(run["pipeline_name"]),
        path=Path(str(run["config_path"])),
        dataset=str(run["dataset_target"]),
        matrix=dict(run["config_json"].get("matrix") or {}),
        stages=dict(run["config_json"]["stages"]),
        raw=dict(run["config_json"]),
    )
    created = 0
    lanes = list(run["lanes_json"] or [{}])
    rows = fetch_pipeline_stage_executions(config, str(run["pipeline_run_id"]))
    if _cleanup_none_outputs(
        config,
        run=run,
        definition=definition,
        rows=rows,
    ):
        rows = fetch_pipeline_stage_executions(
            config,
            str(run["pipeline_run_id"]),
        )
    for stage_id, stage in definition.stages.items():
        stage_changed = False
        for lane_index, lane in _stage_execution_lanes(stage, lanes):
            materialize = (
                _materialize_runner_stage
                if stage.get("runner")
                else _materialize_script_stage
            )
            changed = materialize(
                config,
                run=run,
                stage_id=stage_id,
                stage=stage,
                stages=definition.stages,
                lane_index=lane_index,
                lane=dict(lane),
                all_rows=rows,
            )
            created += int(changed)
            stage_changed = stage_changed or changed
        if stage_changed:
            rows = fetch_pipeline_stage_executions(
                config,
                str(run["pipeline_run_id"]),
            )
            if _cleanup_none_outputs(
                config,
                run=run,
                definition=definition,
                rows=rows,
            ):
                rows = fetch_pipeline_stage_executions(
                    config,
                    str(run["pipeline_run_id"]),
                )
    _cleanup_finished_matrix_lanes(
        config,
        run=run,
        definition=definition,
        rows=rows,
    )
    finished = _finish_if_terminal(
        config,
        run=run,
        definition=definition,
        rows=rows,
    )
    return {
        "pipeline_run_id": run["pipeline_run_id"],
        "materialized": created,
        "finished": finished,
    }


def reconcile_pipelines(config: OrchestratorConfig) -> dict[str, Any]:
    results = []
    for run in fetch_pipeline_runs(config, active_only=True):
        try:
            results.append(reconcile_pipeline_run(config, run))
        except Exception as exc:
            logger.exception(
                "pipeline reconciliation failed: run=%s error=%s",
                run["pipeline_run_id"],
                exc,
            )
            try:
                fail_pipeline_run(
                    config,
                    pipeline_run_id=str(run["pipeline_run_id"]),
                    failure_message=str(exc),
                )
                cleanup_pipeline_outputs(config, run)
            except ValueError:
                # A concurrent user cancellation already made the run terminal.
                pass
            results.append(
                {
                    "pipeline_run_id": run["pipeline_run_id"],
                    "failed": True,
                    "error": str(exc),
                }
            )
    return {"pipeline_count": len(results), "runs": results}

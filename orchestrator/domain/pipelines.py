from __future__ import annotations

import itertools
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:
    yaml = None


ID_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PIPELINE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")
STATIC_REFERENCE_RE = re.compile(
    r"^\$\{\{\s*(dataset|matrix\.[A-Za-z_][A-Za-z0-9_]*)\s*\}\}$"
)
INPUT_REFERENCE_RE = re.compile(
    r"^\$\{\{\s*(dataset|stages\.[A-Za-z_][A-Za-z0-9_]*\.outputs)\s*\}\}$"
)
STAGE_OUTPUT_REFERENCE_RE = re.compile(
    r"^\$\{\{\s*stages\.([A-Za-z_][A-Za-z0-9_]*)\.outputs\s*\}\}$"
)


@dataclass(frozen=True)
class PipelineDefinition:
    name: str
    path: Path
    dataset: str | None
    matrix: dict[str, list[Any]]
    stages: dict[str, dict[str, Any]]
    raw: dict[str, Any]


def _read_yaml(path: Path) -> dict[str, Any]:
    if yaml is None:
        raise RuntimeError("PyYAML is required to read pipeline files")
    if not path.is_file():
        raise FileNotFoundError(f"pipeline file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"pipeline file must contain a mapping: {path}")
    return dict(loaded)


def _pipeline_catalog_paths(root: Path) -> list[Path]:
    if not root.exists():
        return []
    if not root.is_dir():
        raise NotADirectoryError(f"pipeline catalog path is not a directory: {root}")
    return sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in {".yaml", ".yml"}
    )


def resolve_pipeline_path(
    pipeline_catalog: Path,
    *,
    name: str | None,
    file_path: str | None,
) -> Path:
    if bool(name) == bool(file_path):
        raise ValueError("choose exactly one pipeline name or --file")
    if file_path:
        return Path(file_path).expanduser().resolve()

    normalized = str(name or "").strip()
    matches: list[Path] = []
    for path in _pipeline_catalog_paths(pipeline_catalog):
        loaded = _read_yaml(path)
        declared_name = str(loaded.get("name") or path.stem).strip()
        if normalized in {declared_name, path.stem}:
            matches.append(path)
    if not matches:
        raise ValueError(
            f"pipeline {normalized!r} was not found under {pipeline_catalog}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"pipeline {normalized!r} is ambiguous: "
            + ", ".join(str(path) for path in matches)
        )
    return matches[0]


def list_pipeline_definitions(pipeline_catalog: Path) -> list[PipelineDefinition]:
    return [load_pipeline(path) for path in _pipeline_catalog_paths(pipeline_catalog)]


def _validate_references(
    value: Any,
    field_name: str,
    *,
    pattern: re.Pattern[str],
) -> None:
    if isinstance(value, str) and value.lstrip().startswith("${{"):
        if not pattern.fullmatch(value):
            raise ValueError(f"{field_name} contains unsupported reference {value!r}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_references(item, f"{field_name}[{index}]", pattern=pattern)
    elif isinstance(value, dict):
        for key, item in value.items():
            _validate_references(item, f"{field_name}.{key}", pattern=pattern)


def _normalize_needs(path: Path, stage_id: str, value: Any) -> list[str]:
    if value is None or value == "":
        return []
    raw_needs = [value] if isinstance(value, str) else value
    if not isinstance(raw_needs, list) or not raw_needs:
        raise ValueError(f"{path}.stages.{stage_id}.needs must be a stage id or list")
    needs: list[str] = []
    for raw_dependency in raw_needs:
        dependency = str(raw_dependency).strip()
        if not ID_RE.fullmatch(dependency):
            raise ValueError(
                f"{path}.stages.{stage_id}.needs contains invalid id {dependency!r}"
            )
        if dependency == stage_id:
            raise ValueError(f"{path}.stages.{stage_id} cannot depend on itself")
        if dependency not in needs:
            needs.append(dependency)
    return needs


def _validate_dependency_graph(path: Path, stages: dict[str, dict[str, Any]]) -> None:
    for stage_id, stage in stages.items():
        for dependency in stage_dependencies(stage):
            if dependency not in stages:
                raise ValueError(
                    f"{path}.stages.{stage_id}.needs references unknown stage "
                    f"{dependency!r}"
                )

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(stage_id: str) -> None:
        if stage_id in visited:
            return
        if stage_id in visiting:
            raise ValueError(f"{path}.stages contains a dependency cycle at {stage_id!r}")
        visiting.add(stage_id)
        for dependency in stage_dependencies(stages[stage_id]):
            visit(dependency)
        visiting.remove(stage_id)
        visited.add(stage_id)

    for stage_id in stages:
        visit(stage_id)


def load_pipeline(path: Path) -> PipelineDefinition:
    raw = _read_yaml(path)
    version = int(raw.get("pipeline_version", 1))
    if version != 1:
        raise ValueError(f"{path} uses unsupported pipeline_version {version}")
    unsupported_top_level = sorted(
        set(raw) - {"pipeline_version", "name", "dataset", "matrix", "stages"}
    )
    if unsupported_top_level:
        raise ValueError(
            f"{path} contains unsupported fields: " + ", ".join(unsupported_top_level)
        )

    name = str(raw.get("name") or "").strip()
    if not name or not PIPELINE_NAME_RE.fullmatch(name):
        raise ValueError(
            f"{path}.name must use letters, numbers, underscores, or hyphens"
        )
    dataset_value = raw.get("dataset")
    dataset = str(dataset_value).strip() if dataset_value not in {None, ""} else None

    raw_matrix = raw.get("matrix") or {}
    if not isinstance(raw_matrix, dict):
        raise ValueError(f"{path}.matrix must be a mapping")
    matrix: dict[str, list[Any]] = {}
    for raw_key, raw_values in raw_matrix.items():
        key = str(raw_key).strip()
        if not ID_RE.fullmatch(key):
            raise ValueError(f"{path}.matrix contains invalid key {key!r}")
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError(f"{path}.matrix.{key} must be a non-empty list")
        matrix[key] = list(raw_values)

    raw_stages = raw.get("stages")
    if not isinstance(raw_stages, dict) or not raw_stages:
        raise ValueError(f"{path}.stages must be a non-empty mapping")
    stages: dict[str, dict[str, Any]] = {}
    common_fields = {"needs", "if", "scope", "timeout-minutes", "retention"}
    runner_fields = {"runner", "inputs", "with"}
    script_fields = {
        "image",
        "run",
        "access",
        "env",
        "mounts",
        "workdir",
    }
    for raw_stage_id, raw_stage in raw_stages.items():
        stage_id = str(raw_stage_id).strip()
        if not ID_RE.fullmatch(stage_id):
            raise ValueError(f"{path}.stages contains invalid id {stage_id!r}")
        if not isinstance(raw_stage, dict):
            raise ValueError(f"{path}.stages.{stage_id} must be a mapping")
        stage = dict(raw_stage)
        has_runner = bool(str(stage.get("runner") or "").strip())
        has_script = bool(str(stage.get("image") or "").strip()) or "run" in stage
        if has_runner == has_script:
            raise ValueError(
                f"{path}.stages.{stage_id} must define exactly one runner or image + run"
            )

        allowed = common_fields | (runner_fields if has_runner else script_fields)
        unsupported = sorted(set(stage) - allowed)
        if unsupported:
            raise ValueError(
                f"{path}.stages.{stage_id} contains unsupported fields: "
                + ", ".join(unsupported)
            )

        needs = _normalize_needs(path, stage_id, stage.get("needs"))
        condition = str(stage.get("if") or "success()").strip()
        if condition not in {"success()", "always()"}:
            raise ValueError(
                f"{path}.stages.{stage_id}.if must be success() or always()"
            )
        timeout = float(stage.get("timeout-minutes") or 60)
        if timeout <= 0:
            raise ValueError(
                f"{path}.stages.{stage_id}.timeout-minutes must be greater than 0"
            )

        normalized: dict[str, Any] = {
            **stage,
            "needs": needs,
            "if": condition,
            "timeout-minutes": timeout,
        }
        scope = str(stage.get("scope") or "matrix").strip()
        if scope not in {"pipeline", "matrix"}:
            raise ValueError(
                f"{path}.stages.{stage_id}.scope must be pipeline or matrix"
            )
        retention = str(stage.get("retention") or "keep").strip()
        if retention not in {"keep", "pipeline", "matrix", "none"}:
            raise ValueError(
                f"{path}.stages.{stage_id}.retention must be keep, pipeline, matrix, or none"
            )
        normalized["scope"] = scope
        normalized["retention"] = retention
        if has_runner:
            inputs = stage.get("inputs") or {}
            if not isinstance(inputs, dict):
                raise ValueError(f"{path}.stages.{stage_id}.inputs must be a mapping")
            invalid_roles = sorted(set(inputs) - {"data", "candidate", "references"})
            if invalid_roles:
                raise ValueError(
                    f"{path}.stages.{stage_id}.inputs contains unsupported roles: "
                    + ", ".join(invalid_roles)
                )
            for role, value in inputs.items():
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(
                        f"{path}.stages.{stage_id}.inputs.{role} must be one "
                        "dataset or stage-output selector"
                    )
                _validate_references(
                    value,
                    f"{path}.stages.{stage_id}.inputs.{role}",
                    pattern=INPUT_REFERENCE_RE,
                )
            parameters = stage.get("with") or {}
            if not isinstance(parameters, dict):
                raise ValueError(f"{path}.stages.{stage_id}.with must be a mapping")
            _validate_references(
                parameters,
                f"{path}.stages.{stage_id}.with",
                pattern=STATIC_REFERENCE_RE,
            )
            normalized["inputs"] = dict(inputs)
            normalized["with"] = dict(parameters)
        else:
            if not str(stage.get("image") or "").strip() or "run" not in stage:
                raise ValueError(
                    f"{path}.stages.{stage_id} requires both image and run"
                )
            run = stage["run"]
            if not isinstance(run, (str, list)) or not run:
                raise ValueError(
                    f"{path}.stages.{stage_id}.run must be a command string or list"
                )
            if isinstance(run, list) and not all(
                isinstance(item, (str, int, float, bool)) for item in run
            ):
                raise ValueError(
                    f"{path}.stages.{stage_id}.run list must contain scalar arguments"
                )
            environment = stage.get("env") or {}
            if not isinstance(environment, dict):
                raise ValueError(f"{path}.stages.{stage_id}.env must be a mapping")
            _validate_references(
                environment,
                f"{path}.stages.{stage_id}.env",
                pattern=STATIC_REFERENCE_RE,
            )
            normalized["env"] = dict(environment)
        stages[stage_id] = normalized

    _validate_dependency_graph(path, stages)
    for stage_id, stage in stages.items():
        declared_needs = set(stage_dependencies(stage))
        inputs = dict(stage.get("inputs") or {})
        for role, value in inputs.items():
            for reference in _references(value):
                match = STAGE_OUTPUT_REFERENCE_RE.fullmatch(reference)
                if match and match.group(1) not in declared_needs:
                    raise ValueError(
                        f"{path}.stages.{stage_id}.inputs.{role} references "
                        f"{match.group(1)!r}, which must be listed in needs"
                    )
                if (
                    match
                    and stages[match.group(1)]["retention"] == "none"
                ):
                    raise ValueError(
                        f"{path}.stages.{stage_id}.inputs.{role} references "
                        f"{match.group(1)!r}, which uses retention none"
                    )

    normalized_raw = {
        "pipeline_version": 1,
        "name": name,
        "dataset": dataset,
        "matrix": matrix,
        "stages": stages,
    }
    return PipelineDefinition(
        name=name,
        path=path.resolve(),
        dataset=dataset,
        matrix=matrix,
        stages=stages,
        raw=normalized_raw,
    )


def matrix_lanes(matrix: dict[str, list[Any]]) -> list[dict[str, Any]]:
    if not matrix:
        return [{}]
    keys = list(matrix)
    return [
        dict(zip(keys, values, strict=True))
        for values in itertools.product(*(matrix[key] for key in keys))
    ]


def _references(value: Any) -> list[str]:
    found: list[str] = []
    if isinstance(value, str) and value.lstrip().startswith("${{"):
        found.append(value)
    elif isinstance(value, list):
        for item in value:
            found.extend(_references(item))
    elif isinstance(value, dict):
        for item in value.values():
            found.extend(_references(item))
    return found


def stage_dependencies(stage: dict[str, Any]) -> list[str]:
    return list(stage.get("needs") or [])


def resolve_static_value(value: Any, *, dataset: str, lane: dict[str, Any]) -> Any:
    match = STATIC_REFERENCE_RE.fullmatch(value) if isinstance(value, str) else None
    if match:
        reference = match.group(1)
        if reference == "dataset":
            return dataset
        key = reference.removeprefix("matrix.")
        if key not in lane:
            raise ValueError(f"matrix value {key!r} is not defined")
        return lane[key]
    if isinstance(value, list):
        return [
            resolve_static_value(item, dataset=dataset, lane=lane) for item in value
        ]
    if isinstance(value, dict):
        return {
            key: resolve_static_value(item, dataset=dataset, lane=lane)
            for key, item in value.items()
        }
    return value

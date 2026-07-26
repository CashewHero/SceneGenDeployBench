from __future__ import annotations

import json
import re
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from app.config import OrchestratorConfig

CAMERA_POSE_DATA_TYPE = "camera_pose"
DEFAULT_CAMERA_POSITION = [0.0, 0.0, 0.0]
DEFAULT_CAMERA_ROTATION_QUATERNION_XYZW = [0.0, 0.0, 0.0, 1.0]

try:
    import yaml
except ModuleNotFoundError:
    yaml = None


MANIFEST_RESERVED_KEYS = {
    "metadata",
    "tags",
    "path",
    "samples",
    "subsets",
    "manifest",
    "path_prefix",
    "dataset_name",
    "dataset_version",
    "data_types",
    "external_key",
    "sample_id",
}


@dataclass(frozen=True)
class SampleRecord:
    sample_id: str
    dataset_name: str
    dataset_version: str
    external_key: str
    data: dict[str, Any]
    data_types: list[str]
    dataset_data_types: list[str]
    metadata: dict[str, Any]


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-") or "sample"


def _matches_patterns(relative_path: str, patterns: list[str]) -> bool:
    if not patterns:
        return True
    filename = Path(relative_path).name
    for pattern in patterns:
        normalized = pattern.strip()
        if normalized in {"*", "**/*"}:
            return True
        if fnmatch(relative_path, normalized) or fnmatch(filename, normalized):
            return True
    return False


def _is_dataset_manifest(relative_path: str) -> bool:
    name = Path(relative_path).name.lower()
    return name in {"manifest.yaml", "manifest.yml"}


def _normalize_string_list(value: Any, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, list):
        items = value
    else:
        raise ValueError(f"{field_name} must be a list or comma-separated string")
    return [str(item).strip() for item in items if str(item).strip()]


def _merge_string_lists(*values: list[str]) -> list[str]:
    merged: list[str] = []
    for value in values:
        for item in value:
            if item not in merged:
                merged.append(item)
    return merged


def _join_manifest_path(*parts: str) -> str:
    normalized = [part.strip("/") for part in parts if part and part.strip("/")]
    return "/".join(normalized)


def _deep_merge_manifest_nodes(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_manifest_nodes(merged[key], value)
        else:
            merged[key] = value
    return merged


def _resolve_data_path(dataset_root: Path, raw_path: str, data_type: str) -> tuple[Path, str]:
    path = Path(raw_path)
    if not path.is_absolute():
        path = dataset_root / path
    resolved = path.resolve()
    if not resolved.exists() or not resolved.is_file():
        raise FileNotFoundError(f"dataset file for {data_type} not found: {resolved}")
    try:
        relative = resolved.relative_to(dataset_root.resolve()).as_posix()
    except ValueError:
        relative = resolved.as_posix()
    return resolved, relative


def _load_dataset_manifest(dataset_root: Path) -> tuple[Path | None, dict[str, Any]]:
    for name in ("manifest.yaml", "manifest.yml"):
        manifest_path = dataset_root / name
        if not manifest_path.exists():
            continue
        return manifest_path, _read_manifest_file(manifest_path)
    return None, {}


def _read_mapping_file(path: Path, field_name: str) -> dict[str, Any]:
    suffix = path.suffix.lower()
    with path.open("r", encoding="utf-8") as handle:
        if suffix == ".json":
            loaded = json.load(handle) or {}
        else:
            if yaml is None:
                raise RuntimeError("PyYAML is required to read YAML dataset files")
            loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"{field_name} must contain a mapping: {path}")
    return loaded


def _read_manifest_file(manifest_path: Path) -> dict[str, Any]:
    return _read_mapping_file(manifest_path, "dataset manifest")


def _normalize_number_list(value: Any, field_name: str, length: int) -> list[float]:
    if not isinstance(value, list) or len(value) != length:
        raise ValueError(f"{field_name} must be a list of {length} numbers")

    normalized: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise ValueError(f"{field_name} must be a list of {length} numbers")
        normalized.append(float(item))
    return normalized


def _normalize_camera_pose(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping or a path to a YAML/JSON mapping")

    position = value.get("position", DEFAULT_CAMERA_POSITION)
    if position is None:
        position = DEFAULT_CAMERA_POSITION
    rotation = value.get("rotation_quaternion_xyzw", DEFAULT_CAMERA_ROTATION_QUATERNION_XYZW)
    if rotation is None:
        rotation = DEFAULT_CAMERA_ROTATION_QUATERNION_XYZW

    return {
        "position": _normalize_number_list(position, f"{field_name}.position", 3),
        "rotation_quaternion_xyzw": _normalize_number_list(
            rotation,
            f"{field_name}.rotation_quaternion_xyzw",
            4,
        ),
    }


def _read_camera_pose_file(path: Path, field_name: str) -> dict[str, Any]:
    return _normalize_camera_pose(_read_mapping_file(path, field_name), field_name)


def _is_reserved_manifest_key(key: Any) -> bool:
    normalized = str(key).strip()
    return normalized.startswith("_") or normalized in MANIFEST_RESERVED_KEYS


def _extract_manifest_data(sample_payload: dict[str, Any]) -> dict[str, Any]:
    raw_data = {
        key: value for key, value in sample_payload.items() if not _is_reserved_manifest_key(key)
    }
    if not isinstance(raw_data, dict) or not raw_data:
        raise ValueError("each manifest sample must declare at least one data path")

    data: dict[str, Any] = {}
    for data_type, raw_value in raw_data.items():
        data_key = str(data_type).strip()
        if not data_key:
            raise ValueError("sample data type names must be non-empty")
        if data_key == CAMERA_POSE_DATA_TYPE and isinstance(raw_value, dict):
            data[data_key] = dict(raw_value)
            continue
        if not isinstance(raw_value, str) or not raw_value.strip():
            raise ValueError(f"sample data path for {data_key} must be a non-empty string")
        data[data_key] = raw_value.strip()
    return data


def _normalize_manifest_samples(raw_samples: Any, field_name: str) -> list[dict[str, Any]]:
    if raw_samples is None:
        return []
    if isinstance(raw_samples, list):
        normalized: list[dict[str, Any]] = []
        for index, raw_sample in enumerate(raw_samples, start=1):
            if not isinstance(raw_sample, dict):
                raise ValueError(f"{field_name}[{index}] must be a mapping")
            normalized.append(dict(raw_sample))
        return normalized
    if isinstance(raw_samples, dict):
        normalized = []
        for sample_id, raw_sample in raw_samples.items():
            if isinstance(raw_sample, str):
                sample_payload: dict[str, Any] = {"image": raw_sample}
            elif isinstance(raw_sample, dict):
                sample_payload = dict(raw_sample)
            else:
                raise ValueError(f"{field_name}.{sample_id} must be a mapping or string")
            sample_payload.setdefault("sample_id", str(sample_id))
            normalized.append(sample_payload)
        return normalized
    raise ValueError(f"{field_name} must be a list or mapping")


def _normalize_manifest_subsets(raw_subsets: Any, field_name: str) -> list[tuple[str, dict[str, Any]]]:
    if raw_subsets is None:
        return []
    if not isinstance(raw_subsets, dict):
        raise ValueError(f"{field_name} must be a mapping")

    normalized: list[tuple[str, dict[str, Any]]] = []
    for subset_name, subset_payload in raw_subsets.items():
        if isinstance(subset_payload, str):
            normalized.append((str(subset_name), {"manifest": subset_payload.strip()}))
            continue
        if not isinstance(subset_payload, dict):
            raise ValueError(f"{field_name}.{subset_name} must be a mapping or manifest path")
        normalized.append((str(subset_name), dict(subset_payload)))
    return normalized


def _merge_metadata(base: dict[str, Any], override: dict[str, Any], field_name: str) -> dict[str, Any]:
    if not isinstance(override, dict):
        raise ValueError(f"{field_name} must be a mapping")
    merged = dict(base)
    merged.update(override)
    return merged


def _collect_manifest_samples(
    *,
    dataset_root: Path,
    dataset_name: str,
    dataset_version: str,
    node: dict[str, Any],
    node_name: str | None,
    manifest_dir: Path,
    path_prefix: str,
    subset_path: list[str],
    inherited_tags: list[str],
    inherited_metadata: dict[str, Any],
    field_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    current_dataset_version = str(node.get("dataset_version", dataset_version))
    node_path_prefix = _join_manifest_path(path_prefix, str(node.get("path_prefix", "")))
    node_tags = _merge_string_lists(
        inherited_tags,
        _normalize_string_list(node.get("tags"), f"{field_name}.tags"),
    )
    node_metadata = _merge_metadata(
        inherited_metadata,
        node.get("metadata") or {},
        f"{field_name}.metadata",
    )

    if node_name is not None:
        current_subset_path = [*subset_path, node_name]
    else:
        current_subset_path = list(subset_path)

    collected_samples: list[dict[str, Any]] = []
    inferred_data_types: list[str] = []

    for index, raw_sample in enumerate(
        _normalize_manifest_samples(node.get("samples"), f"{field_name}.samples"),
        start=1,
    ):
        data_mapping = _extract_manifest_data(raw_sample)
        resolved_data: dict[str, Any] = {}
        relative_paths: list[str] = []
        for data_type, raw_value in data_mapping.items():
            value_field_name = f"{field_name}.samples[{index}].{data_type}"
            if data_type == CAMERA_POSE_DATA_TYPE and isinstance(raw_value, dict):
                resolved_data[data_type] = _normalize_camera_pose(raw_value, value_field_name)
                if data_type not in inferred_data_types:
                    inferred_data_types.append(data_type)
                continue

            raw_path = str(raw_value)
            candidate_path = raw_path
            if not Path(raw_path).is_absolute():
                candidate_path = str((manifest_dir / node_path_prefix / raw_path).resolve())
            resolved, relative_path = _resolve_data_path(dataset_root, candidate_path, data_type)
            resolved_data[data_type] = (
                _read_camera_pose_file(resolved, value_field_name)
                if data_type == CAMERA_POSE_DATA_TYPE
                else str(resolved)
            )
            relative_paths.append(relative_path)
            if data_type not in inferred_data_types:
                inferred_data_types.append(data_type)

        declared_sample_data_types = _normalize_string_list(
            raw_sample.get("data_types"),
            f"{field_name}.samples[{index}].data_types",
        )
        sample_data_types = list(resolved_data.keys())
        if declared_sample_data_types and set(declared_sample_data_types) != set(sample_data_types):
            joined = ", ".join(sample_data_types)
            raise ValueError(
                f"{field_name}.samples[{index}].data_types must match declared data keys: {joined}"
            )

        fallback_sample_id = relative_paths[0] if relative_paths else f"sample-{index}"
        sample_id = str(raw_sample.get("sample_id") or raw_sample.get("external_key") or fallback_sample_id)
        default_external_key = _join_manifest_path("/".join(current_subset_path), sample_id) or fallback_sample_id
        sample_metadata = _merge_metadata(
            node_metadata,
            raw_sample.get("metadata") or {},
            f"{field_name}.samples[{index}].metadata",
        )
        sample_tags = _merge_string_lists(
            node_tags,
            _normalize_string_list(raw_sample.get("tags"), f"{field_name}.samples[{index}].tags"),
        )
        if sample_tags:
            sample_metadata["tags"] = sample_tags
        if current_subset_path:
            sample_metadata["subset_path"] = current_subset_path
            sample_metadata["subset_key"] = "/".join(current_subset_path)

        collected_samples.append(
            {
                "sample_id": sample_id,
                "dataset_version": current_dataset_version,
                "external_key": str(raw_sample.get("external_key") or default_external_key),
                "data": resolved_data,
                "data_types": sample_data_types,
                "metadata": sample_metadata,
            }
        )

    for subset_name, subset_payload in _normalize_manifest_subsets(node.get("subsets"), f"{field_name}.subsets"):
        child_manifest_dir = manifest_dir
        child_node = dict(subset_payload)
        manifest_ref = child_node.pop("manifest", None)
        if manifest_ref is not None:
            if not isinstance(manifest_ref, str) or not manifest_ref.strip():
                raise ValueError(f"{field_name}.subsets.{subset_name}.manifest must be a non-empty string")
            child_manifest_path = (manifest_dir / manifest_ref).resolve()
            child_manifest_dir = child_manifest_path.parent
            child_node = _deep_merge_manifest_nodes(_read_manifest_file(child_manifest_path), child_node)

        subset_samples, subset_data_types = _collect_manifest_samples(
            dataset_root=dataset_root,
            dataset_name=dataset_name,
            dataset_version=current_dataset_version,
            node=child_node,
            node_name=subset_name,
            manifest_dir=child_manifest_dir,
            path_prefix=node_path_prefix,
            subset_path=current_subset_path,
            inherited_tags=node_tags,
            inherited_metadata=node_metadata,
            field_name=f"{field_name}.subsets.{subset_name}",
        )
        collected_samples.extend(subset_samples)
        inferred_data_types = _merge_string_lists(inferred_data_types, subset_data_types)

    return collected_samples, inferred_data_types


def _discover_manifest_samples(
    dataset_root: Path,
    dataset_name: str,
    sample_limit: int,
    manifest: dict[str, Any],
) -> list[SampleRecord]:
    declared_data_types = _normalize_string_list(
        manifest.get("data_types") or manifest.get("data_type") or manifest.get("available_data_types"),
        "manifest.data_types",
    )
    sample_rows, inferred_data_types = _collect_manifest_samples(
        dataset_root=dataset_root,
        dataset_name=dataset_name,
        dataset_version=str(manifest.get("dataset_version", "unversioned")),
        node=manifest,
        node_name=None,
        manifest_dir=dataset_root,
        path_prefix="",
        subset_path=[],
        inherited_tags=[],
        inherited_metadata={},
        field_name="manifest",
    )
    if not sample_rows:
        raise ValueError("dataset manifest must contain at least one sample")
    sample_rows = sample_rows[:sample_limit]

    dataset_data_types = declared_data_types or inferred_data_types

    return [
        SampleRecord(
            sample_id=row["sample_id"],
            dataset_name=dataset_name,
            dataset_version=row["dataset_version"],
            external_key=row["external_key"],
            data=row["data"],
            data_types=row["data_types"],
            dataset_data_types=dataset_data_types,
            metadata=row["metadata"],
        )
        for row in sample_rows
    ]


def discover_samples(
    config: OrchestratorConfig,
    dataset_name: str | None = None,
    max_samples: int | None = None,
    include_patterns: list[str] | None = None,
) -> list[SampleRecord]:
    if not dataset_name:
        raise ValueError("dataset_name is required")
    sample_limit = max_samples if max_samples is not None else 1000000
    patterns = include_patterns or []
    dataset_root = config.storage.dataset_root / dataset_name
    if not dataset_root.exists():
        raise FileNotFoundError(f"dataset directory not found: {dataset_root}")
    manifest_path, manifest = _load_dataset_manifest(dataset_root)
    if manifest_path is not None:
        return _discover_manifest_samples(dataset_root, dataset_name, sample_limit, manifest)

    files = []
    for path in sorted(dataset_root.rglob("*")):
        if not path.is_file():
            continue
        if any(part.startswith(".") for part in path.relative_to(dataset_root).parts):
            continue
        relative_path = path.relative_to(dataset_root).as_posix()
        if _is_dataset_manifest(relative_path):
            continue
        if not _matches_patterns(relative_path, patterns):
            continue
        files.append((path, relative_path))

    if not files:
        raise FileNotFoundError(f"no input files found under dataset: {dataset_root}")

    samples: list[SampleRecord] = []
    for index, (path, relative_path) in enumerate(files[:sample_limit], start=1):
        suffix = path.suffix.lower().lstrip(".") or "bin"
        slug = _slugify(relative_path)
        samples.append(
            SampleRecord(
                sample_id=f"{dataset_name}-{index:03d}-{slug[:40]}",
                dataset_name=dataset_name,
                dataset_version="unversioned",
                external_key=relative_path,
                data={"image": str(path.resolve())},
                data_types=["image"],
                dataset_data_types=["image"],
                metadata={
                    "source_relpath": relative_path,
                    "size_bytes": path.stat().st_size,
                    "source_format": suffix,
                },
            )
        )
    return samples

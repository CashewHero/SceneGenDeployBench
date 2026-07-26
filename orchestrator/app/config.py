from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from domain.scheduling import normalize_allowed_windows

try:
    import yaml
except ModuleNotFoundError:
    yaml = None


DEFAULT_SYSTEM_CONFIG: dict[str, Any] = {
    "config_version": 1,
    "storage": {
        "dataset_root": "/data/datasets",
        "model_cache_root": "/data/model_cache",
        "output_root": "/data/output",
        "pipeline_root": "/data/pipelines",
    },
    "catalogs": {
        "runners": "runners",
        "pipelines": "pipelines",
    },
    "database": {
        "host": "127.0.0.1",
        "port": 5432,
        "name": "scenegendeploybench",
        "user": "postgres",
        "password": "",
    },
    "orchestrator": {
        "runner_env": {},
        "polling": {
            "startup_seconds": 1.0,
            "post_submit_seconds": 1.0,
            "running_seconds": 2.0,
        },
        "scheduling": {
            "max_attempts": 1,
            "job_timeout_minutes": 60,
            "startup_timeout_minutes": 1.0,
        },
    },
}

_VERSION_TOKEN_RE = re.compile(r"\d+|[A-Za-z]+")
_ENV_INTERPOLATION_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")
SUPPORTED_CONFIG_VERSION = 1
SUPPORTED_CONTRACT_VERSION = 1
SUPPORTED_LAUNCHER_COMPAT_VERSION = 1
SUPPORTED_LAUNCHER_DRIVERS = {"docker", "static_http"}
SUPPORTED_RUNNER_KINDS = {"dataset_downloader", "evaluator", "generator"}


@dataclass(frozen=True)
class StorageConfig:
    dataset_root: Path
    model_cache_root: Path
    output_root: Path
    pipeline_root: Path


@dataclass(frozen=True)
class CatalogConfig:
    runners: Path
    pipelines: Path


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    password: str


@dataclass(frozen=True)
class PollingConfig:
    startup_seconds: float
    post_submit_seconds: float
    running_seconds: float


@dataclass(frozen=True)
class OrchestratorSettings:
    polling: PollingConfig
    runner_env: dict[str, str] = field(default_factory=dict)
    scheduling: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RunnerDefinition:
    selector: str
    runner: str
    version: str
    latest: bool
    display_name: str
    kind: str
    contract_version: int
    inputs: dict[str, dict[str, dict[str, list[str]]]]
    job_parameters: dict[str, Any] = field(default_factory=dict)
    scheduling: dict[str, Any] = field(default_factory=dict)
    launcher: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class OrchestratorConfig:
    config_version: int
    storage: StorageConfig
    catalogs: CatalogConfig
    database: DatabaseConfig
    orchestrator: OrchestratorSettings
    runners: dict[str, RunnerDefinition]
    raw: dict[str, Any] = field(default_factory=dict)
    runners_by_name: dict[str, tuple[RunnerDefinition, ...]] = field(default_factory=dict)
    latest_runners: dict[str, str] = field(default_factory=dict)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged[key] = _deep_merge(base[key], value)
        else:
            merged[key] = value
    return merged


def _read_yaml(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {}
    if yaml is None:
        raise RuntimeError("PyYAML is required to read a YAML config file")
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        raise ValueError(f"config file must contain a mapping: {path}")
    return _interpolate_env_values(loaded)


def _interpolate_env_values(value: Any) -> Any:
    if isinstance(value, str):
        return _ENV_INTERPOLATION_RE.sub(
            lambda match: os.getenv(match.group(1), match.group(2) or ""),
            value,
        )
    if isinstance(value, list):
        return [_interpolate_env_values(item) for item in value]
    if isinstance(value, dict):
        return {key: _interpolate_env_values(item) for key, item in value.items()}
    return value


def _env_override(name: str, caster, default: Any = None) -> Any:
    value = os.getenv(name)
    if value is None or value == "":
        return default
    return caster(value)


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


def _normalize_mapping(value: Any, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return dict(value)


def _normalize_env_mapping(value: Any, field_name: str) -> dict[str, str]:
    env = _normalize_mapping(value, field_name)
    normalized: dict[str, str] = {}
    for key, raw_value in env.items():
        env_key = str(key).strip()
        if not env_key:
            raise ValueError(f"{field_name} contains an empty key")
        if raw_value is None:
            continue
        env_value = str(raw_value)
        if env_value:
            normalized[env_key] = env_value
    return normalized


def _redact_env_mapping(env: dict[str, str]) -> dict[str, str]:
    redacted: dict[str, str] = {}
    for key, value in env.items():
        lowered = key.lower()
        if any(token in lowered for token in ("token", "secret", "password", "passwd", "key")):
            redacted[key] = "***" if value else ""
        else:
            redacted[key] = value
    return redacted


def _normalize_scheduling(value: Any, field_name: str) -> dict[str, Any]:
    scheduling = _normalize_mapping(value, field_name)
    normalized = dict(scheduling)
    max_batch_size = normalized.get("max_batch_size")
    if max_batch_size is not None:
        max_batch_size = int(max_batch_size)
        if max_batch_size <= 0:
            raise ValueError(f"{field_name}.max_batch_size must be greater than 0")
        normalized["max_batch_size"] = max_batch_size
    max_attempts = normalized.get("max_attempts")
    if max_attempts is not None:
        max_attempts = int(max_attempts)
        if max_attempts <= 0:
            raise ValueError(f"{field_name}.max_attempts must be greater than 0")
        normalized["max_attempts"] = max_attempts
    job_timeout_minutes = normalized.get("job_timeout_minutes")
    if job_timeout_minutes is not None:
        job_timeout_minutes = float(job_timeout_minutes)
        if job_timeout_minutes <= 0:
            raise ValueError(f"{field_name}.job_timeout_minutes must be greater than 0")
        normalized["job_timeout_minutes"] = job_timeout_minutes
    startup_timeout_minutes = normalized.get("startup_timeout_minutes")
    if startup_timeout_minutes is not None:
        startup_timeout_minutes = float(startup_timeout_minutes)
        if startup_timeout_minutes <= 0:
            raise ValueError(f"{field_name}.startup_timeout_minutes must be greater than 0")
        normalized["startup_timeout_minutes"] = startup_timeout_minutes
    allowed_windows = normalize_allowed_windows(normalized.get("allowed_windows"), field_name)
    if allowed_windows:
        normalized["allowed_windows"] = allowed_windows
    elif "allowed_windows" in normalized:
        normalized["allowed_windows"] = []
    return normalized


def _normalize_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "yes", "1", "on"}:
            return True
        if normalized in {"false", "no", "0", "off", ""}:
            return False
    raise ValueError(f"{field_name} must be a boolean")


def _version_tokens(value: str) -> tuple[tuple[int, int | str], ...]:
    tokens = _VERSION_TOKEN_RE.findall(value)
    return tuple((0, int(token)) if token.isdigit() else (1, token.lower()) for token in tokens)


def runner_version_sort_key(version: str) -> tuple[tuple[tuple[int, int | str], ...], int, tuple[tuple[int, int | str], ...]]:
    normalized = version.strip()
    release, _, build = normalized.partition("+")
    core, has_prerelease, prerelease = release.partition("-")
    return (
        _version_tokens(core),
        1 if not has_prerelease else 0,
        _version_tokens(prerelease),
    )


def _resolve_path(path_value: str, *, base_dir: Path) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = base_dir / path
    return path.resolve()


def _env_config() -> dict[str, Any]:
    config: dict[str, Any] = {
        "storage": {},
        "catalogs": {},
        "database": {},
        "orchestrator": {"polling": {}, "scheduling": {}, "runner_env": {}},
    }

    dataset_root = _env_override("PATH_DATASETS", str)
    model_cache_root = _env_override("PATH_MODEL_CACHE", str)
    output_root = _env_override("PATH_OUTPUT", str)
    pipeline_root = _env_override("PATH_PIPELINES", str)
    db_host = _env_override("PG_DB_HOST", str)
    db_port = _env_override("PG_DB_PORT", int)
    db_name = _env_override("PG_DB_NAME", str)
    db_user = _env_override("PG_DB_USER", str)
    db_password = _env_override("PG_DB_PASSWORD", str)
    if dataset_root:
        config["storage"]["dataset_root"] = dataset_root
    if model_cache_root:
        config["storage"]["model_cache_root"] = model_cache_root
    if output_root:
        config["storage"]["output_root"] = output_root
    if pipeline_root:
        config["storage"]["pipeline_root"] = pipeline_root
    if db_host:
        config["database"]["host"] = db_host
    if db_port is not None:
        config["database"]["port"] = db_port
    if db_name:
        config["database"]["name"] = db_name
    if db_user:
        config["database"]["user"] = db_user
    if db_password is not None:
        config["database"]["password"] = db_password
    startup_seconds = _env_override("POLL_STARTUP_SECONDS", float)
    post_submit_seconds = _env_override("POLL_POST_SUBMIT_SECONDS", float)
    running_seconds = _env_override("POLL_RUNNING_SECONDS", float)
    max_attempts = _env_override("ORCH_SCHEDULING_MAX_ATTEMPTS", int)
    if startup_seconds is not None:
        config["orchestrator"]["polling"]["startup_seconds"] = startup_seconds
    if post_submit_seconds is not None:
        config["orchestrator"]["polling"]["post_submit_seconds"] = post_submit_seconds
    if running_seconds is not None:
        config["orchestrator"]["polling"]["running_seconds"] = running_seconds
    if max_attempts is not None:
        config["orchestrator"]["scheduling"]["max_attempts"] = max_attempts
    return {
        key: value
        for key, value in config.items()
        if value and (key != "orchestrator" or any(section for section in value.values()))
    }


def _runner_file_paths(runner_catalog: Path) -> list[Path]:
    if not runner_catalog.exists():
        raise FileNotFoundError(f"runner catalog directory not found: {runner_catalog}")
    if not runner_catalog.is_dir():
        raise NotADirectoryError(f"runner catalog path is not a directory: {runner_catalog}")
    return sorted(
        path
        for path in runner_catalog.rglob("*")
        if path.is_file() and path.suffix.lower() in {".yaml", ".yml"}
    )


def _normalize_runner_entry(entry: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return dict(entry)


def _normalize_runner_inputs(
    value: Any,
    field_name: str,
) -> dict[str, dict[str, dict[str, list[str]]]]:
    inputs = _normalize_mapping(value, field_name)
    supported_roles = {"data", "candidate", "references"}
    unsupported_roles = sorted(set(inputs) - supported_roles)
    if unsupported_roles:
        raise ValueError(
            f"{field_name} contains unsupported roles: {', '.join(unsupported_roles)}"
        )
    normalized: dict[str, dict[str, dict[str, list[str]]]] = {}
    for source in ("data", "candidate", "references"):
        source_inputs = _normalize_mapping(inputs.get(source), f"{field_name}.{source}")
        unsupported_source_keys = sorted(
            set(source_inputs) - {"required_sample", "optional_sample"}
        )
        if unsupported_source_keys:
            raise ValueError(
                f"{field_name}.{source} contains unsupported fields: "
                f"{', '.join(unsupported_source_keys)}"
            )
        normalized[source] = {}
        for sample_requirement in ("required_sample", "optional_sample"):
            sample_inputs = _normalize_mapping(
                source_inputs.get(sample_requirement),
                f"{field_name}.{source}.{sample_requirement}",
            )
            unsupported_sample_keys = sorted(
                set(sample_inputs) - {"required_datatype", "optional_datatype"}
            )
            if unsupported_sample_keys:
                raise ValueError(
                    f"{field_name}.{source}.{sample_requirement} contains unsupported fields: "
                    f"{', '.join(unsupported_sample_keys)}"
                )
            normalized[source][sample_requirement] = {
                "required_datatype": _normalize_string_list(
                    sample_inputs.get("required_datatype"),
                    f"{field_name}.{source}.{sample_requirement}.required_datatype",
                ),
                "optional_datatype": _normalize_string_list(
                    sample_inputs.get("optional_datatype"),
                    f"{field_name}.{source}.{sample_requirement}.optional_datatype",
                ),
            }
    return normalized


def _load_runner_definitions(
    runner_catalog: Path, *, scheduling_defaults: dict[str, Any]
) -> dict[str, RunnerDefinition]:
    runners: dict[str, RunnerDefinition] = {}
    latest_selectors: dict[str, str] = {}
    for catalog_path in _runner_file_paths(runner_catalog):
        loaded = _read_yaml(catalog_path)
        catalog_version = int(loaded.get("catalog_version", 1))
        if catalog_version != 1:
            raise ValueError(
                f"runner catalog {catalog_path} uses unsupported catalog_version {catalog_version}"
            )
        entries = loaded.get("runners")
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"runner catalog {catalog_path} must declare a non-empty runners list")

        for index, raw_entry in enumerate(entries, start=1):
            entry = _normalize_runner_entry(raw_entry, f"{catalog_path}.runners[{index}]")
            runner_name = str(entry.get("runner", "")).strip()
            version = str(entry.get("version", "")).strip()
            latest = _normalize_bool(
                entry.get("latest", False),
                f"{catalog_path}.runners[{index}].latest",
            )
            display_name = str(entry.get("display_name", "")).strip()
            if not runner_name:
                raise ValueError(f"{catalog_path}.runners[{index}].runner is required")
            if not version:
                raise ValueError(f"{catalog_path}.runners[{index}].version is required")
            if not display_name:
                raise ValueError(f"{catalog_path}.runners[{index}].display_name is required")

            selector = f"{runner_name}@{version}"
            if selector in runners:
                raise ValueError(f"duplicate runner selector {selector!r} in {catalog_path}")
            if latest and runner_name in latest_selectors:
                raise ValueError(
                    f"multiple runners flagged as latest for {runner_name!r}: "
                    f"{latest_selectors[runner_name]}, {selector}"
                )
            if latest:
                latest_selectors[runner_name] = selector

            inputs = _normalize_runner_inputs(
                entry.get("inputs"),
                f"{catalog_path}.runners[{index}].inputs",
            )
            scheduling = _normalize_scheduling(
                entry.get("scheduling"),
                f"{catalog_path}.runners[{index}].scheduling",
            )
            launcher = _normalize_mapping(
                entry.get("launcher"), f"{catalog_path}.runners[{index}].launcher"
            )
            job_parameters = _normalize_mapping(
                entry.get("job_parameters"),
                f"{catalog_path}.runners[{index}].job_parameters",
            )
            driver = str(launcher.get("driver", "")).strip()
            compat_version = launcher.get("compat_version")
            if not driver:
                raise ValueError(f"{catalog_path}.runners[{index}].launcher.driver is required")
            if driver not in SUPPORTED_LAUNCHER_DRIVERS:
                raise ValueError(
                    f"{catalog_path}.runners[{index}].launcher.driver uses "
                    f"unsupported value {driver!r}"
                )
            if compat_version is None:
                raise ValueError(
                    f"{catalog_path}.runners[{index}].launcher.compat_version is required"
                )
            if int(compat_version) != SUPPORTED_LAUNCHER_COMPAT_VERSION:
                raise ValueError(
                    f"{catalog_path}.runners[{index}].launcher.compat_version "
                    f"must be {SUPPORTED_LAUNCHER_COMPAT_VERSION}"
                )
            kind = str(entry.get("kind", "")).strip()
            if kind not in SUPPORTED_RUNNER_KINDS:
                raise ValueError(
                    f"{catalog_path}.runners[{index}].kind uses unsupported "
                    f"value {kind!r}"
                )
            contract_version = int(entry.get("contract_version", 1))
            if contract_version != SUPPORTED_CONTRACT_VERSION:
                raise ValueError(
                    f"{catalog_path}.runners[{index}].contract_version must be "
                    f"{SUPPORTED_CONTRACT_VERSION}"
                )
            effective_scheduling = (
                _deep_merge(scheduling_defaults, scheduling)
                if scheduling_defaults or scheduling
                else {}
            )
            raw = dict(entry)
            if effective_scheduling:
                raw["scheduling"] = effective_scheduling

            runners[selector] = RunnerDefinition(
                selector=selector,
                runner=runner_name,
                version=version,
                latest=latest,
                display_name=display_name,
                kind=kind,
                contract_version=contract_version,
                inputs=inputs,
                job_parameters=job_parameters,
                scheduling=effective_scheduling,
                launcher=launcher,
                raw=raw,
            )
    if not runners:
        raise ValueError(
            f"runner catalog must contain at least one YAML file: {runner_catalog}"
        )
    return runners


def _index_runners(
    runners: dict[str, RunnerDefinition],
) -> tuple[dict[str, tuple[RunnerDefinition, ...]], dict[str, str]]:
    runners_by_name: dict[str, list[RunnerDefinition]] = {}
    latest_runners: dict[str, str] = {}
    for runner in runners.values():
        runners_by_name.setdefault(runner.runner, []).append(runner)
        if runner.latest:
            latest_runners[runner.runner] = runner.selector
    ordered = {
        runner_name: tuple(
            sorted(
                definitions,
                key=lambda item: runner_version_sort_key(item.version),
                reverse=True,
            )
        )
        for runner_name, definitions in runners_by_name.items()
    }
    return ordered, latest_runners


def load_config(config_path: str | None = None) -> OrchestratorConfig:
    if config_path is None:
        config_path = os.getenv("PATH_CONFIG_SYSTEM")
    path_obj = Path(config_path).resolve() if config_path else None
    config_dir = path_obj.parent if path_obj is not None else Path.cwd()

    merged = _deep_merge(DEFAULT_SYSTEM_CONFIG, _read_yaml(path_obj))
    merged = _deep_merge(merged, _env_config())
    config_version = int(merged.get("config_version", 1))
    if config_version != SUPPORTED_CONFIG_VERSION:
        raise ValueError(
            f"unsupported config_version {config_version}; "
            f"expected {SUPPORTED_CONFIG_VERSION}"
        )

    storage_root = _normalize_mapping(merged.get("storage"), "storage")
    catalogs_root = _normalize_mapping(merged.get("catalogs"), "catalogs")
    database_root = _normalize_mapping(merged.get("database"), "database")
    orchestrator_root = _normalize_mapping(merged.get("orchestrator"), "orchestrator")
    polling_root = _normalize_mapping(orchestrator_root.get("polling"), "orchestrator.polling")
    runner_env_root = _normalize_env_mapping(
        orchestrator_root.get("runner_env"),
        "orchestrator.runner_env",
    )
    scheduling_root = _normalize_scheduling(
        orchestrator_root.get("scheduling"),
        "orchestrator.scheduling",
    )

    runner_catalog = _resolve_path(
        str(catalogs_root.get("runners", "runners")),
        base_dir=config_dir,
    )
    pipeline_catalog = _resolve_path(
        str(catalogs_root.get("pipelines", "pipelines")),
        base_dir=config_dir,
    )
    runners = _load_runner_definitions(
        runner_catalog,
        scheduling_defaults=scheduling_root,
    )
    runners_by_name, latest_runners = _index_runners(runners)

    raw = dict(merged)
    raw["storage"] = {
        "dataset_root": str(
            _resolve_path(str(storage_root["dataset_root"]), base_dir=config_dir)
        ),
        "model_cache_root": str(_resolve_path(str(storage_root["model_cache_root"]), base_dir=config_dir)),
        "output_root": str(_resolve_path(str(storage_root["output_root"]), base_dir=config_dir)),
        "pipeline_root": str(
            _resolve_path(str(storage_root["pipeline_root"]), base_dir=config_dir)
        ),
    }
    raw["catalogs"] = dict(catalogs_root)
    raw["catalogs"]["runners"] = str(runner_catalog)
    raw["catalogs"]["pipelines"] = str(pipeline_catalog)
    raw["database"] = {
        "host": str(database_root["host"]),
        "port": int(database_root["port"]),
        "name": str(database_root["name"]),
        "user": str(database_root["user"]),
        "password": "***" if str(database_root.get("password", "")) else "",
    }
    raw["orchestrator"] = {
        "polling": dict(polling_root),
    }
    if runner_env_root:
        raw["orchestrator"]["runner_env"] = _redact_env_mapping(runner_env_root)
    if scheduling_root:
        raw["orchestrator"]["scheduling"] = dict(scheduling_root)
    raw["runners"] = {selector: runner.raw for selector, runner in runners.items()}

    return OrchestratorConfig(
        config_version=config_version,
        storage=StorageConfig(
            dataset_root=_resolve_path(
                str(storage_root["dataset_root"]),
                base_dir=config_dir,
            ),
            model_cache_root=_resolve_path(str(storage_root["model_cache_root"]), base_dir=config_dir),
            output_root=_resolve_path(str(storage_root["output_root"]), base_dir=config_dir),
            pipeline_root=_resolve_path(
                str(storage_root["pipeline_root"]),
                base_dir=config_dir,
            ),
        ),
        catalogs=CatalogConfig(
            runners=runner_catalog,
            pipelines=pipeline_catalog,
        ),
        database=DatabaseConfig(
            host=str(database_root["host"]),
            port=int(database_root["port"]),
            name=str(database_root["name"]),
            user=str(database_root["user"]),
            password=str(database_root.get("password", "")),
        ),
        orchestrator=OrchestratorSettings(
            polling=PollingConfig(
                startup_seconds=float(polling_root["startup_seconds"]),
                post_submit_seconds=float(polling_root["post_submit_seconds"]),
                running_seconds=float(polling_root["running_seconds"]),
            ),
            runner_env=runner_env_root,
            scheduling=scheduling_root,
        ),
        runners=runners,
        runners_by_name=runners_by_name,
        latest_runners=latest_runners,
        raw=raw,
    )

from __future__ import annotations

import json
import logging
import os
import shutil
import struct
import sys
import tempfile
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

from app.config import OrchestratorConfig
from runner_launchers.docker import (
    _DockerEngineClient,
    _UnixSocketHTTPConnection,
    inspect_current_container,
    mount_source_for_destination,
    mount_source_for_path,
)


ACCESS_NAMES = {
    "datasets",
    "output",
    "pipelines",
    "model-cache",
    "database",
}
logger = logging.getLogger("scenegendeploybench.orchestrator.script_run")


@dataclass(frozen=True)
class ScriptRunResult:
    exit_code: int
    result: dict[str, Any]


def normalize_access(values: list[str] | None) -> set[str]:
    requested = {
        item.strip()
        for value in values or []
        for item in value.split(",")
        if item.strip()
    }
    unsupported = sorted(requested - ACCESS_NAMES - {"all"})
    if unsupported:
        raise ValueError(
            "unsupported --access value(s): "
            + ", ".join(unsupported)
            + "; choose datasets, output, pipelines, model-cache, database, or all"
        )
    if "all" in requested:
        return set(ACCESS_NAMES)
    return requested


def parse_environment(values: list[str] | None) -> dict[str, str]:
    environment: dict[str, str] = {}
    for value in values or []:
        key, separator, raw_value = value.partition("=")
        key = key.strip()
        if not separator or not key:
            raise ValueError("--env values must use KEY=VALUE")
        environment[key] = raw_value
    return environment


def _database_environment(config: OrchestratorConfig) -> dict[str, str]:
    database = config.database
    user = quote(database.user, safe="")
    password = quote(database.password, safe="")
    auth = user if not password else f"{user}:{password}"
    database_name = quote(database.name, safe="")
    return {
        "DEPLOYBENCH_DATABASE_URL": (
            f"postgresql://{auth}@{database.host}:{database.port}/{database_name}"
        ),
        "PGHOST": database.host,
        "PGPORT": str(database.port),
        "PGDATABASE": database.name,
        "PGUSER": database.user,
        "PGPASSWORD": database.password,
    }


def _container_user() -> str:
    uid = os.getenv("UID", "").strip()
    gid = os.getenv("GID", "").strip()
    if not uid and not gid:
        return ""
    if not uid or not gid or not uid.isdigit() or not gid.isdigit():
        raise ValueError("UID and GID must be set together as numeric values")
    return f"{uid}:{gid}"


def _script_command(script: Path, arguments: list[str]) -> list[str]:
    target = f"/workspace/{script.name}"
    if script.suffix.lower() == ".sh":
        return ["sh", target, *arguments]
    if script.suffix.lower() == ".py":
        return ["python", target, *arguments]
    return [target, *arguments]


def _write_output(data: bytes) -> None:
    stream = getattr(sys.stdout, "buffer", None)
    if stream is not None:
        stream.write(data)
        stream.flush()
        return
    sys.stdout.write(data.decode("utf-8", errors="replace"))
    sys.stdout.flush()


def _stream_container_logs(socket_path: str, container_id: str) -> None:
    connection = _UnixSocketHTTPConnection(socket_path, timeout=86400)
    path = (
        f"/containers/{quote(container_id, safe='')}/logs"
        "?follow=1&stdout=1&stderr=1"
    )
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        if response.status >= 400:
            message = response.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Docker logs request failed with HTTP {response.status}: {message}"
            )
        while True:
            header = response.read(8)
            if not header:
                return
            if len(header) != 8:
                raise RuntimeError("Docker returned an incomplete log frame")
            size = struct.unpack(">I", header[4:])[0]
            payload = response.read(size)
            if len(payload) != size:
                raise RuntimeError("Docker returned an incomplete log payload")
            _write_output(payload)
    finally:
        connection.close()


def _extra_bind(
    mounts: list[Any],
    value: str,
) -> str:
    parts = value.rsplit(":", 2)
    if len(parts) == 2:
        source, target = parts
        mode = "ro"
    elif len(parts) == 3:
        source, target, mode = parts
    else:
        raise ValueError("--mount values must use SOURCE:TARGET[:ro|rw]")
    source_path = Path(source).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"mount source not found: {source_path}")
    if not target.startswith("/"):
        raise ValueError("--mount target must be an absolute container path")
    if mode not in {"ro", "rw"}:
        raise ValueError("--mount mode must be ro or rw")
    host_source = mount_source_for_path(mounts, source_path)
    return f"{host_source}:{target}:{mode}"


def remove_script_containers(
    *,
    pipeline_run_id: str | None = None,
    socket_path: str = "/var/run/docker.sock",
) -> int:
    """Remove unfinished one-shot containers after cancellation or restart."""
    labels = [
        "scenegendeploybench.managed=true",
        "scenegendeploybench.operation=run",
    ]
    if pipeline_run_id:
        labels.append(
            f"scenegendeploybench.pipeline_run_id={pipeline_run_id}"
        )
    filters = quote(json.dumps({"label": labels}), safe="")
    client = _DockerEngineClient(socket_path=socket_path)
    containers = client.request(
        "GET",
        f"/containers/json?all=1&filters={filters}",
    )
    removed = 0
    for container in containers or []:
        if not isinstance(container, dict) or not container.get("Id"):
            continue
        try:
            client.request(
                "DELETE",
                f"/containers/{quote(str(container['Id']), safe='')}?force=1",
            )
            removed += 1
        except RuntimeError:
            continue
    return removed


def _stage_workspace_files(
    workspace: Path,
    files: dict[str, str | bytes] | None,
) -> None:
    for raw_path, content in (files or {}).items():
        relative = Path(raw_path)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("workspace file paths must stay inside /workspace")
        target = workspace / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding="utf-8")


def _workspace_files(workspace: Path) -> set[Path]:
    return {
        path.relative_to(workspace)
        for path in workspace.rglob("*")
        if path.is_file() and not path.is_symlink()
    }


def _load_script_result(workspace: Path) -> dict[str, Any]:
    result_path = workspace / "result.json"
    if not result_path.exists():
        return {}
    try:
        loaded = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return dict(loaded) if isinstance(loaded, dict) else {}


def _rewrite_script_output_paths(
    result: dict[str, Any],
    published: dict[str, str],
) -> dict[str, Any]:
    output_files = result.get("output_files")
    if not isinstance(output_files, dict):
        return result
    rewritten: dict[str, Any] = {}
    for sample_id, sample_files in output_files.items():
        if not isinstance(sample_files, dict):
            rewritten[str(sample_id)] = sample_files
            continue
        rewritten[str(sample_id)] = {
            str(data_type): published.get(str(path), path)
            for data_type, path in sample_files.items()
        }
    result["output_files"] = rewritten
    return result


def _publish_script_file(source: Path, destination: Path) -> None:
    shutil.copyfile(source, destination)
    try:
        shutil.copystat(source, destination)
    except OSError as exc:
        logger.warning(
            "could not preserve script output metadata for %s: %s; "
            "keeping copied contents",
            destination,
            exc,
        )


def _publish_script_workspace(
    *,
    workspace: Path,
    initial_files: set[Path],
    pipeline_root: Path,
    publish_dir: Path | None,
    retention: str,
) -> dict[str, Any]:
    result = _load_script_result(workspace)
    created_files = sorted(
        _workspace_files(workspace) - initial_files - {Path("result.json")},
        key=lambda path: path.as_posix(),
    )
    if retention == "none":
        result.pop("output_files", None)
        return result
    if not created_files:
        return result
    if publish_dir is None:
        raise ValueError("persistent script outputs require a publish directory")

    destination_root = (pipeline_root / publish_dir).resolve()
    try:
        destination_root.relative_to(pipeline_root.resolve())
    except ValueError as exc:
        raise ValueError("script output directory must stay inside pipeline_root") from exc

    published: dict[str, str] = {}
    for relative in created_files:
        source = workspace / relative
        destination = destination_root / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        _publish_script_file(source, destination)
        published[relative.as_posix()] = str(destination)
    return _rewrite_script_output_paths(result, published)


def run_script_container(
    config: OrchestratorConfig,
    *,
    image: str,
    script_path: str | None,
    command: list[str],
    access_values: list[str] | None,
    environment_values: list[str] | None,
    mount_values: list[str] | None,
    workdir: str,
    workspace_files: dict[str, str | bytes] | None = None,
    labels: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
    publish_dir: Path | None = None,
    retention: str = "none",
    socket_path: str = "/var/run/docker.sock",
) -> ScriptRunResult:
    normalized_image = image.strip()
    if not normalized_image:
        raise ValueError("--image is required")
    if not script_path and not command:
        raise ValueError("provide a script path or an inline command after --")
    if not workdir.startswith("/"):
        raise ValueError("--workdir must be an absolute container path")
    if timeout_seconds is not None and timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be greater than 0")
    if retention not in {"keep", "pipeline", "matrix", "none"}:
        raise ValueError("retention must be keep, pipeline, matrix, or none")

    access = normalize_access(access_values)
    environment = {
        "HOME": "/tmp",
        "LOGNAME": "deploybench",
        "USER": "deploybench",
        **parse_environment(environment_values),
        "DEPLOYBENCH_RESULT": "/workspace/result.json",
    }
    if "database" in access:
        environment.update(_database_environment(config))

    pipeline_root = config.storage.pipeline_root
    pipeline_root.mkdir(parents=True, exist_ok=True)
    staging_root = pipeline_root / ".runs"
    staging_root.mkdir(parents=True, exist_ok=True)
    workspace = Path(
        tempfile.mkdtemp(prefix="run-", dir=str(staging_root))
    ).resolve()

    container_id: str | None = None
    client = _DockerEngineClient(socket_path=socket_path)
    timeout_timer: threading.Timer | None = None
    timed_out = threading.Event()
    try:
        _stage_workspace_files(workspace, workspace_files)
        if script_path:
            source_script = Path(script_path).expanduser().resolve()
            if not source_script.is_file():
                raise FileNotFoundError(f"script file not found: {source_script}")
            staged_script = workspace / source_script.name
            shutil.copy2(source_script, staged_script)
            staged_script.chmod(staged_script.stat().st_mode | 0o111)
            container_command = _script_command(
                staged_script,
                list(command),
            )
        else:
            container_command = list(command)
        initial_files = _workspace_files(workspace)

        client.ping()
        client.ensure_image(normalized_image)
        inspected = inspect_current_container(client)
        mounts = list(inspected.get("Mounts") or [])
        networks = tuple(
            str(name).strip()
            for name in (
                (inspected.get("NetworkSettings") or {}).get("Networks") or {}
            )
            if str(name).strip()
        )
        workspace_source = mount_source_for_path(mounts, workspace)
        binds = [f"{workspace_source}:/workspace:rw"]

        storage_access = {
            "datasets": (config.storage.dataset_root, "/data/datasets", "ro"),
            "output": (config.storage.output_root, "/data/output", "ro"),
            "pipelines": (config.storage.pipeline_root, "/data/pipelines", "rw"),
            "model-cache": (
                config.storage.model_cache_root,
                "/data/model_cache",
                "rw",
            ),
        }
        path_environment = {
            "datasets": ("PATH_DATASETS", "/data/datasets"),
            "output": ("PATH_OUTPUT", "/data/output"),
            "pipelines": ("PATH_PIPELINES", "/data/pipelines"),
            "model-cache": ("PATH_MODEL_CACHE", "/data/model_cache"),
        }
        for access_name, (source_path, target, mode) in storage_access.items():
            if access_name not in access:
                continue
            source = mount_source_for_destination(mounts, str(source_path))
            binds.append(f"{source}:{target}:{mode}")
            env_name, env_value = path_environment[access_name]
            environment[env_name] = env_value
        binds.extend(_extra_bind(mounts, value) for value in mount_values or [])

        host_config: dict[str, Any] = {
            "AutoRemove": False,
            "Binds": binds,
            "NetworkMode": (
                networks[0]
                if "database" in access and networks
                else "bridge"
            ),
        }
        if "database" in access and not networks:
            raise RuntimeError(
                "database access requires the orchestrator to be attached to a Docker network"
            )
        payload: dict[str, Any] = {
            "Image": normalized_image,
            "Cmd": container_command,
            "WorkingDir": workdir,
            "Env": [
                f"{key}={value}"
                for key, value in sorted(environment.items())
            ],
            "AttachStdout": True,
            "AttachStderr": True,
            "Tty": False,
            "Labels": {
                **dict(labels or {}),
                "scenegendeploybench.managed": "true",
                "scenegendeploybench.operation": "run",
            },
            "HostConfig": host_config,
        }
        container_user = _container_user()
        if container_user:
            payload["User"] = container_user

        container_name = f"scenegendeploybench-run-{uuid.uuid4().hex[:12]}"
        created = client.request(
            "POST",
            f"/containers/create?name={quote(container_name, safe='')}",
            payload,
        )
        if not isinstance(created, dict) or not created.get("Id"):
            raise RuntimeError(
                f"Docker API returned an invalid create response: {created!r}"
            )
        container_id = str(created["Id"])
        client.request(
            "POST",
            f"/containers/{quote(container_id, safe='')}/start",
        )
        if timeout_seconds is not None:
            def stop_after_timeout() -> None:
                try:
                    _DockerEngineClient(socket_path=socket_path).request(
                        "POST",
                        f"/containers/{quote(container_id or '', safe='')}/kill",
                    )
                    timed_out.set()
                except RuntimeError:
                    return

            timeout_timer = threading.Timer(
                timeout_seconds,
                stop_after_timeout,
            )
            timeout_timer.daemon = True
            timeout_timer.start()
        _stream_container_logs(socket_path, container_id)
        if timeout_timer is not None:
            timeout_timer.cancel()
        inspected_run = client.request(
            "GET",
            f"/containers/{quote(container_id, safe='')}/json",
        )
        state = (
            dict(inspected_run.get("State") or {})
            if isinstance(inspected_run, dict)
            else {}
        )
        if state.get("Running"):
            client.request(
                "POST",
                f"/containers/{quote(container_id, safe='')}/wait?condition=not-running",
            )
            inspected_run = client.request(
                "GET",
                f"/containers/{quote(container_id, safe='')}/json",
            )
            state = (
                dict(inspected_run.get("State") or {})
                if isinstance(inspected_run, dict)
                else {}
            )
        if timed_out.is_set():
            return ScriptRunResult(exit_code=124, result={})
        exit_code = int(state.get("ExitCode") or 0)
        result: dict[str, Any] = {}
        if exit_code == 0:
            result = _publish_script_workspace(
                workspace=workspace,
                initial_files=initial_files,
                pipeline_root=pipeline_root,
                publish_dir=publish_dir,
                retention=retention,
            )
        return ScriptRunResult(
            exit_code=exit_code,
            result=result,
        )
    except KeyboardInterrupt:
        return ScriptRunResult(exit_code=130, result={})
    finally:
        if timeout_timer is not None:
            timeout_timer.cancel()
        if container_id:
            try:
                client.request(
                    "DELETE",
                    f"/containers/{quote(container_id, safe='')}?force=1",
                )
            except Exception:
                pass
        shutil.rmtree(workspace, ignore_errors=True)
        try:
            staging_root.rmdir()
        except OSError:
            pass

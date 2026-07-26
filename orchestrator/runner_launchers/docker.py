from __future__ import annotations

import http.client
import json
import os
import re
import socket
import uuid
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

from .base import RunnerLaunchContext


class _UnixSocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float = 10.0) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._socket_path)


@dataclass(frozen=True)
class _DockerHostContext:
    datasets_source: str
    model_cache_source: str
    output_source: str
    pipeline_source: str
    networks: tuple[str, ...]


@dataclass(frozen=True)
class _DockerEngineClient:
    socket_path: str
    timeout_seconds: float = 10.0

    def request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        body = None
        headers: dict[str, str] = {}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        connection = _UnixSocketHTTPConnection(self.socket_path, timeout=self.timeout_seconds)
        try:
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            raw_body = response.read()
        except OSError as exc:
            raise RuntimeError(f"Docker API request failed for {method} {path}: {exc}") from exc
        finally:
            connection.close()

        decoded = raw_body.decode("utf-8", errors="replace") if raw_body else ""
        if response.status >= 400:
            raise RuntimeError(
                f"Docker API {method} {path} failed with HTTP {response.status}: {decoded or response.reason}"
            )
        if not decoded:
            return None
        content_type = response.getheader("Content-Type", "")
        if "application/json" in content_type:
            return json.loads(decoded)
        return decoded

    def image_exists(self, image: str) -> bool:
        connection = _UnixSocketHTTPConnection(
            self.socket_path,
            timeout=self.timeout_seconds,
        )
        path = f"/images/{quote(image, safe='')}/json"
        try:
            connection.request("GET", path)
            response = connection.getresponse()
            raw_body = response.read()
        except OSError as exc:
            raise RuntimeError(f"Docker API request failed for GET {path}: {exc}") from exc
        finally:
            connection.close()
        if response.status == 404:
            return False
        if response.status >= 400:
            decoded = raw_body.decode("utf-8", errors="replace") if raw_body else ""
            raise RuntimeError(
                f"Docker API GET {path} failed with HTTP {response.status}: "
                f"{decoded or response.reason}"
            )
        return True

    def pull_image(self, image: str) -> None:
        path = f"/images/create?fromImage={quote(image, safe='')}"
        connection = _UnixSocketHTTPConnection(
            self.socket_path,
            timeout=max(self.timeout_seconds, 300.0),
        )
        try:
            connection.request("POST", path)
            response = connection.getresponse()
            if response.status >= 400:
                raw_body = response.read()
                decoded = raw_body.decode("utf-8", errors="replace") if raw_body else ""
                raise RuntimeError(
                    f"Docker API POST {path} failed with HTTP {response.status}: "
                    f"{decoded or response.reason}"
                )
            while True:
                raw_line = response.readline()
                if not raw_line:
                    break
                try:
                    event = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(event, dict) or not event.get("error"):
                    continue
                detail = event.get("errorDetail")
                message = (
                    detail.get("message")
                    if isinstance(detail, dict)
                    else event.get("error")
                )
                raise RuntimeError(f"failed to pull Docker image {image}: {message}")
        except OSError as exc:
            raise RuntimeError(f"Docker image pull failed for {image}: {exc}") from exc
        finally:
            connection.close()

    def ensure_image(self, image: str) -> bool:
        if self.image_exists(image):
            return False
        self.pull_image(image)
        return True

    def ping(self) -> None:
        response = self.request("GET", "/_ping")
        if str(response).strip() != "OK":
            raise RuntimeError(f"Docker API ping returned unexpected response: {response!r}")


def inspect_current_container(client: _DockerEngineClient) -> dict[str, Any]:
    container_ref = (os.getenv("HOSTNAME") or "").strip()
    if not container_ref:
        raise RuntimeError(
            "could not determine the current orchestrator container id from HOSTNAME"
        )
    inspected = client.request(
        "GET",
        f"/containers/{quote(container_ref, safe='')}/json",
    )
    if not isinstance(inspected, dict):
        raise RuntimeError(
            "Docker API returned an invalid inspect response for the orchestrator container"
        )
    return inspected


def mount_source_for_destination(
    mounts: list[Any],
    destination: str,
) -> str:
    normalized_destination = str(destination).rstrip("/") or "/"
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        mount_destination = str(mount.get("Destination") or "").rstrip("/") or "/"
        source = str(mount.get("Source") or "").strip()
        if mount_destination == normalized_destination and source:
            return source
    raise RuntimeError(
        f"could not find a host bind source for {normalized_destination}"
    )


def mount_source_for_path(mounts: list[Any], path: str | os.PathLike[str]) -> str:
    resolved = os.path.realpath(os.fspath(path))
    matches: list[tuple[int, str]] = []
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        destination = os.path.realpath(str(mount.get("Destination") or ""))
        source = str(mount.get("Source") or "").strip()
        if not destination or not source:
            continue
        try:
            common = os.path.commonpath([resolved, destination])
        except ValueError:
            continue
        if common != destination:
            continue
        relative = os.path.relpath(resolved, destination)
        host_path = source if relative == "." else os.path.join(source, relative)
        matches.append((len(destination), host_path))
    if not matches:
        raise RuntimeError(
            f"{resolved} is not inside storage mounted into the orchestrator"
        )
    return max(matches, key=lambda item: item[0])[1]


@dataclass
class DockerRunnerLauncher:
    context: RunnerLaunchContext
    _container_id: str | None = field(default=None, init=False, repr=False)
    _endpoint: str | None = field(default=None, init=False, repr=False)

    def validate(self) -> None:
        compat_version = int(self.context.runner.launcher.get("compat_version", 0))
        if compat_version != 1:
            raise ValueError(
                f"runner {self.context.runner.selector} uses unsupported docker compat_version {compat_version}"
            )
        image = self.context.runner.launcher.get("image")
        if not isinstance(image, str) or not image.strip():
            raise ValueError(
                f"runner {self.context.runner.selector} docker launcher requires launcher.image"
            )
        endpoint = self._endpoint_config()
        port = endpoint.get("port", 58090)
        if int(port) <= 0 or int(port) > 65535:
            raise ValueError(
                f"runner {self.context.runner.selector} docker launcher requires launcher.endpoint.port between 1 and 65535"
            )
        env = self.context.runner.launcher.get("env") or {}
        if env is not None and not isinstance(env, dict):
            raise ValueError(
                f"runner {self.context.runner.selector} docker launcher env must be a mapping"
            )
        env_passthrough = self.context.runner.launcher.get("env_passthrough") or []
        if isinstance(env_passthrough, str):
            env_passthrough = [env_passthrough]
        if not isinstance(env_passthrough, list):
            raise ValueError(
                f"runner {self.context.runner.selector} docker launcher env_passthrough must be a list"
            )
        socket_path = self._socket_path()
        if not os.path.exists(socket_path):
            raise RuntimeError(
                f"docker launcher requires Docker socket at {socket_path}; mount it into the orchestrator container"
            )

    def start_runner(self) -> None:
        if self._endpoint is not None:
            return
        self.validate()
        client = _DockerEngineClient(socket_path=self._socket_path())
        client.ping()
        client.ensure_image(str(self.context.runner.launcher.get("image")).strip())
        host_context = self._discover_host_context(client)
        container_name = self._build_container_name()
        payload = self._container_create_payload(host_context)
        container_id: str | None = None
        try:
            created = client.request(
                "POST",
                f"/containers/create?name={quote(container_name, safe='')}",
                payload,
            )
            if not isinstance(created, dict) or not created.get("Id"):
                raise RuntimeError(
                    f"Docker API returned an invalid create response for runner {self.context.runner.selector}: {created!r}"
                )
            container_id = str(created["Id"])
            client.request("POST", f"/containers/{quote(container_id, safe='')}/start")
            for network_name in host_context.networks[1:]:
                client.request(
                    "POST",
                    f"/networks/{quote(network_name, safe='')}/connect",
                    {"Container": container_id},
                )
        except Exception:
            if container_id is not None:
                try:
                    client.request(
                        "DELETE",
                        f"/containers/{quote(container_id, safe='')}?force=1",
                    )
                except Exception:
                    pass
            raise

        self._container_id = container_id
        self._endpoint = f"http://{container_name}:{self._endpoint_port()}"

    def get_endpoint(self) -> str:
        self.start_runner()
        return self._endpoint or ""

    def stop_runner(self) -> None:
        if self._container_id is None:
            return
        try:
            _DockerEngineClient(socket_path=self._socket_path()).request(
                "DELETE",
                f"/containers/{quote(self._container_id, safe='')}?force=1",
            )
        except Exception:
            return

    def _socket_path(self) -> str:
        socket_path = self.context.runner.launcher.get("socket_path") or "/var/run/docker.sock"
        return str(socket_path).strip()

    def _endpoint_config(self) -> dict[str, Any]:
        endpoint = self.context.runner.launcher.get("endpoint") or {}
        if not isinstance(endpoint, dict):
            raise ValueError(
                f"runner {self.context.runner.selector} launcher.endpoint must be a mapping"
            )
        return endpoint

    def _endpoint_port(self) -> int:
        return int(self._endpoint_config().get("port", 58090))

    def _startup_timeout_seconds(self) -> str:
        timeout_minutes = float(self.context.runner.scheduling.get("startup_timeout_minutes", 1.0))
        if timeout_minutes <= 0:
            raise ValueError(
                f"runner {self.context.runner.selector} scheduling.startup_timeout_minutes must be greater than 0"
            )
        # The orchestrator enforces startup_timeout_minutes; the wrapper exits
        # one minute later as a container-local kill switch if cleanup stalls.
        return str((timeout_minutes + 1.0) * 60)

    def _build_container_name(self) -> str:
        # The runner endpoint uses the container name as an in-network host.
        # Keep it DNS-safe: Docker names may allow underscores, but they are
        # not reliably resolvable through libc DNS lookups inside containers.
        selector = re.sub(r"[^a-z0-9-]+", "-", self.context.runner.selector.lower()).strip("-")
        selector = re.sub(r"-{2,}", "-", selector)
        selector = selector or "runner"
        return f"scenegendeploybench-runner-{selector}-{uuid.uuid4().hex[:12]}"

    def _container_env(self) -> list[str]:
        env_payload = {
            "HOME": "/tmp",
            "LOGNAME": "deploybench",
            "PATH_DATASETS": self.context.dataset_root,
            "PATH_MODEL_CACHE": self.context.model_cache_root,
            "PATH_OUTPUT": self.context.output_root,
            "PATH_PIPELINES": self.context.pipeline_root,
            "RUNNER_PORT": str(self._endpoint_port()),
            "RUNNER_NAME": self.context.runner.runner,
            "RUNNER_TYPE": self.context.runner.kind,
            "RUNNER_VERSION": self.context.runner.version,
            "RUNNER_CONTRACT_VERSION": str(self.context.runner.contract_version),
            "RUNNER_STARTUP_TIMEOUT_SECONDS": self._startup_timeout_seconds(),
            "USER": "deploybench",
            "XDG_CACHE_HOME": "/tmp/.cache",
        }
        custom_env = self.context.runner.launcher.get("env") or {}
        for key, value in custom_env.items():
            env_key = str(key).strip()
            if not env_key:
                raise ValueError(
                    f"runner {self.context.runner.selector} launcher env contains an empty key"
                )
            env_payload[env_key] = str(value)
        env_passthrough = self.context.runner.launcher.get("env_passthrough") or []
        if isinstance(env_passthrough, str):
            env_passthrough = [env_passthrough]
        for key in env_passthrough:
            env_key = str(key).strip()
            if not env_key:
                raise ValueError(
                    f"runner {self.context.runner.selector} launcher env_passthrough contains an empty key"
                )
            if env_key in env_payload:
                continue
            configured_env = self.context.runner_env or {}
            configured_value = configured_env.get(env_key)
            if configured_value not in {None, ""}:
                env_payload[env_key] = str(configured_value)
                continue
            process_value = os.getenv(env_key)
            if process_value not in {None, ""}:
                env_payload[env_key] = process_value
        return [f"{key}={value}" for key, value in sorted(env_payload.items())]

    def _discover_host_context(self, client: _DockerEngineClient) -> _DockerHostContext:
        inspected = inspect_current_container(client)
        mounts = inspected.get("Mounts") or []
        datasets_source = mount_source_for_destination(
            mounts,
            self.context.dataset_root,
        )
        model_cache_source = mount_source_for_destination(
            mounts,
            self.context.model_cache_root,
        )
        output_source = mount_source_for_destination(
            mounts,
            self.context.output_root,
        )
        pipeline_source = mount_source_for_destination(
            mounts,
            self.context.pipeline_root,
        )
        networks_map = ((inspected.get("NetworkSettings") or {}).get("Networks") or {})
        networks = tuple(str(name).strip() for name in networks_map.keys() if str(name).strip())
        if not networks:
            raise RuntimeError("docker launcher requires the orchestrator container to be attached to at least one network")
        return _DockerHostContext(
            datasets_source=datasets_source,
            model_cache_source=model_cache_source,
            output_source=output_source,
            pipeline_source=pipeline_source,
            networks=networks,
        )

    def _container_create_payload(self, host_context: _DockerHostContext) -> dict[str, Any]:
        datasets_mode = "rw" if self.context.runner.kind == "dataset_downloader" else "ro"
        host_config: dict[str, Any] = {
            "AutoRemove": True,
            "Binds": [
                f"{host_context.datasets_source}:{self.context.dataset_root}:{datasets_mode}",
                f"{host_context.model_cache_source}:{self.context.model_cache_root}:rw",
                # Runner containers receive the shared output mount
                # directly so they can write generated files without any
                # orchestrator filesystem mediation.
                f"{host_context.output_source}:{self.context.output_root}:rw",
                f"{host_context.pipeline_source}:{self.context.pipeline_root}:ro",
            ],
            "NetworkMode": host_context.networks[0],
        }
        device_requests = self._device_requests()
        if device_requests:
            host_config["DeviceRequests"] = device_requests

        payload = {
            "Image": str(self.context.runner.launcher.get("image")).strip(),
            "Env": self._container_env(),
            "Labels": {
                "scenegendeploybench.managed": "true",
                "scenegendeploybench.runner_selector": self.context.runner.selector,
                "scenegendeploybench.launcher_driver": "docker",
            },
            "HostConfig": host_config,
        }
        container_user = self._container_user()
        if container_user:
            payload["User"] = container_user
        return payload

    def _container_user(self) -> str:
        launcher_user = str(self.context.runner.launcher.get("user") or "").strip()
        if launcher_user:
            return launcher_user

        uid = os.getenv("UID", "").strip()
        gid = os.getenv("GID", "").strip()
        if not uid and not gid:
            return ""
        if not uid or not gid:
            raise ValueError("UID and GID must be set together")
        if not uid.isdigit() or not gid.isdigit():
            raise ValueError("UID and GID must be numeric")
        return f"{uid}:{gid}"

    def _device_requests(self) -> list[dict[str, Any]]:
        raw_gpus = self.context.runner.launcher.get("gpus")
        if raw_gpus is None:
            return []
        gpus = str(raw_gpus).strip().lower()
        if not gpus or gpus in {"none", "0", "false", "no", "off"}:
            return []
        request: dict[str, Any] = {
            "Driver": "nvidia",
            "Capabilities": [["gpu"]],
        }
        if gpus == "all":
            request["Count"] = -1
        else:
            try:
                request["Count"] = int(gpus)
            except ValueError as exc:
                raise ValueError(
                    f"runner {self.context.runner.selector} launcher.gpus must be 'all', 'none', or an integer"
                ) from exc
            if request["Count"] <= 0:
                return []
        return [request]

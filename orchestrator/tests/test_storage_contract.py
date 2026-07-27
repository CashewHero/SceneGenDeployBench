from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from app.config import load_config
from main import build_parser
from runner_launchers.base import RunnerLaunchContext
from runner_launchers.docker import (
    DockerRunnerLauncher,
    _DockerEngineClient,
    _DockerHostContext,
)
from execution.script_run import (
    _publish_script_workspace,
    normalize_access,
    parse_environment,
)
from storage import db as db_storage
from storage.db import (
    DatabaseUnavailableError,
    _job_output_dir,
    connect_database,
    insert_dataset_download_job,
    output_sample_payload,
    sync_dataset_state,
)


SYSTEM_CONFIG = """
config_version: 1
storage:
  dataset_root: /data/datasets
  model_cache_root: /data/model_cache
  output_root: /data/output
  pipeline_root: /data/pipelines
catalogs:
  runners: runners
"""

RUNNER_CONFIG = """
catalog_version: 1
runners:
  - runner: test_runner
    version: 0.1.0
    latest: true
    display_name: Test Runner
    kind: generator
    contract_version: 1
    inputs:
      data:
        required_sample:
          required_datatype: [image]
      candidate:
        required_sample:
          required_datatype: [scene]
    launcher:
      driver: docker
      compat_version: 1
      image: test-runner:local
      endpoint:
        port: 58090
"""


class StorageContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        root = Path(self.temp_dir.name)
        (root / "runners").mkdir()
        (root / "system.yaml").write_text(SYSTEM_CONFIG, encoding="utf-8")
        (root / "runners" / "test.yaml").write_text(RUNNER_CONFIG, encoding="utf-8")
        self.config_path = root / "system.yaml"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_storage_paths_and_environment_override(self) -> None:
        config = load_config(str(self.config_path))
        self.assertEqual(config.storage.model_cache_root, Path("/data/model_cache"))
        self.assertEqual(config.storage.pipeline_root, Path("/data/pipelines"))
        self.assertEqual(
            config.runners["test_runner@0.1.0"].inputs["data"]["required_sample"],
            {
                "required_datatype": ["image"],
                "optional_datatype": [],
            },
        )
        self.assertEqual(
            config.runners["test_runner@0.1.0"].inputs["candidate"]["required_sample"],
            {
                "required_datatype": ["scene"],
                "optional_datatype": [],
            },
        )

        with patch.dict(os.environ, {"PATH_MODEL_CACHE": "/custom/model-cache"}):
            overridden = load_config(str(self.config_path))
        self.assertEqual(overridden.storage.model_cache_root, Path("/custom/model-cache"))

    def test_job_output_path(self) -> None:
        config = load_config(str(self.config_path))
        row = {
            "runner_name": "test_runner",
            "runner_version": "0.1.0",
            "dataset_name": "dataset-a",
            "sample_metadata_json": {},
            "subset_key": "subset-a",
            "external_key": "subset-a/sample-1",
            "sample_id": "sample-1",
            "job_id": "job-1",
        }
        self.assertEqual(
            _job_output_dir(config, row),
            Path("/data/output/test_runner@0.1.0/dataset-a/subset-a/sample-1"),
        )
    def test_docker_runner_receives_all_shared_mounts(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        launcher = DockerRunnerLauncher(RunnerLaunchContext(runner=runner))
        host = _DockerHostContext(
            datasets_source="/host/datasets",
            model_cache_source="/host/model_cache",
            output_source="/host/output",
            pipeline_source="/host/pipelines",
            networks=("benchmark",),
        )
        container = launcher._container_create_payload(host)
        binds = container["HostConfig"]["Binds"]
        self.assertEqual(
            binds,
            [
                "/host/datasets:/data/datasets:ro",
                "/host/model_cache:/data/model_cache:rw",
                "/host/output:/data/output:rw",
                "/host/pipelines:/data/pipelines:ro",
            ],
        )
        self.assertIn("PATH_DATASETS=/data/datasets", container["Env"])
        self.assertIn("PATH_MODEL_CACHE=/data/model_cache", container["Env"])
        self.assertIn("PATH_OUTPUT=/data/output", container["Env"])
        self.assertIn("PATH_PIPELINES=/data/pipelines", container["Env"])

    def test_docker_image_is_pulled_only_when_missing(self) -> None:
        client = _DockerEngineClient("/var/run/docker.sock")
        with (
            patch.object(_DockerEngineClient, "image_exists", return_value=True),
            patch.object(_DockerEngineClient, "pull_image") as pull_image,
        ):
            self.assertFalse(client.ensure_image("example:0.1.0"))
            pull_image.assert_not_called()
        with (
            patch.object(_DockerEngineClient, "image_exists", return_value=False),
            patch.object(_DockerEngineClient, "pull_image") as pull_image,
            self.assertLogs("scenegendeploybench.docker", level="INFO") as logs,
        ):
            self.assertTrue(client.ensure_image("example:0.1.0"))
            pull_image.assert_called_once_with("example:0.1.0")
        self.assertIn("pulling it now", logs.output[0])
        self.assertIn("pull completed", logs.output[1])

    def test_docker_preflight_skips_probe_without_gpu_request(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        client = Mock()
        with (
            patch("runner_launchers.docker.os.path.exists", return_value=True),
            patch(
                "runner_launchers.docker._DockerEngineClient",
                return_value=client,
            ),
        ):
            result = DockerRunnerLauncher(
                RunnerLaunchContext(runner=runner)
            ).preflight()

        self.assertTrue(result.available)
        client.ping.assert_called_once_with()
        client.ensure_image.assert_not_called()
        client.request.assert_not_called()

    def test_docker_gpu_preflight_starts_and_removes_probe(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        runner = replace(
            runner,
            launcher={**runner.launcher, "gpus": "all"},
        )
        client = Mock()
        client.request.side_effect = [{"Id": "probe-1"}, None, None]
        with (
            patch("runner_launchers.docker.os.path.exists", return_value=True),
            patch(
                "runner_launchers.docker._DockerEngineClient",
                return_value=client,
            ),
        ):
            result = DockerRunnerLauncher(
                RunnerLaunchContext(runner=runner)
            ).preflight()

        self.assertTrue(result.available)
        client.ensure_image.assert_called_once_with("test-runner:local")
        create_request, start_request, delete_request = (
            client.request.call_args_list
        )
        self.assertEqual(create_request.args[0], "POST")
        self.assertIn("/containers/create?name=", create_request.args[1])
        self.assertEqual(
            create_request.args[2]["HostConfig"]["DeviceRequests"],
            [
                {
                    "Driver": "nvidia",
                    "Capabilities": [["gpu"]],
                    "Count": -1,
                }
            ],
        )
        self.assertEqual(
            start_request,
            call("POST", "/containers/probe-1/start"),
        )
        self.assertEqual(
            delete_request,
            call("DELETE", "/containers/probe-1?force=1"),
        )

    def test_docker_gpu_device_selection_uses_device_ids(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        runner = replace(
            runner,
            launcher={
                **runner.launcher,
                "gpus": (
                    "device=GPU-748e4f90-0154-4547-2102-ddaa87955eb8"
                ),
            },
        )

        requests = DockerRunnerLauncher(
            RunnerLaunchContext(runner=runner)
        )._device_requests()

        self.assertEqual(
            requests,
            [
                {
                    "Driver": "nvidia",
                    "Capabilities": [["gpu"]],
                    "DeviceIDs": [
                        "GPU-748e4f90-0154-4547-2102-ddaa87955eb8"
                    ],
                }
            ],
        )

    def test_docker_gpu_preflight_reports_start_failure_and_cleans_up(
        self,
    ) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        runner = replace(
            runner,
            launcher={**runner.launcher, "gpus": "all"},
        )
        client = Mock()
        client.request.side_effect = [
            {"Id": "probe-1"},
            RuntimeError("nvidia runtime failed"),
            None,
        ]
        with (
            patch("runner_launchers.docker.os.path.exists", return_value=True),
            patch(
                "runner_launchers.docker._DockerEngineClient",
                return_value=client,
            ),
        ):
            result = DockerRunnerLauncher(
                RunnerLaunchContext(runner=runner)
            ).preflight()

        self.assertFalse(result.available)
        self.assertEqual(result.code, "LAUNCHER_PREFLIGHT_FAILED")
        self.assertIn("nvidia runtime failed", result.message or "")
        self.assertEqual(
            client.request.call_args_list[-1],
            call("DELETE", "/containers/probe-1?force=1"),
        )

    def test_claim_candidates_exclude_unavailable_runners(self) -> None:
        config = load_config(str(self.config_path))
        cursor = Mock()
        cursor.fetchall.return_value = []
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)
        with patch.object(
            db_storage,
            "connect_database",
            return_value=nullcontext(connection),
        ):
            db_storage._claim_candidate_rows(
                config,
                excluded_runner_selectors={"test_runner@0.1.0"},
            )

        query, params = cursor.execute.call_args.args
        self.assertIn(
            "AND NOT (runner_selector = ANY(%s))",
            query,
        )
        self.assertEqual(params[0], ["test_runner@0.1.0"])

    def test_database_connection_error_is_actionable(self) -> None:
        config = load_config(str(self.config_path))

        class FakeOperationalError(Exception):
            pass

        fake_psycopg = SimpleNamespace(
            OperationalError=FakeOperationalError,
            connect=Mock(side_effect=FakeOperationalError("raw connection details")),
        )
        with (
            patch.object(db_storage, "psycopg", fake_psycopg),
            patch.object(db_storage, "dict_row", object()),
            patch.object(db_storage, "Jsonb", object()),
        ):
            with self.assertRaisesRegex(
                DatabaseUnavailableError,
                r"Database unavailable at 127\.0\.0\.1:5432/scenegendeploybench",
            ) as raised:
                with connect_database(config):
                    pass
        self.assertNotIn("raw connection details", str(raised.exception))

    def test_dataset_rescan_command_accepts_one_dataset_or_all(self) -> None:
        parser = build_parser()
        targeted = parser.parse_args(["dataset", "rescan", "example_set1"])
        complete = parser.parse_args(["dataset", "rescan"])
        self.assertEqual(targeted.dataset_name, "example_set1")
        self.assertIsNone(complete.dataset_name)

    def test_dataset_download_job_does_not_rescan_datasets(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        cursor = Mock()
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)
        with (
            patch.object(db_storage, "sync_runner_state") as sync_runners,
            patch.object(db_storage, "sync_dataset_state") as sync_datasets,
            patch.object(
                db_storage,
                "connect_database",
                return_value=nullcontext(connection),
            ),
            patch.object(db_storage, "insert_resolved_job_row"),
        ):
            insert_dataset_download_job(
                config,
                dataset_name="example_set1",
                runner=runner,
                parameters={},
                timeout_seconds=60,
                allow_start_outside_window=False,
            )
        sync_runners.assert_called_once_with(config)
        sync_datasets.assert_not_called()

    def test_targeted_dataset_rescan_passes_only_that_dataset(self) -> None:
        config = load_config(str(self.config_path))
        cursor = Mock()
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)
        with (
            patch.object(
                db_storage,
                "connect_database",
                return_value=nullcontext(connection),
            ),
            patch.object(
                db_storage,
                "_sync_samples",
                return_value=(1, 3),
            ) as sync_samples,
        ):
            result = sync_dataset_state(config, dataset_name="example_set1")
        sync_samples.assert_called_once_with(
            cursor,
            config,
            dataset_name="example_set1",
        )
        self.assertEqual(
            result,
            {
                "dataset": "example_set1",
                "dataset_count": 1,
                "sample_count": 3,
            },
        )

    def test_output_files_keep_sample_and_data_type_shape(self) -> None:
        outputs, data_types = output_sample_payload(
            "/data/output/test_runner@0.1.0/dataset-a/sample-1",
            {
                "sample-1": {
                    "image": "copied.png",
                    "camera_pose": "pose.json",
                },
                "sample-2": {
                    "image": "/shared/render.png",
                },
            },
        )
        self.assertEqual(
            outputs,
            {
                "sample-1": {
                    "image": "/data/output/test_runner@0.1.0/dataset-a/sample-1/copied.png",
                    "camera_pose": "/data/output/test_runner@0.1.0/dataset-a/sample-1/pose.json",
                },
                "sample-2": {
                    "image": "/shared/render.png",
                },
            },
        )
        self.assertEqual(data_types, ["image", "camera_pose"])

    def test_script_run_options_are_explicit(self) -> None:
        self.assertEqual(
            normalize_access(["datasets,output", "database"]),
            {"datasets", "output", "database"},
        )
        self.assertEqual(
            parse_environment(["METHOD=psnr", "EMPTY="]),
            {"METHOD": "psnr", "EMPTY": ""},
        )

    def test_script_workspace_keeps_all_created_files(self) -> None:
        root = Path(self.temp_dir.name)
        workspace = root / "workspace"
        pipeline_root = root / "pipelines"
        workspace.mkdir()
        (workspace / "pipeline.json").write_text("{}\n", encoding="utf-8")
        (workspace / "report.txt").write_text("done\n", encoding="utf-8")
        (workspace / "extra.csv").write_text("score\n1\n", encoding="utf-8")
        (workspace / "result.json").write_text(
            """
            {
              "output_files": {
                "summary": {"text": "report.txt"}
              },
              "metrics": []
            }
            """,
            encoding="utf-8",
        )

        result = _publish_script_workspace(
            workspace=workspace,
            initial_files={Path("pipeline.json")},
            pipeline_root=pipeline_root,
            publish_dir=Path("example/20260725T120000"),
            retention="keep",
        )

        published = (
            pipeline_root
            / "example/20260725T120000/report.txt"
        )
        self.assertEqual(published.read_text(encoding="utf-8"), "done\n")
        self.assertEqual(
            result["output_files"],
            {"summary": {"text": str(published)}},
        )
        self.assertTrue(
            (pipeline_root / "example/20260725T120000/extra.csv").exists()
        )
        self.assertFalse(
            (pipeline_root / "example/20260725T120000/result.json").exists()
        )

    def test_script_response_is_optional_and_does_not_control_files(self) -> None:
        root = Path(self.temp_dir.name)
        workspace = root / "workspace-without-result"
        pipeline_root = root / "pipelines-without-result"
        workspace.mkdir()
        (workspace / "report.txt").write_text("done\n", encoding="utf-8")

        result = _publish_script_workspace(
            workspace=workspace,
            initial_files=set(),
            pipeline_root=pipeline_root,
            publish_dir=Path("example/run"),
            retention="keep",
        )

        self.assertEqual(result, {})
        self.assertEqual(
            (pipeline_root / "example/run/report.txt").read_text(encoding="utf-8"),
            "done\n",
        )


if __name__ == "__main__":
    unittest.main()

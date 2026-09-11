from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, Mock, call, patch

from app.config import load_config
from main import build_parser
from runner_launchers.base import RunnerLaunchContext
from runner_launchers.docker import (
    DockerRunnerLauncher,
    _DockerEngineClient,
    _DockerHostContext,
)
from execution import dispatch as dispatch_execution
from execution.script_run import (
    _publish_script_workspace,
    normalize_access,
    parse_environment,
)
from storage import db as db_storage
from storage import pipelines as pipeline_storage
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

    def test_docker_runner_passthrough_only_uses_configured_runner_env(self) -> None:
        config = load_config(str(self.config_path))
        original = config.runners["test_runner@0.1.0"]
        runner = replace(
            original,
            launcher=original.launcher | {"env_passthrough": ["HF_TOKEN", "PG_DB_PASSWORD"]},
        )
        launcher = DockerRunnerLauncher(
            RunnerLaunchContext(runner=runner, runner_env={"HF_TOKEN": "configured-token"})
        )
        with patch.dict(os.environ, {"HF_TOKEN": "process-token", "PG_DB_PASSWORD": "process-password"}):
            environment = dict(item.split("=", 1) for item in launcher._container_env())
        self.assertEqual(environment["HF_TOKEN"], "configured-token")
        self.assertNotIn("PG_DB_PASSWORD", environment)

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
        self.assertIn("DISTINCT ON (runner_name)", query)
        self.assertEqual(params[0], ["test_runner@0.1.0"])

    def test_recent_batch_runner_history_uses_five_batches(self) -> None:
        config = load_config(str(self.config_path))
        cursor = Mock()
        cursor.fetchall.return_value = [
            {"runner_name": "runner-a"},
            {"runner_name": "runner-b"},
        ]
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)
        with patch.object(
            db_storage,
            "connect_database",
            return_value=nullcontext(connection),
        ):
            runners = db_storage._recent_batch_runner_names(config)

        self.assertEqual(runners, ["runner-a", "runner-b"])
        query, params = cursor.execute.call_args.args
        self.assertIn("ORDER BY updated_at DESC", query)
        self.assertEqual(
            params,
            (db_storage.RECENT_BATCH_RUNNER_HISTORY_SIZE,),
        )
        self.assertEqual(db_storage.RECENT_BATCH_RUNNER_HISTORY_SIZE, 5)

    def test_output_samples_merge_dataset_and_producer_inputs(self) -> None:
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
            db_storage.fetch_output_sample_rows(
                config,
                dataset="output/test_runner@0.1.0/example_set1",
            )

        query = cursor.execute.call_args.args[0]
        self.assertIn(
            "COALESCE(samples.inputs_json, '{}'::jsonb)",
            query,
        )
        self.assertIn("|| COALESCE(", query)
        self.assertIn("producer_jobs.request_json", query)

    def test_explicit_dataset_overrides_different_candidate_input(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        data_row = {
            "dataset_name": "override-set",
            "dataset_version": "1",
            "external_key": "override/sample-1",
            "sample_id": "sample-1",
            "subset_key": "override",
            "inputs_json": {"image": "/data/override.png"},
            "metadata_json": {"source": "override"},
        }
        candidate_row = {
            "dataset_name": "candidate-set",
            "dataset_version": "1",
            "external_key": "candidate/sample-1",
            "sample_id": "sample-1",
            "subset_key": "candidate",
            "inputs_json": {"image": "/data/original.png"},
            "metadata_json": {"source": "original"},
            "outputs_json": {"sample-1": {"scene": "/output/scene.bin"}},
            "output_metadata_json": {},
            "source_job_id": "job-generator",
        }
        cursor = Mock()
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)
        with (
            patch.object(db_storage, "sync_runner_state"),
            patch.object(
                db_storage,
                "_target_sample_rows",
                side_effect=[
                    (False, "override-set", [data_row]),
                    (True, "candidate-set", [candidate_row]),
                ],
            ),
            patch.object(
                db_storage,
                "connect_database",
                return_value=nullcontext(connection),
            ),
            patch.object(db_storage, "generated_identifier", return_value="job-1"),
            patch.object(
                db_storage,
                "find_reusable_job",
                return_value=None,
            ),
            patch.object(db_storage, "insert_resolved_job_row") as insert_job,
        ):
            db_storage.insert_jobs(
                config,
                dataset="override-set/override/sample-1",
                candidate="output/test_runner@0.1.0/candidate-set/candidate/sample-1",
                references=[],
                runner=runner,
                job_type="evaluation",
                parameters={},
                timeout_seconds=60,
                source_job_id=None,
                allow_start_outside_window=False,
            )

        kwargs = insert_job.call_args.kwargs
        self.assertEqual(
            kwargs["inputs"],
            {
                "data": {"sample-1": {"image": "/data/override.png"}},
                "candidate": {"sample-1": {"scene": "/output/scene.bin"}},
            },
        )
        self.assertEqual(
            kwargs["identity"]["metadata_json"],
            {"source": "override"},
        )
        self.assertEqual(kwargs["source_job_id"], "job-generator")

    def test_job_add_reuses_active_job_unless_rerun(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        sample_row = {
            "dataset_name": "example_set1",
            "dataset_version": "1",
            "external_key": "sample-1",
            "sample_id": "sample-1",
            "subset_key": "",
            "inputs_json": {"image": "/data/sample-1.png"},
            "metadata_json": {},
        }
        candidate_row = {
            **sample_row,
            "outputs_json": {
                "sample-1": {"scene": "/output/sample-1.bin"}
            },
            "output_metadata_json": {},
            "source_job_id": "job-generator",
        }
        connection = Mock()
        connection.cursor.return_value = nullcontext(Mock())

        with (
            patch.object(db_storage, "sync_runner_state"),
            patch.object(
                db_storage,
                "_target_sample_rows",
                side_effect=[
                    (False, "example_set1", [sample_row]),
                    (True, "example_set1", [candidate_row]),
                    (False, "example_set1", [sample_row]),
                    (True, "example_set1", [candidate_row]),
                ],
            ),
            patch.object(
                db_storage,
                "connect_database",
                return_value=nullcontext(connection),
            ),
            patch.object(
                db_storage,
                "find_reusable_job",
                return_value={"job_id": "job-active", "status": "running"},
            ) as find_reusable,
            patch.object(db_storage, "insert_resolved_job_row") as insert_job,
        ):
            reused = db_storage.insert_jobs(
                config,
                dataset="example_set1",
                candidate="output/test_runner@0.1.0/example_set1",
                references=[],
                runner=runner,
                job_type="generation",
                parameters={},
                timeout_seconds=60,
                source_job_id=None,
                allow_start_outside_window=False,
            )
            rerun = db_storage.insert_jobs(
                config,
                dataset="example_set1",
                candidate="output/test_runner@0.1.0/example_set1",
                references=[],
                runner=runner,
                job_type="generation",
                parameters={},
                timeout_seconds=60,
                source_job_id=None,
                allow_start_outside_window=False,
                rerun=True,
            )

        self.assertEqual(reused["created_job_count"], 0)
        self.assertEqual(reused["reused_job_count"], 1)
        self.assertTrue(reused["jobs"][0]["reused"])
        self.assertEqual(reused["jobs"][0]["state"], "running")
        self.assertEqual(rerun["created_job_count"], 1)
        self.assertEqual(rerun["reused_job_count"], 0)
        self.assertFalse(rerun["jobs"][0]["reused"])
        find_reusable.assert_called_once()
        insert_job.assert_called_once()

    def test_pipeline_stage_links_reused_completed_job(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        cursor = Mock()
        cursor.fetchone.return_value = {
            "pipeline_stage_execution_id": "stage-1"
        }
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)

        with (
            patch.object(
                pipeline_storage,
                "connect_database",
                return_value=nullcontext(connection),
            ),
            patch.object(
                pipeline_storage,
                "find_reusable_job",
                return_value={"job_id": "job-completed", "status": "completed"},
            ),
            patch.object(
                pipeline_storage,
                "insert_resolved_job_row",
            ) as insert_job,
            patch.object(
                pipeline_storage,
                "Jsonb",
                side_effect=lambda value: value,
            ),
        ):
            job_id = pipeline_storage.insert_pipeline_stage_job(
                config,
                pipeline_run_id="pipeline-1",
                stage_id="generate",
                lane_index=0,
                lane={},
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "1",
                    "external_key": "sample-1",
                    "sample_id": "sample-1",
                    "metadata_json": {},
                },
                inputs={
                    "data": {
                        "sample-1": {"image": "/data/sample-1.png"}
                    }
                },
                parameters={},
                timeout_seconds=60,
                allow_start_outside_window=False,
                job_type="generation",
                source_job_id=None,
            )

        self.assertEqual(job_id, "job-completed")
        insert_job.assert_not_called()
        update_query, update_params = cursor.execute.call_args.args
        self.assertIn("UPDATE pipeline_stage_executions", update_query)
        self.assertEqual(update_params[0], "job-completed")

    def test_pipeline_stage_links_matching_job_from_same_run(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        cursor = Mock()
        cursor.fetchone.return_value = {
            "pipeline_stage_execution_id": "stage-2"
        }
        connection = Mock()
        connection.cursor.return_value = nullcontext(cursor)

        with (
            patch.object(
                pipeline_storage,
                "connect_database",
                return_value=nullcontext(connection),
            ),
            patch.object(
                pipeline_storage,
                "find_reusable_job",
                return_value={"job_id": "job-active", "status": "pending"},
            ) as find_matching,
            patch.object(
                pipeline_storage,
                "insert_resolved_job_row",
            ) as insert_job,
            patch.object(
                pipeline_storage,
                "Jsonb",
                side_effect=lambda value: value,
            ),
        ):
            job_id = pipeline_storage.insert_pipeline_stage_job(
                config,
                pipeline_run_id="pipeline-1",
                stage_id="generate",
                lane_index=1,
                lane={"first": 1, "second": "b"},
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "1",
                    "external_key": "sample-1",
                    "sample_id": "sample-1",
                    "metadata_json": {},
                },
                inputs={
                    "data": {
                        "sample-1": {"image": "/data/sample-1.png"}
                    }
                },
                parameters={"first": 1},
                timeout_seconds=60,
                allow_start_outside_window=False,
                job_type="generation",
                source_job_id=None,
                rerun=True,
            )

        self.assertEqual(job_id, "job-active")
        find_matching.assert_called_once()
        self.assertEqual(find_matching.call_args.kwargs["pipeline_run_id"], "pipeline-1")
        self.assertEqual(find_matching.call_args.kwargs["stage_id"], "generate")
        insert_job.assert_not_called()
        update_query, update_params = cursor.execute.call_args.args
        self.assertIn("UPDATE pipeline_stage_executions", update_query)
        self.assertEqual(update_params[0], "job-active")

    def test_matching_pipeline_job_includes_active_jobs_from_same_stage(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        cursor = Mock()
        cursor.fetchone.return_value = {"job_id": "job-active", "status": "running"}

        with patch.object(db_storage, "Jsonb", side_effect=lambda value: value):
            reusable_job = db_storage.find_reusable_job(
                cursor,
                pipeline_run_id="pipeline-1",
                stage_id="generate",
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "1",
                    "external_key": "sample-1",
                    "sample_id": "sample-1",
                    "metadata_json": {},
                },
                inputs={
                    "data": {
                        "sample-1": {"image": "/data/sample-1.png"}
                    }
                },
                parameters={"first": 1},
                job_type="generation",
            )

        self.assertEqual(
            reusable_job,
            {"job_id": "job-active", "status": "running"},
        )
        query, parameters = cursor.execute.call_args.args
        self.assertIn("job.pipeline_run_id = %s", query)
        self.assertIn("owner_stage.stage_id = %s", query)
        self.assertIn("'pending', 'running', 'completed'", query)
        self.assertIn("pipeline-1", parameters)
        self.assertIn("generate", parameters)

    def test_reuse_lookup_requires_an_intact_completed_result(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        cursor = Mock()
        cursor.fetchone.return_value = {
            "job_id": "job-completed",
            "status": "completed",
        }

        with patch.object(db_storage, "Jsonb", side_effect=lambda value: value):
            reusable_job = db_storage.find_reusable_job(
                cursor,
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "1",
                    "external_key": "sample-1",
                    "sample_id": "sample-1",
                    "metadata_json": {"projection": "equirectangular"},
                },
                inputs={
                    "candidate": {
                        "sample-1": {"scene": "/output/sample-1.bin"}
                    }
                },
                parameters={"metric": "psnr"},
                job_type="evaluation",
            )

        self.assertEqual(
            reusable_job,
            {"job_id": "job-completed", "status": "completed"},
        )
        query = cursor.execute.call_args.args[0]
        self.assertIn("status = 'completed'", query)
        self.assertIn("job.status IN ('pending', 'running')", query)
        self.assertIn("job.pipeline_run_id IS NULL", query)
        self.assertIn("outputs_removed", query)
        self.assertIn("request_json->'inputs'", query)
        self.assertIn("config_json =", query)

    def test_claim_candidates_prefer_other_then_least_recent_runner(
        self,
    ) -> None:
        candidates = [
            {"job_id": "job-a", "runner_name": "runner-a"},
            {"job_id": "job-b", "runner_name": "runner-b"},
            {"job_id": "job-c", "runner_name": "runner-c"},
        ]

        prioritized = db_storage._prioritize_claim_candidates(
            candidates,
            ["runner-a", "runner-b"],
        )
        self.assertEqual(
            [row["runner_name"] for row in prioritized],
            ["runner-c", "runner-b", "runner-a"],
        )

        all_recent = db_storage._prioritize_claim_candidates(
            candidates,
            ["runner-a", "runner-b", "runner-c"],
        )
        self.assertEqual(
            [row["runner_name"] for row in all_recent],
            ["runner-c", "runner-b", "runner-a"],
        )

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

    def test_dataset_download_command_accepts_rescan_override(self) -> None:
        parser = build_parser()
        default = parser.parse_args(["dataset", "download", "example_set1"])
        enabled = parser.parse_args(
            ["dataset", "download", "example_set1", "--rescan", "true"]
        )
        disabled = parser.parse_args(
            ["dataset", "download", "example_set1", "--rescan", "false"]
        )
        self.assertIsNone(default.rescan)
        self.assertTrue(enabled.rescan)
        self.assertFalse(disabled.rescan)

    def test_runner_rescan_after_download_defaults_true_and_accepts_false(
        self,
    ) -> None:
        config = load_config(str(self.config_path))
        self.assertTrue(
            config.runners["test_runner@0.1.0"].rescan_after_download
        )
        runner_path = self.config_path.parent / "runners" / "test.yaml"
        runner_path.write_text(
            RUNNER_CONFIG.replace(
                "    inputs:\n",
                "    rescan_after_download: false\n    inputs:\n",
            ),
            encoding="utf-8",
        )
        config = load_config(str(self.config_path))
        self.assertFalse(
            config.runners["test_runner@0.1.0"].rescan_after_download
        )

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
            patch.object(
                db_storage,
                "insert_resolved_job_row",
            ) as insert_job,
        ):
            insert_dataset_download_job(
                config,
                dataset_name="example_set1",
                runner=runner,
                parameters={},
                timeout_seconds=60,
                allow_start_outside_window=False,
                rescan_after_download=False,
            )
        sync_runners.assert_called_once_with(config)
        sync_datasets.assert_not_called()
        self.assertFalse(
            insert_job.call_args.kwargs["rescan_after_download"]
        )

    def test_dataset_download_request_stores_rescan_flag(self) -> None:
        config = load_config(str(self.config_path))
        runner = replace(
            config.runners["test_runner@0.1.0"],
            rescan_after_download=False,
        )
        with patch.object(
            db_storage,
            "Jsonb",
            side_effect=lambda value: value,
        ):
            inherited_request = db_storage.insert_resolved_job_row(
                Mock(),
                job_id="job-download",
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "unversioned",
                    "external_key": "",
                    "subset_key": "",
                    "sample_id": None,
                    "metadata_json": {"dataset_download": True},
                },
                inputs={},
                parameters={"dataset_name": "example_set1"},
                timeout_seconds=60,
                job_type="dataset_download",
                source_job_id=None,
                allow_start_outside_window=False,
                now="2026-07-30T00:00:00Z",
            )
            overridden_request = db_storage.insert_resolved_job_row(
                Mock(),
                job_id="job-download-override",
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "unversioned",
                    "external_key": "",
                    "subset_key": "",
                    "sample_id": None,
                    "metadata_json": {"dataset_download": True},
                },
                inputs={},
                parameters={"dataset_name": "example_set1"},
                timeout_seconds=60,
                job_type="dataset_download",
                source_job_id=None,
                allow_start_outside_window=False,
                now="2026-07-30T00:00:00Z",
                rescan_after_download=True,
            )
        self.assertFalse(
            inherited_request["job"]["rescan_after_download"]
        )
        self.assertTrue(
            overridden_request["job"]["rescan_after_download"]
        )

    def test_job_request_includes_primary_output_metadata(self) -> None:
        config = load_config(str(self.config_path))
        runner = config.runners["test_runner@0.1.0"]
        with patch.object(
            db_storage,
            "Jsonb",
            side_effect=lambda value: value,
        ):
            request = db_storage.insert_resolved_job_row(
                Mock(),
                job_id="job-evaluation",
                runner=runner,
                identity={
                    "dataset_name": "example_set1",
                    "dataset_version": "1",
                    "external_key": "sample-1",
                    "subset_key": "",
                    "sample_id": "sample-1",
                    "metadata_json": {},
                },
                inputs={},
                parameters={},
                timeout_seconds=60,
                job_type="evaluation",
                source_job_id="job-generation",
                allow_start_outside_window=False,
                now="2026-08-06T00:00:00Z",
                primary_output_metadata={
                    "scene_scale": 0.7,
                    "scene_coordinate_system": "rub",
                },
            )

        self.assertEqual(
            request["job"]["primary_output_metadata"],
            {"scene_scale": 0.7, "scene_coordinate_system": "RUB"},
        )

    def test_completed_dataset_download_rescans_only_when_enabled(
        self,
    ) -> None:
        terminal = {
            "state": "finished",
            "updated_at": "2026-07-30T00:00:00Z",
            "result": {"status": "completed"},
        }
        base_job = {
            "job_id": "job-download",
            "job_type": "dataset_download",
            "parameters": {"dataset_name": "example_set1"},
        }
        completed = {"status": "completed"}
        with (
            patch.object(
                dispatch_execution,
                "write_job_terminal_result",
                return_value=completed,
            ),
            patch.object(
                dispatch_execution,
                "sync_dataset_state",
                return_value={
                    "dataset": "example_set1",
                    "dataset_count": 1,
                    "sample_count": 2,
                },
            ) as sync_datasets,
        ):
            dispatch_execution._record_terminal_job(
                Mock(),
                batch_id="batch-1",
                job=SimpleNamespace(
                    job_id="job-download",
                    output_dir=Path("/data/output/download"),
                    request_payload={
                        "job": {
                            **base_job,
                            "rescan_after_download": True,
                        }
                    },
                ),
                terminal=terminal,
            )
            sync_datasets.assert_called_once_with(
                ANY,
                dataset_name="example_set1",
            )
            sync_datasets.reset_mock()
            dispatch_execution._record_terminal_job(
                Mock(),
                batch_id="batch-2",
                job=SimpleNamespace(
                    job_id="job-download-no-rescan",
                    output_dir=Path("/data/output/download"),
                    request_payload={
                        "job": {
                            **base_job,
                            "rescan_after_download": False,
                        }
                    },
                ),
                terminal=terminal,
            )
            sync_datasets.assert_not_called()

    def test_running_job_recovery_resumes_matching_runner(self) -> None:
        job = SimpleNamespace(
            job_id="job-1",
            request_payload={
                "job": {
                    "timeout_seconds": 3600,
                }
            },
        )
        plan = SimpleNamespace(batch_id="batch-1")
        running = {
            "batch_id": "batch-1",
            "state": "running",
            "current_job_id": "job-1",
        }
        terminal = {
            **running,
            "state": "finished",
            "result": {"status": "completed"},
        }
        with (
            patch.object(dispatch_execution, "get_status", return_value=running),
            patch.object(
                dispatch_execution,
                "wait_for_terminal_state",
                return_value=terminal,
            ) as wait_terminal,
        ):
            recovered = dispatch_execution._recover_terminal_if_available(
                endpoint="http://runner:58090",
                plan=plan,
                job=job,
                polling=Mock(),
            )

        self.assertEqual(recovered, terminal)
        wait_terminal.assert_called_once()

    def test_schema_preserves_running_job_and_pipeline_state(self) -> None:
        schema = db_storage.SCHEMA_SQL
        self.assertIn("started_at TIMESTAMPTZ", schema)
        self.assertIn("running_job_count INTEGER", schema)
        self.assertNotIn(
            "UPDATE pipeline_runs\nSET status = 'pending'",
            schema,
        )

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

    def test_scene_scale_must_be_nonzero_and_finite(self) -> None:
        self.assertEqual(
            db_storage.normalize_output_metadata({"scene_scale": 0.7}),
            {"scene_scale": 0.7},
        )
        self.assertEqual(
            db_storage.normalize_output_metadata({"scene_scale": -0.7}),
            {"scene_scale": -0.7},
        )
        for value in (True, 0, float("inf"), float("nan"), "0.7"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "scene_scale"):
                    db_storage.normalize_output_metadata({"scene_scale": value})

    def test_scene_coordinate_system_is_normalized_and_validated(self) -> None:
        self.assertEqual(
            db_storage.normalize_output_metadata(
                {"scene_scale": 0.7, "scene_coordinate_system": " rub "}
            ),
            {"scene_scale": 0.7, "scene_coordinate_system": "RUB"},
        )
        for value in (None, "", "   ", 1, {}):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "scene_coordinate_system"):
                    db_storage.normalize_output_metadata(
                        {"scene_coordinate_system": value}
                    )

    def test_output_sample_stores_runner_output_metadata(self) -> None:
        cursor = Mock()
        with patch.object(
            db_storage,
            "Jsonb",
            side_effect=lambda value: value,
        ):
            db_storage._upsert_output_sample(
                cursor,
                row={
                    "job_id": "job-generation",
                    "runner_selector": "generator@1.0.0",
                    "runner_name": "generator",
                    "runner_version": "1.0.0",
                    "dataset_name": "example_set1",
                    "dataset_version": "1",
                    "external_key": "sample-1",
                    "subset_key": "",
                    "sample_id": "sample-1",
                    "sample_metadata_json": {"projection": "equirectangular"},
                },
                output_dir="/data/output/generator/sample-1",
                output_files={"sample-1": {"3dgs": "scene.ply"}},
                output_metadata={
                    "scene_scale": 0.7,
                    "scene_coordinate_system": "RUB",
                },
                now="2026-08-06T00:00:00Z",
            )

        parameters = cursor.execute.call_args.args[1]
        self.assertEqual(
            parameters[11]["output_metadata"],
            {"scene_scale": 0.7, "scene_coordinate_system": "RUB"},
        )

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

    def test_script_workspace_keeps_contents_when_metadata_is_unsupported(
        self,
    ) -> None:
        root = Path(self.temp_dir.name)
        workspace = root / "workspace-metadata-unsupported"
        pipeline_root = root / "pipelines-metadata-unsupported"
        workspace.mkdir()
        (workspace / "report.txt").write_text("done\n", encoding="utf-8")

        with patch(
            "execution.script_run.shutil.copystat",
            side_effect=PermissionError("metadata denied"),
        ):
            result = _publish_script_workspace(
                workspace=workspace,
                initial_files=set(),
                pipeline_root=pipeline_root,
                publish_dir=Path("example/run"),
                retention="keep",
            )

        self.assertEqual(result, {})
        self.assertEqual(
            (pipeline_root / "example/run/report.txt").read_text(
                encoding="utf-8"
            ),
            "done\n",
        )


if __name__ == "__main__":
    unittest.main()

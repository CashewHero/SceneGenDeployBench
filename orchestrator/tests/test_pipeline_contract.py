from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from cli.commands import _validate_nested_stage, parse_key_value_settings
from domain.pipelines import (
    load_pipeline,
    merge_pipeline_inputs,
    merge_pipeline_parameters,
    resolve_static_value,
)
from main import build_parser
from execution.pipelines import (
    _child_pipeline_config,
    _dependency_rows,
    _effective_retention,
    _resolve_runtime_value,
    _script_context,
    _script_execution_directory,
    _stage_execution_lanes,
    _stage_output_sources,
    cleanup_pipeline_outputs,
)


class PipelineContractTests(unittest.TestCase):
    def test_cli_validation_accepts_dynamic_nested_matrix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog = Path(temp_dir)
            (catalog / "child.yaml").write_text(
                """
                pipeline_version: 1
                name: child
                matrix:
                  trajectory: [default]
                stages:
                  prepare:
                    image: python:3
                    run: [python, -c, pass]
                """,
                encoding="utf-8",
            )
            config = SimpleNamespace(
                catalogs=SimpleNamespace(pipelines=catalog)
            )

            _validate_nested_stage(
                config,
                {
                    "pipeline": "child",
                    "matrix": "${{ stages.discover.outputs.matrix }}",
                },
            )

    def test_pipeline_runner_default_and_cli_override_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "runner-input.yaml"
            path.write_text(
                """
                pipeline_version: 1
                name: runner_input
                dataset: test-data
                runner: test_runner
                stages:
                  generate:
                    runner: ${{ runner }}
                    inputs:
                      data: ${{ dataset }}
                    with:
                      selected_runner: ${{ runner }}
                """,
                encoding="utf-8",
            )

            definition = load_pipeline(path)

        self.assertEqual(
            definition.inputs,
            {"dataset": "test-data", "runner": "test_runner"},
        )
        self.assertEqual(definition.raw["runner"], "test_runner")
        self.assertEqual(definition.stages["generate"]["runner"], "${{ runner }}")
        resolved_inputs = merge_pipeline_inputs(
            definition.inputs,
            {"dataset": "other-data", "runner": "other_runner@0.2.0"},
        )
        self.assertEqual(
            resolved_inputs,
            {"dataset": "other-data", "runner": "other_runner@0.2.0"},
        )
        self.assertEqual(
            resolve_static_value(
                definition.stages["generate"]["with"],
                inputs=resolved_inputs,
                lane={},
            ),
            {"selected_runner": "other_runner@0.2.0"},
        )

        args = build_parser().parse_args(
            ["pipeline", "add", "runner_input", "--runner", "other_runner"]
        )
        self.assertEqual(args.runner, "other_runner")

    def test_rerun_flag_is_shared_by_job_and_pipeline_add(self) -> None:
        parser = build_parser()

        self.assertFalse(
            parser.parse_args(["job", "add", "--dataset", "test-data"]).rerun
        )
        self.assertTrue(
            parser.parse_args(
                ["job", "add", "--dataset", "test-data", "--rerun"]
            ).rerun
        )
        self.assertFalse(parser.parse_args(["pipeline", "add", "example"]).rerun)
        self.assertTrue(
            parser.parse_args(["pipeline", "add", "example", "--rerun"]).rerun
        )

    def test_pipeline_runner_reference_requires_a_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError, "pipeline input 'runner' is not defined"
        ):
            resolve_static_value(
                "${{ runner }}",
                inputs={"dataset": "test-data", "runner": None},
                lane={},
            )

    def test_pipeline_parameters_resolve_and_cli_accepts_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "parameters.yaml"
            path.write_text(
                """
                pipeline_version: 1
                name: parameters
                parameters:
                  reference_count: 60
                  save_outputs: false
                stages:
                  prepare:
                    image: python:3
                    env:
                      REFERENCE_COUNT: ${{ parameters.reference_count }}
                    run:
                      - python
                      - -c
                      - pass
                """,
                encoding="utf-8",
            )
            definition = load_pipeline(path)

        parameters = merge_pipeline_parameters(
            definition.parameters,
            {"reference_count": 80},
        )
        self.assertEqual(
            resolve_static_value(
                definition.stages["prepare"]["env"],
                inputs=definition.inputs,
                lane={},
                parameters=parameters,
            ),
            {"REFERENCE_COUNT": 80},
        )
        with self.assertRaisesRegex(ValueError, "has no parameters: missing"):
            merge_pipeline_parameters(definition.parameters, {"missing": 1})

        args = build_parser().parse_args(
            ["pipeline", "add", "parameters", "--set", "reference_count=80"]
        )
        self.assertEqual(args.settings, ["reference_count=80"])
        self.assertEqual(
            parse_key_value_settings(
                ["initial_scene_scale_overwrite=-0.7"],
                {"initial_scene_scale_overwrite": None},
            ),
            {"initial_scene_scale_overwrite": -0.7},
        )
        self.assertEqual(
            resolve_static_value(
                "${{ parameters.settings.reference_count }}",
                inputs=definition.inputs,
                lane={},
                parameters={"settings": {"reference_count": 12}},
            ),
            12,
        )
        self.assertEqual(
            resolve_static_value(
                "${{ matrix.case.dataset }}",
                inputs=definition.inputs,
                lane={"case": {"dataset": "nested-data"}},
            ),
            "nested-data",
        )

    def test_nested_pipeline_stage_uses_with_and_rejects_retention(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "parent.yaml"
            path.write_text(
                """
                pipeline_version: 1
                name: parent
                dataset: test-data
                runner: test_runner
                matrix:
                  seed: [1, 2]
                parameters:
                  reference_count: 60
                stages:
                  child:
                    pipeline: child_pipeline
                    dataset: ${{ dataset }}
                    runner: ${{ runner }}
                    matrix:
                      seed: ["${{ matrix.seed }}"]
                    with:
                      reference_count: ${{ parameters.reference_count }}
                """,
                encoding="utf-8",
            )
            definition = load_pipeline(path)
            stage = definition.stages["child"]
            self.assertEqual(stage["pipeline"], "child_pipeline")
            self.assertEqual(stage["dataset"], "${{ dataset }}")
            self.assertEqual(stage["runner"], "${{ runner }}")
            self.assertEqual(
                stage["with"]["reference_count"],
                "${{ parameters.reference_count }}",
            )
            self.assertEqual(stage["matrix"]["seed"], ["${{ matrix.seed }}"])

            path.write_text(
                """
                pipeline_version: 1
                name: parent
                dataset: test-data
                stages:
                  child:
                    pipeline: child_pipeline
                    retention: pipeline
                """,
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "retention belongs in the child"):
                load_pipeline(path)

    def test_nested_pipeline_stage_resolves_separate_overrides(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog = Path(temp_dir)
            child_path = catalog / "child.yaml"
            child_path.write_text(
                """
                pipeline_version: 1
                name: child
                dataset: child-data
                runner: child-runner
                parameters:
                  reference_count: 10
                matrix:
                  seed: [0]
                stages:
                  prepare:
                    image: python:3
                    run: [python, -c, pass]
                """,
                encoding="utf-8",
            )
            stage = {
                "pipeline": "child",
                "dataset": "${{ dataset }}",
                "runner": "${{ runner }}",
                "matrix": {"seed": ["${{ matrix.seed }}"]},
                "with": {
                    "reference_count": "${{ parameters.reference_count }}"
                },
            }
            run = {
                "pipeline_name": "parent",
                "dataset_target": "parent-data",
                "config_json": {
                    "runner": "parent-runner@0.1.0",
                    "parameters": {"reference_count": 60},
                },
            }
            config = SimpleNamespace(
                catalogs=SimpleNamespace(pipelines=catalog),
            )
            with patch(
                "execution.pipelines._runner",
                return_value=SimpleNamespace(selector="parent-runner@0.1.0"),
            ):
                _, dataset, payload, lanes = _child_pipeline_config(
                    config,
                    run=run,
                    stage=stage,
                    lane={"seed": 2},
                )

        self.assertEqual(dataset, "parent-data")
        self.assertEqual(payload["runner"], "parent-runner@0.1.0")
        self.assertEqual(payload["parameters"], {"reference_count": 60})
        self.assertEqual(payload["matrix"], {"seed": [2]})
        self.assertEqual(lanes, [{"seed": 2}])

    def test_nested_pipeline_stage_uses_dynamic_matrix_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            catalog = Path(temp_dir)
            child_path = catalog / "child.yaml"
            child_path.write_text(
                """
                pipeline_version: 1
                name: child
                dataset: child-data
                runner: child-runner
                matrix:
                  trajectory: ["${{ dataset }}"]
                  profile: [default]
                stages:
                  prepare:
                    image: python:3
                    run: [python, -c, pass]
                    env:
                      DATASET_TARGET: ${{ matrix.trajectory }}
                """,
                encoding="utf-8",
            )
            stage = {
                "pipeline": "child",
                "dataset": "${{ dataset }}",
                "runner": "${{ runner }}",
                "needs": ["discover"],
                "matrix": "${{ stages.discover.outputs.other }}",
                "with": {},
            }
            run = {
                "pipeline_name": "parent",
                "dataset_target": "parent-data",
                "config_json": {
                    "dataset": "parent-data",
                    "runner": "parent-runner@0.1.0",
                    "parameters": {},
                },
            }
            stages = {
                "discover": {"scope": "pipeline"},
                "child": stage,
            }
            rows = [
                {
                    "stage_id": "discover",
                    "lane_index": 0,
                    "status": "completed",
                    "result_json": {
                        "other": {
                            "trajectory": ["data/one", "data/two"]
                        }
                    },
                }
            ]
            config = SimpleNamespace(
                catalogs=SimpleNamespace(pipelines=catalog),
            )
            with patch(
                "execution.pipelines._runner",
                return_value=SimpleNamespace(selector="parent-runner@0.1.0"),
            ):
                _, dataset, payload, lanes = _child_pipeline_config(
                    config,
                    run=run,
                    stage=stage,
                    lane={},
                    stages=stages,
                    stage_rows=rows,
                )
                partial_stage = {
                    **stage,
                    "matrix": {
                        "trajectory": (
                            "${{ stages.discover.outputs.other.trajectory }}"
                        )
                    },
                }
                stages["child"] = partial_stage
                _, _, partial_payload, partial_lanes = _child_pipeline_config(
                    config,
                    run=run,
                    stage=partial_stage,
                    lane={},
                    stages=stages,
                    stage_rows=rows,
                )

        self.assertEqual(dataset, "parent-data")
        self.assertEqual(
            payload["matrix"],
            {"trajectory": ["data/one", "data/two"]},
        )
        self.assertEqual(
            lanes,
            [
                {"trajectory": "data/one"},
                {"trajectory": "data/two"},
            ],
        )
        self.assertEqual(
            partial_payload["matrix"],
            {
                "trajectory": ["data/one", "data/two"],
                "profile": ["default"],
            },
        )
        self.assertEqual(
            partial_lanes,
            [
                {"trajectory": "data/one", "profile": "default"},
                {"trajectory": "data/two", "profile": "default"},
            ],
        )

    def test_stage_output_expressions_are_general_runtime_values(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "parent.yaml"
            path.write_text(
                """
                pipeline_version: 1
                name: parent
                dataset: test-data
                stages:
                  discover:
                    image: python:3
                    run: [python, -c, pass]
                  consume:
                    needs: discover
                    runner: ${{ stages.discover.outputs.config.runner }}
                    inputs: ${{ stages.discover.outputs.config.inputs }}
                    with: ${{ stages.discover.outputs.config.parameters }}
                """,
                encoding="utf-8",
            )

            definition = load_pipeline(path)

        stage = definition.stages["consume"]
        rows = [
            {
                "stage_id": "discover",
                "lane_index": 2,
                "result_json": {
                    "config": {
                        "runner": "test_runner@0.1.0",
                        "inputs": {"data": "test-data/frame"},
                        "parameters": {"count": 4},
                    }
                },
            }
        ]
        common = {
            "inputs": {"dataset": "test-data", "runner": None},
            "lane": {},
            "parameters": {},
            "stage": stage,
            "stages": definition.stages,
            "lane_index": 2,
            "stage_rows": rows,
        }
        self.assertEqual(
            _resolve_runtime_value(stage["runner"], **common),
            "test_runner@0.1.0",
        )
        self.assertEqual(
            _resolve_runtime_value(stage["inputs"], **common),
            {"data": "test-data/frame"},
        )
        self.assertEqual(
            _resolve_runtime_value(
                "${{ stages.discover.outputs }}", **common
            ),
            rows[0]["result_json"],
        )

    def test_stage_output_expression_requires_declared_dependency(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "missing-needs.yaml"
            path.write_text(
                """
                pipeline_version: 1
                name: missing_needs
                stages:
                  discover:
                    image: python:3
                    run: [python, -c, pass]
                  consume:
                    runner: test_runner
                    with:
                      count: ${{ stages.discover.outputs.count }}
                """,
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ValueError, "must be listed in needs"):
                load_pipeline(path)

    def test_runner_stage_may_have_empty_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "download.yaml"
            path.write_text(
                """
                pipeline_version: 1
                name: download
                dataset: test-data
                stages:
                  download:
                    runner: dataset_downloader
                    with:
                      mode: raw
                """,
                encoding="utf-8",
            )

            definition = load_pipeline(path)

        self.assertEqual(definition.stages["download"]["inputs"], {})
        self.assertEqual(definition.stages["download"]["scope"], "matrix")
        self.assertEqual(definition.stages["download"]["retention"], "keep")

    def test_scope_controls_execution_and_dependencies(self) -> None:
        lanes = [{"value": 1}, {"value": 2}]
        stages = {
            "prepare": {"scope": "pipeline", "needs": []},
            "generate": {"scope": "matrix", "needs": ["prepare"]},
            "report": {"scope": "pipeline", "needs": ["generate"]},
        }
        rows = [
            {"stage_id": "prepare", "lane_index": 0},
            {"stage_id": "generate", "lane_index": 0},
            {"stage_id": "generate", "lane_index": 1},
        ]

        self.assertEqual(_stage_execution_lanes(stages["prepare"], lanes), [(0, {})])
        self.assertEqual(
            _dependency_rows(
                rows,
                stage=stages["generate"],
                stages=stages,
                lane_index=1,
            ),
            [rows[0]],
        )
        self.assertEqual(
            _dependency_rows(
                rows,
                stage=stages["report"],
                stages=stages,
                lane_index=0,
            ),
            rows[1:],
        )
        self.assertEqual(
            _effective_retention(
                {"scope": "pipeline", "retention": "matrix"}
            ),
            "pipeline",
        )

    def test_script_directory_uses_lane_only_for_multiple_lanes(self) -> None:
        run = {
            "pipeline_run_id": "pipeline_20260725T120000_deadbeef",
            "pipeline_name": "example",
            "lanes_json": [{"value": 1}, {"value": 2}],
        }
        self.assertEqual(
            _script_execution_directory(
                run,
                stage_id="prepare",
                stage={"scope": "pipeline"},
                lane_index=0,
            ),
            Path("example/20260725T120000/prepare"),
        )
        self.assertEqual(
            _script_execution_directory(
                run,
                stage_id="generate",
                stage={"scope": "matrix"},
                lane_index=1,
            ),
            Path("example/20260725T120000/generate/1"),
        )
        run["lanes_json"] = [{"value": 1}]
        self.assertEqual(
            _script_execution_directory(
                run,
                stage_id="generate",
                stage={"scope": "matrix"},
                lane_index=0,
            ),
            Path("example/20260725T120000/generate"),
        )

    def test_script_outputs_keep_all_samples(self) -> None:
        sources = _stage_output_sources(
            [
                {
                    "status": "completed",
                    "dataset_name": "pipeline",
                    "dataset_version": "run",
                    "external_key": "__script__",
                    "sample_id": "__script__",
                    "job_id": None,
                    "sample_metadata_json": {"pose_coordinate_system": "NED"},
                    "result_json": {
                        "output_files": {
                            "sample-a": {"image": "/data/pipelines/a.png"},
                            "sample-b": {"table": "/data/pipelines/b.csv"},
                        },
                        "output_metadata": {"scene_scale": 0.7},
                    },
                }
            ]
        )

        self.assertEqual(
            [
                (source.identity["sample_id"], source.data)
                for source in sources
            ],
            [
                ("sample-a", {"image": "/data/pipelines/a.png"}),
                ("sample-b", {"table": "/data/pipelines/b.csv"}),
            ],
        )
        self.assertEqual(
            [source.output_metadata for source in sources],
            [{"scene_scale": 0.7}, {"scene_scale": 0.7}],
        )
        self.assertEqual(
            [source.identity["metadata_json"] for source in sources],
            [
                {"pose_coordinate_system": "NED"},
                {"pose_coordinate_system": "NED"},
            ],
        )

    def test_stage_output_sources_only_use_output_files(self) -> None:
        sources = _stage_output_sources(
            [
                {
                    "status": "completed",
                    "result_json": {
                        "target": "example/trajectory/frame_000010"
                    },
                }
            ],
        )

        self.assertEqual(sources, [])

    def test_script_context_includes_dependency_matrix(self) -> None:
        context = _script_context(
            run={
                "pipeline_run_id": "pipeline_20260725T120000_deadbeef",
                "pipeline_name": "example",
                "dataset_target": "example-data",
                "config_json": {"parameters": {"reference_count": 60}},
            },
            stage_id="report",
            stage={"scope": "pipeline"},
            lane_index=0,
            lane={},
            dependencies=[
                {
                    "stage_id": "evaluate",
                    "sample_id": "sample-a",
                    "external_key": "scene/sample-a",
                    "lane_index": 1,
                    "lane_json": {"sigma_px": 2.0},
                    "status": "completed",
                    "job_id": "job-1",
                    "result_json": {"metrics": []},
                }
            ],
        )

        self.assertEqual(
            context["needs"]["evaluate"][0]["matrix"],
            {"sigma_px": 2.0},
        )
        self.assertEqual(
            context["needs"]["evaluate"][0]["lane_index"],
            1,
        )
        self.assertIsNone(context["pipeline"]["runner"])
        self.assertEqual(
            context["pipeline"]["parameters"],
            {"reference_count": 60},
        )

    def test_pipeline_retention_removes_script_folder_and_runner_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = SimpleNamespace(
                storage=SimpleNamespace(
                    pipeline_root=root / "pipelines",
                    output_root=root / "output",
                ),
            )
            run_root = root / "pipelines/example/20260725T120000"
            script_root = run_root / "create"
            script_root.mkdir(parents=True)
            (script_root / "report.txt").write_text("report", encoding="utf-8")
            runner_root = root / "output/runner/dataset/sample"
            runner_root.mkdir(parents=True)
            output_file = runner_root / "scene.ply"
            log_file = runner_root / "runner.log"
            output_file.write_text("scene", encoding="utf-8")
            log_file.write_text("log", encoding="utf-8")
            run = {
                "pipeline_run_id": "pipeline_20260725T120000_deadbeef",
                "pipeline_name": "example",
                "lanes_json": [{}],
                "config_json": {
                    "stages": {
                        "create": {
                            "image": "python:3",
                            "retention": "pipeline",
                        },
                        "generate": {
                            "runner": "runner",
                            "retention": "pipeline",
                        },
                    }
                },
            }
            records = [
                {
                    "stage_id": "generate",
                    "lane_index": 0,
                    "job_id": "job-1",
                    "output_dir": str(runner_root),
                    "result_json": {
                        "output_files": {
                            "sample": {"3dgs": "scene.ply"}
                        }
                    },
                    "artifacts_json": [{"path": "runner.log"}],
                }
            ]

            with (
                patch(
                    "execution.pipelines.fetch_pipeline_job_outputs",
                    return_value=records,
                ),
                patch(
                    "execution.pipelines.mark_pipeline_job_outputs_removed"
                ) as mark_removed,
            ):
                cleanup_pipeline_outputs(config, run)

            self.assertFalse(run_root.exists())
            self.assertFalse(output_file.exists())
            self.assertFalse(log_file.exists())
            mark_removed.assert_called_once_with(config, ["job-1"])

    def test_matrix_retention_removes_only_one_script_lane(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = SimpleNamespace(
                storage=SimpleNamespace(
                    pipeline_root=root / "pipelines",
                    output_root=root / "output",
                ),
            )
            run_root = root / "pipelines/example/20260725T120000/render"
            lane_zero = run_root / "0"
            lane_one = run_root / "1"
            lane_zero.mkdir(parents=True)
            lane_one.mkdir()
            (lane_zero / "result.txt").write_text("zero", encoding="utf-8")
            (lane_one / "result.txt").write_text("one", encoding="utf-8")
            runner_zero = root / "output/runner/dataset/zero"
            runner_one = root / "output/runner/dataset/one"
            runner_zero.mkdir(parents=True)
            runner_one.mkdir()
            (runner_zero / "scene.ply").write_text("zero", encoding="utf-8")
            (runner_one / "scene.ply").write_text("one", encoding="utf-8")
            run = {
                "pipeline_run_id": "pipeline_20260725T120000_deadbeef",
                "pipeline_name": "example",
                "lanes_json": [{"value": 1}, {"value": 2}],
                "config_json": {
                    "stages": {
                        "render": {
                            "image": "python:3",
                            "scope": "matrix",
                            "retention": "matrix",
                        },
                        "generate": {
                            "runner": "runner",
                            "scope": "matrix",
                            "retention": "matrix",
                        }
                    }
                },
            }
            records = [
                {
                    "stage_id": "generate",
                    "lane_index": lane,
                    "job_id": f"job-{lane}",
                    "output_dir": str(output_dir),
                    "result_json": {
                        "output_files": {
                            "sample": {"3dgs": "scene.ply"}
                        }
                    },
                    "artifacts_json": [],
                }
                for lane, output_dir in (
                    (0, runner_zero),
                    (1, runner_one),
                )
            ]

            with (
                patch(
                    "execution.pipelines.fetch_pipeline_job_outputs",
                    return_value=records,
                ),
                patch(
                    "execution.pipelines.mark_pipeline_job_outputs_removed"
                ) as mark_removed,
            ):
                cleanup_pipeline_outputs(
                    config,
                    run,
                    retentions={"matrix"},
                    lane_index=0,
                )

            self.assertFalse(lane_zero.exists())
            self.assertTrue(lane_one.exists())
            self.assertFalse((runner_zero / "scene.ply").exists())
            self.assertTrue((runner_one / "scene.ply").exists())
            mark_removed.assert_called_once_with(config, ["job-0"])

    def test_matrix_retention_defers_shared_runner_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = SimpleNamespace(
                storage=SimpleNamespace(
                    pipeline_root=root / "pipelines",
                    output_root=root / "output",
                ),
            )
            runner_root = root / "output/runner/dataset/shared"
            runner_root.mkdir(parents=True)
            output_file = runner_root / "scene.ply"
            output_file.write_text("shared", encoding="utf-8")
            run = {
                "pipeline_run_id": "pipeline_20260725T120000_deadbeef",
                "pipeline_name": "example",
                "lanes_json": [{"value": 1}, {"value": 2}],
                "config_json": {
                    "stages": {
                        "generate": {
                            "runner": "runner",
                            "scope": "matrix",
                            "retention": "matrix",
                        }
                    }
                },
            }
            records = [
                {
                    "stage_id": "generate",
                    "lane_index": 0,
                    "job_id": "job-shared",
                    "output_dir": str(runner_root),
                    "result_json": {
                        "output_files": {
                            "sample": {"3dgs": "scene.ply"}
                        }
                    },
                    "artifacts_json": [],
                    "shared": True,
                }
            ]

            with (
                patch(
                    "execution.pipelines.fetch_pipeline_job_outputs",
                    return_value=records,
                ),
                patch(
                    "execution.pipelines.mark_pipeline_job_outputs_removed"
                ) as mark_removed,
            ):
                cleanup_pipeline_outputs(
                    config,
                    run,
                    retentions={"matrix"},
                    lane_index=0,
                )
                self.assertTrue(output_file.exists())
                mark_removed.assert_not_called()

                cleanup_pipeline_outputs(
                    config,
                    run,
                    retentions={"matrix"},
                )

            self.assertFalse(output_file.exists())
            mark_removed.assert_called_once_with(config, ["job-shared"])


if __name__ == "__main__":
    unittest.main()

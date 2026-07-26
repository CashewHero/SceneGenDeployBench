# Database

PostgreSQL stores durable benchmark state.

## Tables

- `samples`: dataset samples and normalized input paths
- `runners`: runner catalog entries
- `jobs`: durable execution records
- `batches`: batch membership and runner endpoint metadata
- `job_metrics`: scalar metrics
- `output_samples`: generated outputs available as downstream inputs
- `pipeline_runs`: durable pipeline-level state
- `pipeline_stage_executions`: one matrix-lane/sample execution of a stage

## Relationships

```text
samples ----< jobs >---- runners
               |
               +---- batches
               +---- job_metrics
               +---- output_samples

pipeline_runs ----< pipeline_stage_executions
       |                       |
       +----< jobs >------------+
```

## Key Fields

`samples`:

- `dataset_name`
- `dataset_version`
- `external_key`
- `inputs_json`
- `data_types_json`
- `metadata_json`

`runners`:

- `selector`
- `runner_name`
- `runner_type`
- `version`
- `contract_version`
- `inputs_json`
- `launcher_driver`
- `container_image`

`jobs`:

- `job_id`
- `runner_selector`
- `job_type`
- `status`
- `attempt_count`
- `request_json`
- `result_json`
- `artifacts_json`
- `output_dir`
- `failure_code`
- `failure_message`

`batches`:

- `batch_id`
- `runner_selector`
- `runner_endpoint`
- `job_ids_json`
- per-state job counts

`job_metrics`:

- `job_id`
- `metric_namespace`
- `metric_name`
- `metric_type`
- `numeric_value`
- `text_value`
- `unit`
- `source`

`output_samples`:

- `source_job_id`
- `source_runner_selector`
- original dataset identity (`dataset_name`, `dataset_version`, `external_key`)
- `outputs_json`: reusable files keyed by id and then semantic data type
- `data_types_json`

`pipeline_runs`:

- `pipeline_run_id`
- `pipeline_name`
- `status`
- `dataset_target`
- normalized pipeline config and matrix lanes

`pipeline_stage_executions`:

- `pipeline_stage_execution_id`
- `pipeline_run_id`
- `stage_id`
- `lane_index`
- dataset sample identity
- `job_id` for runner-backed stages
- local status/result fields for script stages

## Dataset Manifest Data

Manifest sample keys become semantic data types in `samples.inputs_json`. `data_types` is optional dataset-level metadata; samples may still contain additional data keys:

```yaml
data_types: [image, depth]  # dataset-level advertised types
```

Optional dataset-level camera defaults can live in inherited `metadata` and are passed to runners as `job.primary_sample_metadata`:

```yaml
metadata:
  projection: equirectangular
  pose_convention: camera_to_world
  pose_coordinate_system: NED
  pose_units: meters
  resolution: [2560, 1280]
  fov: [360, 180]
```

`camera_pose` is frame-specific. It may be inline YAML or a YAML/JSON path, and is passed as normalized JSON. See [Camera Pose Inputs](../runner_wrapper/docs/camera_pose.md) for supported convention, coordinate-system, units, and projection values:

```yaml
camera_pose:
  position: [0.0, 0.0, 0.0]
  rotation_quaternion_xyzw: [0, 0, 0, 1]
```

`camera_trajectory` should stay file-backed. The YAML/JSON file should contain its own camera defaults and `frames[]` entries. See [Camera Trajectory Inputs](../runner_wrapper/docs/camera_trajectory.md) for the runner-facing contract.

## Values

Runner types:

- `generator`
- `evaluator`
- `dataset_downloader`

Job types:

- `generation`
- `evaluation`
- `dataset_download`

Job statuses:

- `pending`
- `completed`
- `failed`
- `cancelled`

## Boundaries

- PostgreSQL stores metadata and state.
- Artifact bytes stay on disk.
- Pipeline scripts use `/data/pipelines` for explicitly retained files.
- Runner live state is polled from the runner process.
- Missing file-backed samples/runners are flagged, not immediately deleted.

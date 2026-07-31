# Orchestration

## Service

The long-lived orchestrator synchronizes the runner catalog, reconciles pipelines, claims pending jobs, dispatches batches, and persists results.

Service endpoints:

- `GET /status`
- `GET /config`
- `POST /shutdown`

## Config

Primary files:

- `config/system.yaml`
- `config/runners/*.yaml`
- `config/pipelines/*.yaml`

Useful environment variables:

- `PATH_CONFIG_SYSTEM`
- `PATH_DATASETS`
- `PATH_MODEL_CACHE`
- `PATH_OUTPUT`
- `PATH_PIPELINES`
- `ORCHESTRATOR_PORT`
- `PG_DB_HOST`, `PG_DB_PORT`, `PG_DB_NAME`, `PG_DB_USER`, `PG_DB_PASSWORD`
- `POLL_STARTUP_SECONDS`, `POLL_POST_SUBMIT_SECONDS`, `POLL_RUNNING_SECONDS`
- `ORCH_SCHEDULING_MAX_ATTEMPTS`

## Runner Selection

- Concrete identity: `runner@version`
- `runner@latest` resolves to the catalog entry marked `latest: true`
- Bare runner names resolve to latest, then newest configured version
- `--runner` may be omitted only when the config has one runner name

## Dataset Discovery

Manifest mode uses `manifest.yaml` or `manifest.yml`.

File-scan mode treats every matched file as an `image` sample.

Manifest sample keys that are not reserved become data types, such as:

- `image`
- `depth`
- `camera_pose`
- `scene`
- `mesh`

Reserved keys:

- `metadata`, `tags`, `path`, `samples`, `subsets`, `manifest`
- `path_prefix`, `dataset_name`, `dataset_version`
- `data_types`, `external_key`, `sample_id`
- keys beginning with `_`

See [Database](database.md#dataset-manifest-data) for data-type details such as `camera_pose` and `camera_trajectory`.

## Batch Lifecycle

1. Select eligible pending jobs.
2. Create or reuse a durable batch row.
3. Assign `runtime.output_dir`.
4. Resolve and start/connect to the runner.
5. Submit jobs sequentially with `POST /run-job`.
6. Poll `/status` until each job is `finished` or `failed`.
7. Persist result JSON, metrics, and artifact metadata.
8. Shut down the runner when the batch exits.

The scheduler remembers runner names used by the five most recently batches. It prefers a pending runner outside that history or the least recently used runner.

Generator output path:

```text
/data/output/<runner>@<version>/<dataset>/<subset>/<sample>
```

Evaluator output path:

```text
/data/output/<evaluator>@<version>/<generator>@<version>/<dataset>/<subset>/<sample>
```

Dataset downloader output path:

```text
/data/output/<dataset_downloader>@<version>/<dataset>
```

The downloader may organize artifacts below this directory using its own job parameters. Dataset downloaders write dataset files under `/data/datasets/<dataset>` and produce `manifest.yaml`.

## Retries

- `orchestrator.scheduling.max_attempts` sets the default retry budget.
- `runner.scheduling.max_attempts` overrides it per runner.
- Failed jobs return to `pending` until their attempt budget is exhausted.
- Final failure is persisted when the budget is exhausted.

## Scheduling Windows

Runner scheduling can define:

- `timezone`
- `allowed_windows`
- `max_batch_size`
- `max_attempts`
- `job_timeout_minutes`
- `startup_timeout_minutes`

`max_batch_size` limits how many pending jobs the scheduler claims for one runner batch. It does not limit how many jobs `job add` creates. For an eight-hour timeout budget, choose values where `max_batch_size * job_timeout_minutes <= 480`.

Window policies:

- `start_policy: open`
- `end_policy: finish_batch | finish_job | end_now`

Per-job scheduling overrides are documented under [CLI Jobs](cli.md#jobs).

## Launchers

`static_http`:

- uses `launcher.endpoint.base_url`
- connects to an existing runner process

`docker`:

- uses Docker socket `/var/run/docker.sock`
- pulls `launcher.image` when it is missing from the Docker host, then starts it
- mounts datasets read-only for generators/evaluators, read-write for dataset downloaders, and output read-write
- mounts `/data/model_cache` read-write for reusable model assets
- passes `RUNNER_NAME`, `RUNNER_TYPE`, `RUNNER_VERSION`, `RUNNER_CONTRACT_VERSION`, `RUNNER_PORT`, plus `launcher.env`
- passes variables listed in `launcher.env_passthrough`; values come from `orchestrator.runner_env` first, then the orchestrator process environment
- YAML strings support `${VAR:-default}` environment expansion

## Storage Boundary

The orchestrator records assigned output paths, runner-reported `output_files`, and administrative artifacts. It does not scan, modify, or reconcile files under `/data/output`.

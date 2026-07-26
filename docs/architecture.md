# Architecture

## Runtime

```text
Config + Datasets -> Orchestrator -> Runner Launcher -> Runner
                           |                |              |
                           v                v              v
                      PostgreSQL       Docker/static     shared storage
```

## Components

- **Orchestrator**: loads config, syncs reference data, creates jobs, dispatches batches, and persists terminal results.
- **Runner**: exposes the runner HTTP API, executes one adapter, and returns reusable `output_files`, metrics, administrative artifacts, and failures.
- **PostgreSQL**: stores samples, runners, jobs, batches, pipeline runs, metrics, and result metadata.
- **Filesystem**: stores inputs under `/data/datasets`, reusable models under `/data/model_cache`, runner outputs under `/data/output`, and pipeline/script files under `/data/pipelines`.

## State Ownership

- Orchestrator owns planning, dispatch, retries, and durable state.
- Runner owns live execution state and output files.
- Pipeline runner stages are ordinary jobs; script stages are tracked directly by their pipeline stage execution.
- PostgreSQL is the durable control-plane source of truth.
- `/data/output` is not scheduler state; the orchestrator records runner results but does not scan or reconcile output directories.

For concrete batch lifecycle and output-path behavior, see [Orchestration](orchestration.md).

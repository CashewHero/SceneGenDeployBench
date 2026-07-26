# SceneGenDeployBench

SceneGenDeployBench is a deployment-oriented benchmark scaffold for image-to-3D scene generation. It separates orchestration, runner execution, storage, and evaluation so model adapters can run in a consistent containerized workflow.

## Layout

```text
config/            system config and runner catalogs
deploy/            Docker Compose stack
docs/              technical references
orchestrator/      service, CLI, dispatch, storage
runner_wrapper/    copyable runner wrapper for model repos
```

## Core Pieces

- `orchestrator/`: loads config, syncs datasets/runners into PostgreSQL, creates jobs, dispatches batches, and stores results.
- `runner_wrapper/`: HTTP runner service plus adapter scaffold. Copy it into a model repo and implement one runner role.
- `config/runners/*.yaml`: runner catalog entries, input requirements, launcher settings, and scheduling defaults.
- `config/pipelines/*.yaml`: reusable runner and script pipelines.

## Quick Start

Use the local test stack when you want to quickly check that the project starts.

```bash
cp deploy/.env.example deploy/.env
./scripts/localtest.sh
```

`./scripts/localtest.sh` rebuilds the local images, starts PostgreSQL, and starts the orchestrator on `http://127.0.0.1:58080`. Use `./scripts/localtest.sh up` later when you only want to start the stack without rebuilding.

Check it:

```bash
curl http://127.0.0.1:58080/status
./scripts/localtest.sh exec deploybench config show
./scripts/localtest.sh exec deploybench job list
```

Stop it:

```bash
./scripts/localtest.sh down
```

Reset the local database and start fresh:

```bash
./scripts/localtest.sh reset-db
```

For image builds, Compose setup, shared paths, and logs, see [Deployment](docs/deployment.md).

## Runner Wrapper Subtree

After changing `runner_wrapper/`, update the export branch:

```bash
NEW_SHA=$(git subtree split --prefix=runner_wrapper HEAD)
git branch -f subtree/runner_wrapper "$NEW_SHA"
git push origin subtree/runner_wrapper --force-with-lease
```

## Docs

- [Architecture](docs/architecture.md): components and state ownership
- [Orchestration](docs/orchestration.md): runtime, scheduling, batches, and storage behavior
- [CLI](docs/cli.md): commands and options
- [Pipelines](docs/pipelines.md): pipeline YAML contract
- [Runner API](runner_wrapper/docs/api.md): runner HTTP and job contracts
- [Database](docs/database.md): durable schema and stored data
- [Deployment](docs/deployment.md): images, Compose, shared paths, and logs
- [Research Design](docs/research-design.md): benchmark methodology
- [Runner Wrapper](runner_wrapper/README.md): adapting a model repository

# Orchestrator

`orchestrator/` contains the SceneGenDeployBench control-plane service and `deploybench` CLI.

## Responsibilities

- load system config and runner catalogs
- discover datasets and generated outputs
- sync samples and runners into PostgreSQL
- create durable jobs
- claim jobs into batches
- launch or connect to runners
- persist terminal results, metrics, and artifact metadata
- coordinate durable pipelines and direct script containers

## Files

- `main.py`: CLI entrypoint
- `app/`: config and HTTP service
- `cli/`: command handlers and rendering
- `domain/`: datasets, batches, pipelines, targets, scheduling
- `execution/`: dispatch, pipelines, scripts, and runner HTTP client
- `runner_launchers/`: Docker and static HTTP launchers
- `storage/`: PostgreSQL schema and access

## References

- [CLI](../docs/cli.md): exact commands and options
- [Orchestration](../docs/orchestration.md): service, scheduling, batches, and launchers
- [Architecture](../docs/architecture.md): component and state boundaries
- [Deployment](../docs/deployment.md): images, Compose, shared paths, and logs
- [Database](../docs/database.md): durable schema and stored data

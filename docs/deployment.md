# Deployment

## Images

```bash
docker build -t scenegendeploybench-orchestrator:latest orchestrator
docker build -f runner_wrapper/Dockerfile -t scenegendeploybench-testrunner:latest runner_wrapper
```

Published images:

- `ghcr.io/cashewhero/scenegendeploybench-orchestrator`
- `ghcr.io/cashewhero/scenegendeploybench-testrunner`

Use immutable `sha-<short-sha>` or SemVer tags for reproducible deployments.

## Test Runner

```bash
docker run --rm -p 58090:58090 \
  -v "$PWD/data/datasets:/data/datasets" \
  -v "$PWD/data/model_cache:/data/model_cache" \
  -v "$PWD/data/output:/data/output" \
  -v "$PWD/data/pipelines:/data/pipelines:ro" \
  scenegendeploybench-testrunner:latest
```

## Compose

`deploy/docker-compose.yaml` provides:

- `postgres`
- `orchestrator`

Required environment variables:

- `PATH_DATASETS`
- `PATH_MODEL_CACHE`
- `PATH_OUTPUT`
- `PATH_PIPELINES`
- `PATH_POSTGRES_DATA`
- `PATH_CONFIG`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASS`

Set `UID`/`GID` to the host user. `DOCKER_GID` is needed when the Docker socket group differs from `GID`. Docker-launched runners inherit `UID:GID` unless a runner catalog sets `launcher.user`.

```bash
cp deploy/.env.example deploy/.env
docker compose --project-directory . --env-file deploy/.env -f deploy/docker-compose.yaml up -d postgres orchestrator
```

The Compose stack mounts the Docker socket so the orchestrator can launch catalog entries with `launcher.driver: docker`.

Check the service after Compose starts:

```bash
curl "http://127.0.0.1:${ORCHESTRATOR_PORT:-58080}/status"
docker compose --project-directory . --env-file deploy/.env \
  -f deploy/docker-compose.yaml exec orchestrator deploybench config show
```

## Logs

```bash
docker logs <container>
docker compose logs orchestrator
docker compose logs postgres
```

Log levels:

- `SCENEGENDEPLOYBENCH_LOG_LEVEL`
- `RUNNER_LOG_LEVEL`
- `THIRD_PARTY_LOG_LEVEL` (defaults to `WARNING`)

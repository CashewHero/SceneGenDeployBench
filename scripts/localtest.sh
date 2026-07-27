#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
DEPLOY_DIR="${REPO_ROOT}/deploy"
COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.yaml"
DEPLOY_ENV_FILE="${DEPLOY_DIR}/.env"

LOCALTEST_PROJECT="scenegendeploybench-localtest"
ORCHESTRATOR_IMAGE="ghcr.io/cashewhero/scenegendeploybench-orchestrator:local"
RUNNER_IMAGE="ghcr.io/cashewhero/scenegendeploybench-testrunner:local"

WAIT_MAX_ATTEMPTS=60
DEFAULT_ORCHESTRATOR_PORT=58080

usage() {
  cat <<EOF
Usage:
  ./scripts/localtest.sh
  ./scripts/localtest.sh test
  ./scripts/localtest.sh build
  ./scripts/localtest.sh up
  ./scripts/localtest.sh reset-db
  ./scripts/localtest.sh down
  ./scripts/localtest.sh log [docker compose logs args...]
  ./scripts/localtest.sh exec deploybench config show

Behavior:
  - no command tests, rebuilds the local images, then starts the stack
  - test runs the same unit suites used by GitHub Actions
  - build tests before rebuilding the local images
  - up only starts the stack
  - uses deploy/docker-compose.yaml and deploy/.env as the stack definition
  - manages the stack with docker compose instead of individual docker run calls
  - uses the bind mounts configured in deploy/.env
  - keeps the configured Postgres bind mount unless you use reset-db
EOF
}

compose_env_value() {
  local key="$1"
  awk -F= -v key="${key}" '
    function trim(value) {
      sub(/^[[:space:]]+/, "", value)
      sub(/[[:space:]]+$/, "", value)
      return value
    }
    /^[[:space:]]*#/ { next }
    NF < 2 { next }
    {
      current_key = trim($1)
      if (current_key != key) {
        next
      }
      value = substr($0, index($0, "=") + 1)
      print trim(value)
    }
  ' "${DEPLOY_ENV_FILE}" | tail -n 1
}

compose() {
  docker compose \
    -p "${LOCALTEST_PROJECT}" \
    --project-directory "${REPO_ROOT}" \
    --env-file "${DEPLOY_ENV_FILE}" \
    -f "${COMPOSE_FILE}" \
    "$@"
}

warn_if_orchestrator_image_mismatch() {
  local configured_images

  if ! configured_images="$(compose config --images 2>/dev/null)"; then
    echo "warning: cannot verify which orchestrator image Compose will use; local image ${ORCHESTRATOR_IMAGE} may not be used" >&2
    return
  fi
  if grep -Fxq "${ORCHESTRATOR_IMAGE}" <<<"${configured_images}"; then
    return
  fi

  echo "warning: localtest builds ${ORCHESTRATOR_IMAGE}, but Compose is configured for different images:" >&2
  echo "${configured_images}" >&2
  echo "warning: the locally built orchestrator image will not be used; set ORCHESTRATOR_VERSION=local in ${DEPLOY_ENV_FILE}" >&2
}

local_env_path() {
  local key="$1"
  local value

  value="$(compose_env_value "${key}")"
  [[ -n "${value}" ]] || return 0

  if [[ "${value}" = /* ]]; then
    echo "${value}"
  else
    echo "${REPO_ROOT}/${value#./}"
  fi
}

orchestrator_port() {
  local port

  port="$(compose_env_value ORCHESTRATOR_PORT)"
  echo "${port:-${DEFAULT_ORCHESTRATOR_PORT}}"
}

orchestrator_status_url() {
  echo "http://127.0.0.1:$(orchestrator_port)/status"
}

orchestrator_config_url() {
  echo "http://127.0.0.1:$(orchestrator_port)/config"
}

check_docker_access() {
  if docker info >/dev/null 2>&1; then
    return 0
  fi

  echo "cannot access the Docker daemon; make sure 'docker info' works in this shell" >&2
  exit 1
}

ensure_localtest_env() {
  [[ -f "${COMPOSE_FILE}" ]] || { echo "missing compose file: ${COMPOSE_FILE}" >&2; exit 1; }
  [[ -f "${DEPLOY_ENV_FILE}" ]] || { echo "missing env file: ${DEPLOY_ENV_FILE}" >&2; exit 1; }

  local datasets_dir
  local model_cache_dir
  local output_dir
  local pipelines_dir
  local postgres_data_dir
  local config_dir

  datasets_dir="$(local_env_path PATH_DATASETS)"
  model_cache_dir="$(local_env_path PATH_MODEL_CACHE)"
  output_dir="$(local_env_path PATH_OUTPUT)"
  pipelines_dir="$(local_env_path PATH_PIPELINES)"
  postgres_data_dir="$(local_env_path PATH_POSTGRES_DATA)"
  config_dir="$(local_env_path PATH_CONFIG)"

  [[ -n "${datasets_dir}" ]] || { echo "missing PATH_DATASETS in ${DEPLOY_ENV_FILE}" >&2; exit 1; }
  [[ -n "${model_cache_dir}" ]] || { echo "missing PATH_MODEL_CACHE in ${DEPLOY_ENV_FILE}" >&2; exit 1; }
  [[ -n "${output_dir}" ]] || { echo "missing PATH_OUTPUT in ${DEPLOY_ENV_FILE}" >&2; exit 1; }
  [[ -n "${pipelines_dir}" ]] || { echo "missing PATH_PIPELINES in ${DEPLOY_ENV_FILE}" >&2; exit 1; }
  [[ -n "${postgres_data_dir}" ]] || { echo "missing PATH_POSTGRES_DATA in ${DEPLOY_ENV_FILE}" >&2; exit 1; }
  [[ -n "${config_dir}" ]] || { echo "missing PATH_CONFIG in ${DEPLOY_ENV_FILE}" >&2; exit 1; }

  mkdir -p "${datasets_dir}" "${model_cache_dir}" "${output_dir}" "${pipelines_dir}" "${postgres_data_dir}"
  [[ -d "${config_dir}" ]] || { echo "missing config directory: ${config_dir}" >&2; exit 1; }
}

wait_for_postgres() {
  local postgres_user
  local postgres_db
  local attempt=1

  postgres_user="$(compose_env_value POSTGRES_USER)"
  postgres_db="$(compose_env_value POSTGRES_DB)"

  until compose exec -T postgres pg_isready -U "${postgres_user}" -d "${postgres_db}" >/dev/null 2>&1; do
    if (( attempt >= WAIT_MAX_ATTEMPTS )); then
      echo "postgres did not become ready" >&2
      compose ps >&2 || true
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
}

wait_for_orchestrator() {
  local attempt=1
  local status_url

  status_url="$(orchestrator_status_url)"

  until curl --silent --fail "${status_url}" >/dev/null; do
    if (( attempt >= WAIT_MAX_ATTEMPTS )); then
      echo "orchestrator did not become ready" >&2
      compose ps >&2 || true
      exit 1
    fi
    attempt=$((attempt + 1))
    sleep 1
  done
}

run_tests() {
  PYTHONPATH="${REPO_ROOT}/orchestrator" \
    python3 -m unittest discover -s "${REPO_ROOT}/orchestrator/tests" -v
  PYTHONPATH="${REPO_ROOT}" \
    python3 -m unittest discover -s "${REPO_ROOT}/runner_wrapper/tests" -v
}

build_images() {
  warn_if_orchestrator_image_mismatch
  run_tests
  docker build \
    -t "${ORCHESTRATOR_IMAGE}" \
    "${REPO_ROOT}/orchestrator"
  docker build \
    -t "${RUNNER_IMAGE}" \
    -f "${REPO_ROOT}/runner_wrapper/Dockerfile" \
    "${REPO_ROOT}/runner_wrapper"
}

reset_db() {
  local postgres_data_dir
  local deploy_uid
  local deploy_gid

  ensure_localtest_env
  compose down --remove-orphans >/dev/null 2>&1 || true
  postgres_data_dir="$(local_env_path PATH_POSTGRES_DATA)"
  [[ -n "${postgres_data_dir}" ]] || { echo "missing PATH_POSTGRES_DATA in ${DEPLOY_ENV_FILE}" >&2; exit 1; }
  mkdir -p "${postgres_data_dir}"
  deploy_uid="$(compose_env_value UID)"
  deploy_gid="$(compose_env_value GID)"
  docker run --rm \
    -v "${postgres_data_dir}:/localtest-postgres-data" \
    python:3.12-slim \
    sh -c 'find /localtest-postgres-data -mindepth 1 -maxdepth 1 -exec rm -rf {} +'
  if [[ -n "${deploy_uid}" && -n "${deploy_gid}" ]]; then
    docker run --rm \
      -v "${postgres_data_dir}:/localtest-postgres-data" \
      python:3.12-slim \
      chown "${deploy_uid}:${deploy_gid}" /localtest-postgres-data
  fi
}

down() {
  ensure_localtest_env
  compose down --remove-orphans
}

log() {
  ensure_localtest_env
  if [[ "$#" -eq 0 ]]; then
    compose logs -f orchestrator
  else
    compose logs "$@" orchestrator
  fi
}

up() {
  ensure_localtest_env
  warn_if_orchestrator_image_mismatch
  compose up -d postgres orchestrator
  wait_for_postgres
  wait_for_orchestrator

  cat <<EOF
Local test environment is up.

Compose:
  project: ${LOCALTEST_PROJECT}
  file: ${COMPOSE_FILE}
  env: ${DEPLOY_ENV_FILE}

Postgres:
  data: $(local_env_path PATH_POSTGRES_DATA)

Identity:
  uid: $(compose_env_value UID || true)
  gid: $(compose_env_value GID || true)
  docker gid: $(compose_env_value DOCKER_GID || true)

Orchestrator:
  status: $(orchestrator_status_url)
  config: $(orchestrator_config_url)

Examples:
  ./scripts/localtest.sh exec deploybench config show
  ./scripts/localtest.sh exec deploybench job list
  ./scripts/localtest.sh down
EOF
}

build_and_up() {
  ensure_localtest_env
  build_images
  up
}

run_exec() {
  if [[ "$#" -eq 0 ]]; then
    echo "exec requires a command" >&2
    exit 1
  fi
  ensure_localtest_env
  if [[ -t 0 && -t 1 ]]; then
    compose exec orchestrator "$@"
  else
    compose exec -T orchestrator "$@"
  fi
}

main() {
  command -v python3 >/dev/null 2>&1 || { echo "python3 is required" >&2; exit 1; }
  if [[ "${1:-}" == "test" ]]; then
    run_tests
    return
  fi

  command -v docker >/dev/null 2>&1 || { echo "docker is required" >&2; exit 1; }
  command -v curl >/dev/null 2>&1 || { echo "curl is required" >&2; exit 1; }
  check_docker_access
  docker compose version >/dev/null 2>&1 || { echo "docker compose is required" >&2; exit 1; }

  case "${1:-}" in
    "")
      build_and_up
      ;;
    up)
      up
      ;;
    build)
      ensure_localtest_env
      build_images
      ;;
    reset-db)
      reset_db
      up
      ;;
    down)
      down
      ;;
    log)
      shift
      log "$@"
      ;;
    exec)
      shift
      run_exec "$@"
      ;;
    -h|--help|help)
      usage
      ;;
    *)
      echo "unknown command: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
}

main "$@"

#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DOCKER_HOST:-}" && "$(uname -s)" == Darwin ]]; then
  export DOCKER_HOST="unix://${HOME}/.colima/gkg/docker.sock"
fi

if [[ -n "${GKG_TEST_CLICKHOUSE_URL:-}" ]]; then
  exec cargo nextest run "$@"
fi

image="clickhouse/clickhouse-server:${CLICKHOUSE_VERSION:-26.6}"
password="testpass"
publish_host="127.0.0.1"
persistent="${CI:+0}"
persistent="${persistent:-1}"
name="gkg-clickhouse-${CI_JOB_ID:-local}"
if [[ "$persistent" == 0 ]]; then
  name="${name}-${RANDOM}-$$"
fi

if [[ -n "${TESTCONTAINERS_HOST_OVERRIDE:-}" ]]; then
  publish_host="0.0.0.0"
fi

cleanup() {
  if [[ "$persistent" == 0 ]]; then
    docker rm -f "$name" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if docker inspect "$name" >/dev/null 2>&1 && [[ "$(docker inspect --format '{{.Config.Image}}' "$name")" != "$image" ]]; then
  docker rm -f "$name" >/dev/null
fi
if ! docker inspect "$name" >/dev/null 2>&1; then
  docker run --detach --name "$name" --publish "${publish_host}::8123" --env CLICKHOUSE_USER=default --env "CLICKHOUSE_PASSWORD=$password" --env CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 "$image" >/dev/null
fi
port="$(docker port "$name" 8123/tcp | sed -n '1{s/.*://;p;}')"
host="${TESTCONTAINERS_HOST_OVERRIDE:-127.0.0.1}"
export GKG_TEST_CLICKHOUSE_URL="http://${host}:${port}"
export GKG_TEST_DATABASE_PREFIX="run_${CI_JOB_ID:-$$}_"

drop_run_databases() {
  curl --silent --fail --user "default:$password" --data-binary "SELECT name FROM system.databases WHERE startsWith(name, '$GKG_TEST_DATABASE_PREFIX') FORMAT TSVRaw" "$GKG_TEST_CLICKHOUSE_URL" |
    while read -r database; do
      curl --silent --fail --user "default:$password" --data-binary "DROP DATABASE IF EXISTS $database SYNC" "$GKG_TEST_CLICKHOUSE_URL" >/dev/null
    done
}

for _ in {1..200}; do
  if curl --silent --fail --user "default:$password" "$GKG_TEST_CLICKHOUSE_URL/ping" >/dev/null 2>&1; then
    trap 'drop_run_databases || true; cleanup' EXIT
    cargo nextest run "$@"
    exit
  fi
  sleep 0.05
done

docker logs "$name" >&2
exit 1

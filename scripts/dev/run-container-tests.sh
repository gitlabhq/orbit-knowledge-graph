#!/usr/bin/env bash
set -euo pipefail

if [[ -z "${DOCKER_HOST:-}" && "$(uname -s)" == Darwin ]]; then
  export DOCKER_HOST="unix://${HOME}/.colima/gkg/docker.sock"
fi

if [[ -n "${GKG_TEST_CLICKHOUSE_URL:-}" ]]; then
  exec cargo nextest run "$@"
fi

image="clickhouse/clickhouse-server:${CLICKHOUSE_VERSION:-26.6}"
name="gkg-clickhouse-${CI_JOB_ID:-local}-${RANDOM}-$$"
password="testpass"
publish_host="127.0.0.1"

if [[ -n "${TESTCONTAINERS_HOST_OVERRIDE:-}" ]]; then
  publish_host="0.0.0.0"
fi

cleanup() {
  docker rm -f "$name" >/dev/null 2>&1 || true
}
trap cleanup EXIT

port="$(docker run --detach --rm --name "$name" --publish "${publish_host}::8123" --env CLICKHOUSE_USER=default --env "CLICKHOUSE_PASSWORD=$password" --env CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 "$image" >/dev/null && docker port "$name" 8123/tcp | sed -n '1{s/.*://;p;}')"
host="${TESTCONTAINERS_HOST_OVERRIDE:-127.0.0.1}"
export GKG_TEST_CLICKHOUSE_URL="http://${host}:${port}"

for _ in {1..200}; do
  if curl --silent --fail --user "default:$password" "$GKG_TEST_CLICKHOUSE_URL/ping" >/dev/null 2>&1; then
    cargo nextest run "$@"
    exit
  fi
  sleep 0.05
done

docker logs "$name" >&2
exit 1

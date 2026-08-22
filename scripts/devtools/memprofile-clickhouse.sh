#!/usr/bin/env bash
# Long-lived ClickHouse container for code-indexing memory profiling runs.
#
# Usage: memprofile-clickhouse.sh up|down|status
#
# The profiler drops and recreates its database on every run, so the container
# is kept across runs to avoid paying container startup on each iteration.

set -euo pipefail

NAME="${CH_CONTAINER_NAME:-gkg-memprofile-ch}"
IMAGE="${CH_IMAGE:-clickhouse/clickhouse-server:26.2}"
HTTP_PORT="${CH_HTTP_PORT:-18123}"
NATIVE_PORT="${CH_NATIVE_PORT:-19000}"
PASSWORD="${CH_PASSWORD:-memprofile}"
MEM_LIMIT="${CH_MEM_LIMIT:-6g}"

export DOCKER_HOST="${DOCKER_HOST:-unix://${HOME}/.colima/default/docker.sock}"

case "${1:-up}" in
  up)
    if docker ps --format '{{.Names}}' | grep -qx "$NAME"; then
      echo "$NAME already running on :$HTTP_PORT"
      exit 0
    fi
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker run -d --name "$NAME" \
      --memory "$MEM_LIMIT" \
      -e CLICKHOUSE_PASSWORD="$PASSWORD" \
      -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
      -p "${HTTP_PORT}:8123" \
      -p "${NATIVE_PORT}:9000" \
      --ulimit nofile=262144:262144 \
      "$IMAGE" >/dev/null
    echo -n "waiting for clickhouse on :$HTTP_PORT"
    for _ in $(seq 1 120); do
      if curl -sf "http://localhost:${HTTP_PORT}/ping" >/dev/null; then
        echo " ready"
        exit 0
      fi
      echo -n .
      sleep 1
    done
    echo " timed out"
    docker logs --tail 50 "$NAME"
    exit 1
    ;;
  down)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    echo "$NAME removed"
    ;;
  status)
    docker ps --filter "name=$NAME" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
    ;;
  *)
    echo "Usage: $0 up|down|status" >&2
    exit 1
    ;;
esac

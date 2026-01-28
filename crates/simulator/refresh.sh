#!/bin/bash
# Refresh ClickHouse container with fresh state

set -e

CONTAINER_NAME="gkg-clickhouse"
VOLUME_NAME="gkg-clickhouse-data"

echo "Stopping and removing existing container..."
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

echo "Removing data volume for fresh start..."
docker volume rm "$VOLUME_NAME" 2>/dev/null || true

echo "Starting ClickHouse 25.12..."
docker run -d --name "$CONTAINER_NAME" \
  -p 8123:8123 -p 9000:9000 \
  --memory=32g \
  --ulimit nofile=262144:262144 \
  --cap-add=SYS_NICE \
  --cap-add=NET_ADMIN \
  --cap-add=IPC_LOCK \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$VOLUME_NAME":/var/lib/clickhouse \
  clickhouse/clickhouse-server:25.12

echo "Waiting for ClickHouse to start..."
sleep 3

# Health check
for i in {1..10}; do
  if curl -s "http://localhost:8123/?query=SELECT%20version()" 2>/dev/null; then
    echo "ClickHouse is ready!"
    exit 0
  fi
  sleep 1
done

echo "Check manually: curl 'http://localhost:8123/?query=SELECT%20version()'"

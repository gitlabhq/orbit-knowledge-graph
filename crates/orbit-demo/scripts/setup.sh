#!/usr/bin/env bash
# Pulls run here, on a fresh VM, before services.sh locks its egress on every launch.
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if vm_running; then
  colima stop "$PROFILE"
fi
colima_start

for image in "$CLICKHOUSE_IMAGE" "$RELAY_IMAGE" "$RUSTFS_IMAGE"; do
  "${DOCKER[@]}" pull "$image"
done

extensions="$ORBIT_DEMO_ROOT/../../target/duckdb-extensions"
mkdir -p "$extensions"
mise exec -- duckdb -c "SET extension_directories = ['$extensions']; INSTALL httpfs; INSTALL ducklake;"
echo "orbit-demo images and DuckDB S3 extensions are ready; run 'mise run demo'"

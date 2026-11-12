#!/usr/bin/env bash
# Pulls run here, on a fresh VM, before clickhouse.sh locks its egress on every launch.
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

if vm_running; then
  colima stop "$PROFILE"
fi
colima_start

for image in "$CLICKHOUSE_IMAGE" "$RELAY_IMAGE"; do
  "${DOCKER[@]}" pull "$image"
done
echo "orbit-demo images pulled; run 'mise run demo'"

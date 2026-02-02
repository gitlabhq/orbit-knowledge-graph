#!/usr/bin/env bash
set -euo pipefail

echo "Starting Trello sync loop (Ctrl+C to stop)"

while true; do
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Running trello sync..."
    cargo run -p gkg-server --release -- --mode trello-sync || echo "Sync failed, will retry..."
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Sleeping for 60 seconds..."
    sleep 5
done

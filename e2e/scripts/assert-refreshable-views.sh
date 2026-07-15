#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

CH_POD=clickhouse-0
INDEXER_DEPLOY=gkg-indexer-default

ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
    sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"'
}

log "Asserting the namespace-storage-snapshot refreshable view exists"

found=0
for _ in $(seq 1 24); do
  count=$(ch_query "SELECT count() FROM system.tables WHERE database = 'gkg' AND name LIKE 'v%_namespace_storage_snapshot_refresh'" 2>/dev/null || true)
  if [ -n "$count" ] && [ "$count" != "0" ]; then
    found=1
    break
  fi
  sleep 5
done

if [ "$found" = "1" ]; then
  log "Refreshable view present"
  exit 0
fi

log "FAIL: the refreshable view was never created. The indexer swallowed its"
log "creation error, so the graph writer is missing a grant it holds in production."

log "Reproducing the graph writer's system.parts read:"
GRAPH_PASS=$($KC get secret gkg-secrets -n "$NS_GKG" -o jsonpath='{.data.graph-password}' 2>/dev/null | base64 -d || true)
printf '%s\n' "SELECT count() FROM system.parts WHERE database = 'gkg'" \
  | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
      env CHPW="$GRAPH_PASS" sh -c 'clickhouse-client --user gkg_writer --password "$CHPW"' 2>&1 \
  | tail -20 || true

log "GKG indexer log lines:"
$KC logs "deploy/$INDEXER_DEPLOY" -n "$NS_GKG" --all-containers --tail=-1 2>/dev/null \
  | grep -iE "refreshable|system\.parts|ACCESS_DENIED|Not enough privileges|Code: 497" \
  | tail -40 || true

exit 1

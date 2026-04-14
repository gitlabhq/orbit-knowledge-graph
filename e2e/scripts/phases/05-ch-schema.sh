#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

# Datalake schema is managed by GitLab's ClickHouse migrations (global.clickhouse).
# Only the GKG graph schema needs manual application.

log "Phase 5: Applying ClickHouse graph schema"

$KC exec -i -n "$NS_CH" clickhouse-0 -- \
  clickhouse-client --user default --password "$E2E_CH_DEFAULT_PASS" \
  --database gkg --multiquery < "$GKG_ROOT/config/graph.sql"

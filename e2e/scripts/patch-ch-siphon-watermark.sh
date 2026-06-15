#!/usr/bin/env bash
# Workaround until siphon.image.tag ships the _siphon_watermark column.
# gkg!1729 made the SDLC indexer query `_siphon_watermark`, but siphon
# 0.0.99-beta does not emit it, so every datalake query fails Code 47
# UNKNOWN_IDENTIFIER and the indexer writes zero nodes. Add the column to every
# siphon CDC table plus the non-siphon_-prefixed ontology source tables.
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

CH_POD=clickhouse-0
ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
    sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"'
}

# Default to _siphon_replicated_at (matching fixtures/siphon.sql), NOT now():
# ADD COLUMN does not materialize existing snapshot rows, so a now() default is
# recomputed at read time and pins their watermark to the current instant. That
# races the indexer's max(_siphon_watermark) checkpoint forward each cycle, so
# rows replicated mid-test fall below the floor and never index. Replicated-at
# is a stored, monotonic value, so the checkpoint reflects real replication time.
add_watermark() {
  log "Adding _siphon_watermark to datalake.$1"
  ch_query "ALTER TABLE datalake.\`$1\` \
    ADD COLUMN IF NOT EXISTS \`_siphon_watermark\` DateTime64(6, 'UTC') DEFAULT _siphon_replicated_at"
}

for table in $(ch_query "SELECT name FROM system.tables \
  WHERE database = 'datalake' AND name LIKE 'siphon\_%' \
    AND engine NOT IN ('MaterializedView', 'View', 'Dictionary', 'Null') FORMAT TSV"); do
  add_watermark "$table"
done

for table in merge_requests work_items; do
  add_watermark "$table"
done

log "Done seeding _siphon_watermark"

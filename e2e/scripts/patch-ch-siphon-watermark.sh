#!/usr/bin/env bash
# Create the _siphon_watermark column the SDLC indexer queries (gkg!1729).
# Siphon 0.0.100-beta handles the column but does NOT create it: it omits it
# from every insert so ClickHouse fills the DEFAULT. We add it to every siphon
# CDC table plus the non-siphon_-prefixed ontology source tables.
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

CH_POD=clickhouse-0
ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
    sh -c 'clickhouse-client -n --user default --password "$CLICKHOUSE_PASSWORD"'
}

# Default now64() matches what siphon expects ClickHouse to manage. The
# MATERIALIZE is essential: ADD COLUMN leaves pre-existing snapshot rows
# un-stored, so a now() default is recomputed at read time and pins their
# watermark to the current instant, racing the indexer checkpoint forward and
# stranding rows replicated mid-run. MATERIALIZE writes a concrete value to
# those rows so the watermark is stable, exactly as it is for rows siphon
# inserts after the column exists.
#
# One multiquery batch: per-table kubectl execs cost ~1.3s each on the
# setup critical path.
TABLES=$(ch_query "SELECT name FROM system.tables \
  WHERE database = 'datalake' AND name LIKE 'siphon\_%' \
    AND engine NOT IN ('MaterializedView', 'View', 'Dictionary', 'Null') FORMAT TSV")
TABLES="$TABLES
merge_requests
work_items"

BATCH=""
for table in $TABLES; do
  log "Queueing _siphon_watermark for datalake.$table"
  BATCH+="ALTER TABLE datalake.\`$table\` \
    ADD COLUMN IF NOT EXISTS \`_siphon_watermark\` DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC');
ALTER TABLE datalake.\`$table\` MATERIALIZE COLUMN \`_siphon_watermark\`;
"
done
ch_query "$BATCH"

log "Done seeding _siphon_watermark"

#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

CH_POD=clickhouse-0
ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
    sh -c 'clickhouse-client -n --user default --password "$CLICKHOUSE_PASSWORD"'
}

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

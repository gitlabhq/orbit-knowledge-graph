#!/usr/bin/env bash
# Sequential, resumable driver: skips tables that already have an ok result for
# the tier in $RESULTS.
#
#   run_tier.sh fast                 # every swept table
#   run_tier.sh full v85_gl_edge v85_gl_sec_edge
set -u
TIER=$1; shift
DIR=$(dirname "$0")
RESULTS=${RESULTS:-/tmp/tombstone-sweep/results.jsonl}
export RESULTS
touch "$RESULTS"

if [ $# -gt 0 ]; then
  TABLES="$*"
else
  TABLES=$(echo "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE 'v85_gl_%' AND engine LIKE '%MergeTree%' ORDER BY total_bytes FORMAT TSVRaw" | "$DIR/chw")
fi

for t in $TABLES; do
  if grep -q "\"table\":\"$t\",\"tier\":\"$TIER\",\"status\":\"ok\"" "$RESULTS"; then
    echo "skip $t ($TIER already ok)"
    continue
  fi
  echo "=== $t ($TIER) $(date -u +%H:%M:%SZ)"
  "$DIR/probe_one_table.sh" "$t" "$TIER" || echo "!!! $t failed, continuing"
done
echo "TIER $TIER DONE $(date -u +%H:%M:%SZ)"

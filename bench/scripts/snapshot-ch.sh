#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASE="$1"; T_START="$2"; T_END="$3"
RUN_DIR="${BENCH_DIR}/results/${RUN_ID}"
SNAP_DIR="${RUN_DIR}/snapshots/${PHASE}"
mkdir -p "${SNAP_DIR}"

: "${CH_URL:=http://localhost:8123}"

log "Snapshotting CH system tables for phase=${PHASE}"

curl -s "${CH_URL}" -d \
  "SELECT * FROM system.query_log WHERE event_time BETWEEN toDateTime(${T_START}) AND toDateTime(${T_END}) FORMAT Parquet" \
  -o "${SNAP_DIR}/query_log.parquet" 2>/dev/null || true

curl -s "${CH_URL}" -d \
  "SELECT database, table, sum(bytes_on_disk) AS bytes, sum(rows) AS rows \
   FROM system.parts WHERE active AND database IN ('datalake','gkg') \
   GROUP BY database, table ORDER BY database, table FORMAT Parquet" \
  -o "${SNAP_DIR}/parts_summary.parquet" 2>/dev/null || true

log "  snapshots written to ${SNAP_DIR}"

#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${PREFIX:?set PREFIX to the dump prefix in gs://gkg-datalake-dumps}"
: "${CH_URL:=http://localhost:8123}"
: "${SUBSET_PREFIX:=}"
WORK="/tmp/ra-${RUN_ID}-dump"

log "Importing dump ${PREFIX} into run=${RUN_ID}"

heartbeat_start
trap heartbeat_stop EXIT

mkdir -p "${WORK}"
log "Downloading from gs://gkg-datalake-dumps/${PREFIX}/datalake/"
gcloud storage rsync "gs://gkg-datalake-dumps/${PREFIX}/datalake/" "${WORK}/datalake/" --recursive

if gcloud storage ls "gs://gkg-datalake-dumps/${PREFIX}/graph/" > /dev/null 2>&1; then
  log "Downloading graph source"
  gcloud storage rsync "gs://gkg-datalake-dumps/${PREFIX}/graph/" "${WORK}/graph/" --recursive
fi

import_source() {
  local src_dir="$1" db="$2"
  [[ -d "${src_dir}" ]] || return 0

  log "Importing ${db} from ${src_dir}"
  for schema_file in "${src_dir}"/*.schema.sql; do
    [[ -f "${schema_file}" ]] || continue
    local table
    table=$(basename "${schema_file}" .schema.sql)

    # Cloud -> OSS DDL rewrite (from clickhouse-datalake-dumps bin/import.sh).
    local ddl
    ddl=$(sed \
      -e "s/SharedReplacingMergeTree([^)]*)/ReplacingMergeTree()/g" \
      -e "s/SharedMergeTree([^)]*)/MergeTree()/g" \
      -e "s/\`[a-z_]*\`\.\`/\`/g" \
      "${schema_file}")

    curl -s "${CH_URL}/?database=${db}" -d "DROP TABLE IF EXISTS ${table}" || true
    curl -s "${CH_URL}/?database=${db}" -d "${ddl}" || {
      log "  WARNING: DDL failed for ${table}, skipping"
      continue
    }

    local parts=0
    for part_file in "${src_dir}/${table}".part*.Native.gz; do
      [[ -f "${part_file}" ]] || continue
      gzip -dc "${part_file}" | curl -s -H "Content-Type: application/octet-stream" \
        "${CH_URL}/?database=${db}&query=INSERT+INTO+${table}+FORMAT+Native" --data-binary @- || {
        log "  WARNING: part import failed for ${part_file}"
      }
      parts=$((parts + 1))
    done
    log "  ${table}: ${parts} parts"
  done
}

curl -s "${CH_URL}" -d "CREATE DATABASE IF NOT EXISTS staging"
curl -s "${CH_URL}" -d "CREATE DATABASE IF NOT EXISTS datalake"

import_source "${WORK}/datalake" "staging"
import_source "${WORK}/graph" "staging"

log "Copying staging -> datalake with scrub"
for table in $(curl -s "${CH_URL}/?database=staging" -d "SHOW TABLES" | tr '\n' ' '); do
  SUBSET_FILTER=""
  if [[ -n "${SUBSET_PREFIX}" ]]; then
    SUBSET_FILTER="WHERE startsWith(traversal_path, '${SUBSET_PREFIX}')"
  fi

  curl -s "${CH_URL}" -d \
    "INSERT INTO datalake.${table} SELECT * FROM staging.${table} ${SUBSET_FILTER}" 2>/dev/null || {
    log "  WARNING: copy failed for ${table} (likely missing traversal_path column), copying without filter"
    curl -s "${CH_URL}" -d "INSERT INTO datalake.${table} SELECT * FROM staging.${table}" 2>/dev/null || true
  }
done

log "Materializing _siphon_watermark"
"${E2E_DIR}/scripts/patch-ch-siphon-watermark.sh" || true

log "Import complete"

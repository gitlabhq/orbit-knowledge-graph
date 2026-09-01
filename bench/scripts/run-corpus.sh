#!/usr/bin/env bash
# Run the query corpus against the bench ClickHouse via query-profiler.
# Requires a port-forward to the bench CH on localhost:18123.
#
# Usage:
#   KCTX=... RUN_ID=bench12 bash bench/scripts/run-corpus.sh
#   KCTX=... RUN_ID=bench12 bash bench/scripts/run-corpus.sh sdlc
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="ra-ch-${RUN_ID}"
CH_PASS=$(kubectl --context="${KCTX}" get secret ra-ch-credentials -n "${CH_NS}" \
  -o jsonpath='{.data.default-password}' | base64 -d)
GRAPH_PASS=$(kubectl --context="${KCTX}" -n "e2e-${RUN_ID}-gkg" get secret gkg-secrets \
  -o jsonpath='{.data.graph-password}' | base64 -d)

# Resolve a traversal path and the schema version from the live data.
SCHEMA_VER=$(kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASS}" -q \
  "SELECT max(version) FROM gkg.gkg_schema_version FORMAT TSV" 2>/dev/null)
TPATH=$(kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASS}" -q \
  "SELECT DISTINCT traversal_path FROM gkg.v${SCHEMA_VER}_gl_project LIMIT 1 FORMAT TSV" 2>/dev/null)

export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_DATABASE=gkg
export CLICKHOUSE_USER=gkg_writer
export CLICKHOUSE_PASSWORD="${GRAPH_PASS}"

log "Schema version: v${SCHEMA_VER}, traversal path: ${TPATH}"

# Build the profiler once.
log "Building query-profiler..."
cargo build -p query-profiler --quiet 2>/dev/null
PROFILER="${REPO_ROOT}/target/debug/query-profiler"

SUITE_FILTER="${1:-}"
CORPUS_DIR="${REPO_ROOT}/fixtures/queries/corpus"
RESULTS_DIR="${BENCH_DIR}/results/${RUN_ID}/corpus"
mkdir -p "${RESULTS_DIR}"

TOTAL=0; PASS=0; FAIL=0
ERRORS=()

for yaml in "${CORPUS_DIR}"/*.yaml; do
  suite=$(basename "${yaml}" .yaml)
  [[ "${suite}" == "raw_sql_ab" ]] && continue
  [[ -n "${SUITE_FILTER}" && "${suite}" != "${SUITE_FILTER}" ]] && continue

  log "Suite: ${suite}"

  for qname in $(yq eval 'keys | .[]' "${yaml}"); do
    TOTAL=$((TOTAL + 1))
    EXPECT=$(yq eval ".${qname}.expect // \"rows\"" "${yaml}")
    QUERY=$(yq eval ".${qname}.query" "${yaml}")
    OUTPUT="${RESULTS_DIR}/${suite}_${qname}.json"

    if [[ "${EXPECT}" == "error" ]]; then
      if "${PROFILER}" -t "${TPATH}" "${QUERY}" >/dev/null 2>&1; then
        log "  FAIL ${qname} (expected error, got success)"
        FAIL=$((FAIL + 1)); ERRORS+=("${suite}/${qname}: expected error")
      else
        log "  PASS ${qname} (expected error)"
        PASS=$((PASS + 1))
      fi
    else
      if "${PROFILER}" -t "${TPATH}" -o "${OUTPUT}" "${QUERY}" 2>/dev/null; then
        ROWS=$(python3 -c "import json; d=json.load(open('${OUTPUT}')); print(d.get('summary',{}).get('result_rows',0))" 2>/dev/null || echo "0")
        if [[ "${EXPECT}" == "empty" || "${ROWS}" -gt 0 ]]; then
          log "  PASS ${qname} (${ROWS} rows)"
          PASS=$((PASS + 1))
        else
          log "  FAIL ${qname} (0 rows, expected rows)"
          FAIL=$((FAIL + 1)); ERRORS+=("${suite}/${qname}: 0 rows")
        fi
      else
        log "  FAIL ${qname} (query error)"
        FAIL=$((FAIL + 1)); ERRORS+=("${suite}/${qname}: execution error")
      fi
    fi
  done
done

echo ""
echo "=== Corpus Results ==="
echo "Total: ${TOTAL}  Pass: ${PASS}  Fail: ${FAIL}"
if [[ ${#ERRORS[@]} -gt 0 ]]; then
  echo ""
  echo "Failures:"
  for e in "${ERRORS[@]}"; do echo "  - ${e}"; done
fi

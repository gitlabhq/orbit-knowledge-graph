#!/usr/bin/env bash
# Compute SLOs from Prometheus metrics (pod scrape) + CH stats.
# Usage: KCTX=... RUN_ID=bench6 TIER=small bash bench/scripts/slos.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"
GKG_NS="e2e-${RUN_ID}-gkg"

ch() { timeout 15 $KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q "$1" 2>/dev/null || echo ""; }

# --- Targets ---
T_P90=$(tier '.slos.query_p90_ms')
T_RATE=$(tier '.slos.query_success_rate')
T_LAG=$(tier '.slos.sdlc_watermark_lag_p99_s')
T_OOM=$(tier '.slos.oom_count')

# --- Scrape indexer Prometheus (if available) ---
INDEXER_AVAIL=$($KC get deploy gkg-indexer-default -n "${GKG_NS}" \
  -o jsonpath='{.status.availableReplicas}' 2>/dev/null || echo 0)

PROM_LAG=""
PROM_ROWS=""
if [[ "${INDEXER_AVAIL:-0}" -gt 0 ]]; then
  PROM_DATA=$($KC exec -n "${GKG_NS}" deploy/gkg-indexer-default -- \
    wget -qO- --timeout=5 http://localhost:9394/metrics 2>/dev/null || true)
  PROM_LAG=$(echo "${PROM_DATA}" | grep "^gkg_indexer_sdlc_watermark_lag_seconds{" | awk '{print $2}' | head -1)
  PROM_ROWS=$(echo "${PROM_DATA}" | grep "^gkg_indexer_sdlc_pipeline_rows_processed_total{" | awk '{s+=$2} END {printf "%.0f", s}')
fi

# --- 1. Watermark lag ---
if [[ -n "${PROM_LAG}" ]]; then
  LAG="${PROM_LAG%.*}"
  LAG_SRC="prom"
else
  CKPT=$(ch "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%_checkpoint' AND name NOT LIKE '%code%' LIMIT 1 FORMAT TSV")
  if [[ -n "${CKPT}" ]]; then
    LAG=$(ch "SELECT round(max(dateDiff('second', watermark, now()))) FROM gkg.${CKPT} FINAL WHERE NOT _deleted AND key LIKE 'ns.%' FORMAT TSV")
  fi
  LAG_SRC="CH"
fi
: "${LAG:=0}"

# --- 2. CH query stats ---
CH_STATS=$(ch "
  SELECT count(),
         countIf(type='ExceptionWhileProcessing'),
         round(quantile(0.9)(query_duration_ms)),
         round(countIf(type='QueryFinish') / count(), 6)
  FROM system.query_log
  WHERE type IN ('QueryFinish','ExceptionWhileProcessing')
    AND query_kind='Select' AND current_database IN ('gkg','datalake')
  FORMAT TSV")

CH_TOTAL=0; CH_ERR=0; CH_P90=0; CH_RATE="1.0"
if [[ -n "${CH_STATS}" ]]; then
  IFS=$'\t' read -r CH_TOTAL CH_ERR CH_P90 CH_RATE <<< "${CH_STATS}"
fi

# --- 3. OOM count ---
OOM_COUNT=0
for ns in "${GKG_NS}" "${CH_NS}"; do
  while IFS= read -r reason; do
    [[ "${reason}" == "OOMKilled" ]] && OOM_COUNT=$((OOM_COUNT + 1))
  done < <($KC get pods -n "${ns}" \
    -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.lastState.terminated.reason}{"\n"}{end}{end}' 2>/dev/null || true)
done

# --- 4. CH peak + errors (informational) ---
CH_PEAK=$(ch "SELECT formatReadableSize(max(memory_usage)), round(max(query_duration_ms))
  FROM system.query_log WHERE type='QueryFinish' AND current_database IN ('gkg','datalake') FORMAT TSV")
CH_ERRORS=$(ch "SELECT exception_code, count(), substring(any(exception),1,100)
  FROM system.query_log WHERE type='ExceptionWhileProcessing' AND current_database IN ('gkg','datalake')
  GROUP BY exception_code ORDER BY count() DESC LIMIT 3 FORMAT TSV")
GRAPH_ROWS=$(ch "SELECT formatReadableQuantity(sum(total_rows)) FROM system.tables WHERE database='gkg' AND total_rows > 0 FORMAT TSV")
DL_ROWS=$(ch "SELECT formatReadableQuantity(sum(total_rows)) FROM system.tables WHERE database='datalake' AND total_rows > 0 FORMAT TSV")

# --- Evaluate ---
pass() { python3 -c "print('PASS' if $1 else 'FAIL')"; }
P_P90=$(pass "${CH_P90:-0} <= ${T_P90}")
P_RATE=$(pass "${CH_RATE:-1} >= ${T_RATE}")
P_LAG=$(pass "${LAG} <= ${T_LAG}")
P_OOM=$(pass "${OOM_COUNT} <= ${T_OOM}")

echo ""
echo "=== SLO Report: run=${RUN_ID} tier=${TIER} ==="
echo ""
printf "%-28s %10s %10s %6s  %s\n" "SLO" "Target" "Actual" "" "Source"
printf "%-28s %10s %10s %6s  %s\n" "---" "------" "------" "---" "------"
printf "%-28s %10s %10s %6s  %s\n" "query_p90_ms"            "<=${T_P90}"   "${CH_P90}"     "${P_P90}"  "CH query_log"
printf "%-28s %10s %10s %6s  %s\n" "query_success_rate"       ">=${T_RATE}" "${CH_RATE}"    "${P_RATE}" "CH query_log"
printf "%-28s %10s %10s %6s  %s\n" "sdlc_watermark_lag_p99_s" "<=${T_LAG}"  "${LAG}"       "${P_LAG}"  "${LAG_SRC}"
printf "%-28s %10s %10s %6s  %s\n" "oom_count"                "<=${T_OOM}"  "${OOM_COUNT}" "${P_OOM}"  "kubectl"
printf "%-28s %10s %10s %6s\n"     "code_backfill_hours_max"  "-" "-" "N/A"
printf "%-28s %10s %10s %6s\n"     "cpu_throttle_ratio_max"   "-" "-" "N/A"
echo ""
echo "  CH: ${CH_TOTAL} queries, ${CH_ERR} errors, p90=${CH_P90}ms"
[[ -n "${CH_PEAK}" ]] && echo "  peak: ${CH_PEAK}"
echo "  graph: ${GRAPH_ROWS:-?}    datalake: ${DL_ROWS:-?}"
[[ -n "${PROM_ROWS}" ]] && echo "  prom pipeline rows: ${PROM_ROWS}"
if [[ -n "${CH_ERRORS}" ]]; then
  echo ""
  echo "  CH errors:"
  while IFS=$'\t' read -r code cnt ex; do
    printf "    [%s] x%-6s %s\n" "${code}" "${cnt}" "${ex}"
  done <<< "${CH_ERRORS}"
fi
echo ""
PASSED=0
for r in "${P_P90}" "${P_RATE}" "${P_LAG}" "${P_OOM}"; do [[ "${r}" == "PASS" ]] && PASSED=$((PASSED+1)); done
echo "=== ${PASSED}/4 SLOs passed ==="

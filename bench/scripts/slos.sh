#!/usr/bin/env bash
# Compute SLOs from CH query_log + kubectl and compare against tier targets.
# Usage: KCTX=... RUN_ID=bench6 TIER=small bash bench/scripts/slos.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"
GKG_NS="e2e-${RUN_ID}-gkg"

CH_PASS=$($KC get statefulset clickhouse -n "${CH_NS}" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_PASSWORD")].value}')

ch() {
  $KC exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASS}" -q "$1" 2>/dev/null
}

# --- Targets ---
T_P90=$(tier '.slos.query_p90_ms')
T_RATE=$(tier '.slos.query_success_rate')
T_LAG=$(tier '.slos.sdlc_watermark_lag_p99_s')
T_OOM=$(tier '.slos.oom_count')

# --- Collect all CH metrics in one query ---
METRICS=$(ch "
  SELECT
    (SELECT round(quantile(0.9)(query_duration_ms))
     FROM system.query_log
     WHERE type='QueryFinish' AND query_kind='Select'
       AND current_database IN ('gkg','datalake')) AS p90,

    (SELECT round(quantile(0.5)(query_duration_ms))
     FROM system.query_log
     WHERE type='QueryFinish' AND query_kind='Select'
       AND current_database IN ('gkg','datalake')) AS p50,

    (SELECT round(quantile(0.99)(query_duration_ms))
     FROM system.query_log
     WHERE type='QueryFinish' AND query_kind='Select'
       AND current_database IN ('gkg','datalake')) AS p99,

    (SELECT round(max(query_duration_ms))
     FROM system.query_log
     WHERE type='QueryFinish' AND query_kind='Select'
       AND current_database IN ('gkg','datalake')) AS max_ms,

    (SELECT count()
     FROM system.query_log
     WHERE type='QueryFinish' AND query_kind='Select'
       AND current_database IN ('gkg','datalake')) AS total,

    (SELECT countIf(type='ExceptionWhileProcessing')
     FROM system.query_log
     WHERE type IN ('QueryFinish','ExceptionWhileProcessing')
       AND query_kind='Select' AND current_database IN ('gkg','datalake')) AS errors,

    (SELECT round(countIf(type='QueryFinish') / count(), 6)
     FROM system.query_log
     WHERE type IN ('QueryFinish','ExceptionWhileProcessing')
       AND query_kind='Select' AND current_database IN ('gkg','datalake')) AS rate,

    (SELECT formatReadableSize(max(memory_usage))
     FROM system.query_log
     WHERE type='QueryFinish' AND current_database IN ('gkg','datalake')) AS peak_mem,

    (SELECT formatReadableQuantity(sum(total_rows))
     FROM system.tables WHERE database='gkg' AND total_rows > 0) AS graph_rows,

    (SELECT formatReadableQuantity(sum(total_rows))
     FROM system.tables WHERE database='datalake' AND total_rows > 0) AS dl_rows
  FORMAT TSV
")

IFS=$'\t' read -r Q_P90 Q_P50 Q_P99 Q_MAX Q_TOTAL Q_ERR Q_RATE PEAK_MEM GRAPH_ROWS DL_ROWS <<< "${METRICS}"

# --- Watermark lag ---
CKPT=$(ch "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%_checkpoint' AND name NOT LIKE '%code%' LIMIT 1 FORMAT TSV")
LAG_P99=$(ch "SELECT round(quantile(0.99)(dateDiff('second', watermark, now()))) FROM gkg.${CKPT} FINAL WHERE NOT _deleted AND key LIKE 'ns.%' FORMAT TSV")
: "${LAG_P99:=0}"

# --- OOM count ---
OOM_COUNT=0
for ns in "${GKG_NS}" "${CH_NS}"; do
  while IFS= read -r line; do
    [[ "${line}" == *"OOMKilled"* ]] && OOM_COUNT=$((OOM_COUNT + 1))
  done < <($KC get pods -n "${ns}" -o jsonpath='{range .items[*]}{.lastState.terminated.reason}{"\n"}{end}' 2>/dev/null || true)
done

# --- CH errors ---
CH_ERRORS=$(ch "
  SELECT exception_code, count(), substring(any(exception),1,100)
  FROM system.query_log
  WHERE type='ExceptionWhileProcessing' AND current_database IN ('gkg','datalake')
  GROUP BY exception_code ORDER BY count() DESC LIMIT 3
  FORMAT TSV")

# --- Evaluate ---
pass() { python3 -c "print('PASS' if $1 else 'FAIL')"; }

P1=$(pass "${Q_P90} <= ${T_P90}")
P2=$(pass "${Q_RATE} >= ${T_RATE}")
P3=$(pass "${LAG_P99} <= ${T_LAG}")
P4=$(pass "${OOM_COUNT} <= ${T_OOM}")

echo ""
echo "=== SLO Report: run=${RUN_ID} tier=${TIER} ==="
echo ""
printf "%-28s %10s %10s %6s\n" "SLO" "Target" "Actual" ""
printf "%-28s %10s %10s %6s\n" "---" "------" "------" "---"
printf "%-28s %10s %10s %6s\n" "query_p90_ms"            "<=${T_P90}"   "${Q_P90}"     "${P1}"
printf "%-28s %10s %10s %6s\n" "query_success_rate"       ">=${T_RATE}" "${Q_RATE}"    "${P2}"
printf "%-28s %10s %10s %6s\n" "sdlc_watermark_lag_p99_s" "<=${T_LAG}"  "${LAG_P99}"  "${P3}"
printf "%-28s %10s %10s %6s\n" "oom_count"                "<=${T_OOM}"  "${OOM_COUNT}" "${P4}"
printf "%-28s %10s %10s %6s\n" "code_backfill_hours_max"  "-"           "-"            "N/A"
printf "%-28s %10s %10s %6s\n" "cpu_throttle_ratio_max"   "-"           "-"            "N/A"
echo ""
echo "  latency: p50=${Q_P50}  p90=${Q_P90}  p99=${Q_P99}  max=${Q_MAX}ms  (${Q_TOTAL} queries, ${Q_ERR} errors)"
echo "  peak query mem: ${PEAK_MEM}"
echo "  graph: ${GRAPH_ROWS}    datalake: ${DL_ROWS}"
echo ""
if [[ -n "${CH_ERRORS}" ]]; then
  echo "  errors:"
  while IFS=$'\t' read -r code cnt ex; do
    printf "    [%s] x%-6s %s\n" "${code}" "${cnt}" "${ex}"
  done <<< "${CH_ERRORS}"
  echo ""
fi

PASSED=0
for r in "${P1}" "${P2}" "${P3}" "${P4}"; do [[ "${r}" == "PASS" ]] && PASSED=$((PASSED+1)); done
echo "=== ${PASSED}/4 SLOs passed ==="

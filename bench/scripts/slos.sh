#!/usr/bin/env bash
# Compute SLOs from Cloud Monitoring (GMP) + CH query_log + kubectl.
# Survives pod restarts because GMP ships metrics every 15s to Cloud Monitoring.
# Usage: KCTX=... RUN_ID=bench7 TIER=small bash bench/scripts/slos.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"
GKG_NS="e2e-${RUN_ID}-gkg"
PROJECT=$(cd "${TF_DIR}" && "$TF" output -raw project 2>/dev/null)
: "${SLO_WINDOW_HOURS:=24}"

ch() { timeout 15 $KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q "$1" 2>/dev/null || echo ""; }

# --- Cloud Monitoring query helper ---
ACCESS_TOKEN=$(gcloud auth print-access-token 2>/dev/null)
START=$(date -u -v-${SLO_WINDOW_HOURS}H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
     || date -u -d "${SLO_WINDOW_HOURS} hours ago" +%Y-%m-%dT%H:%M:%SZ)
END=$(date -u +%Y-%m-%dT%H:%M:%SZ)

cm() {
  local metric="$1"
  curl -sf -H "Authorization: Bearer ${ACCESS_TOKEN}" \
    "https://monitoring.googleapis.com/v3/projects/${PROJECT}/timeSeries?filter=metric.type%3D%22prometheus.googleapis.com%2F${metric}%22%20AND%20resource.labels.namespace%3D%22${GKG_NS}%22&interval.startTime=${START}&interval.endTime=${END}&pageSize=10000" 2>/dev/null
}

# --- Targets ---
T_P90=$(tier '.slos.query_p90_ms')
T_RATE=$(tier '.slos.query_success_rate')
T_LAG=$(tier '.slos.sdlc_watermark_lag_p99_s')
T_OOM=$(tier '.slos.oom_count')

log "Querying Cloud Monitoring (${SLO_WINDOW_HOURS}h window)"

# --- 1. Watermark lag (Cloud Monitoring gauge, all pods) ---
# Full window for informational, last hour for SLO (steady-state).
STEADY_START=$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
            || date -u -d "1 hour ago" +%Y-%m-%dT%H:%M:%SZ)
LAG_DATA=$(cm "gkg_indexer_sdlc_watermark_lag_seconds/gauge" | python3 -c "
import json,sys
from datetime import datetime
d=json.load(sys.stdin)
all_vals=[]; steady_vals=[]
cutoff=datetime.fromisoformat('${STEADY_START}'.replace('Z','+00:00'))
for ts in d.get('timeSeries',[]):
    for p in ts.get('points',[]):
        v=p['value']['doubleValue']
        all_vals.append(v)
        t=datetime.fromisoformat(p['interval']['endTime'].replace('Z','+00:00'))
        if t >= cutoff: steady_vals.append(v)
all_vals.sort()
steady_vals.sort()
lag_max=max(all_vals) if all_vals else 0
lag_p99_full=all_vals[int(len(all_vals)*0.99)] if all_vals else 0
lag_p99_steady=steady_vals[int(len(steady_vals)*0.99)] if steady_vals else 0
lag_current=all_vals[-1] if all_vals else 0
print(f'{lag_max:.0f}\t{lag_p99_full:.0f}\t{lag_p99_steady:.0f}\t{lag_current:.3f}\t{len(all_vals)}\t{len(steady_vals)}')
" 2>/dev/null || echo "0	0	0	0	0	0")
IFS=$'\t' read -r LAG_MAX LAG_P99_FULL LAG_P99 LAG_CURRENT LAG_SAMPLES LAG_STEADY_SAMPLES <<< "${LAG_DATA}"

# --- 2. Messages processed (Cloud Monitoring counter, all pods) ---
# SDLC success rate excludes code tasks (retries/dead-letters are expected for empty repos).
MSG_DATA=$(cm "gkg_etl_messages_processed_total/counter" | python3 -c "
import json,sys
d=json.load(sys.stdin)
sdlc_ok=0; sdlc_err=0; code_ok=0; code_err=0
for ts in d.get('timeSeries',[]):
    labels=ts['metric']['labels']
    outcome=labels.get('outcome','?')
    topic=labels.get('topic','')
    vals=[p['value']['doubleValue'] for p in ts.get('points',[])]
    if not vals: continue
    v=max(vals)
    is_code='code.task' in topic
    if outcome=='ack':
        if is_code: code_ok+=v
        else: sdlc_ok+=v
    elif outcome in ('nack','dead_letter','term'):
        if is_code: code_err+=v
        else: sdlc_err+=v
print(f'{int(sdlc_ok)}\t{int(sdlc_err)}\t{int(code_ok)}\t{int(code_err)}')
" 2>/dev/null || echo "0	0	0	0")
IFS=$'\t' read -r SDLC_OK SDLC_ERR CODE_OK CODE_ERR <<< "${MSG_DATA}"
SDLC_TOTAL=$((SDLC_OK + SDLC_ERR))
if [[ "${SDLC_TOTAL}" -gt 0 ]]; then
  MSG_RATE=$(python3 -c "print(round(${SDLC_OK} / ${SDLC_TOTAL}, 6))")
else
  MSG_RATE="1.0"
fi

# --- 3. CH query stats (survives restarts, on disk) ---
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

# --- 4. OOM count (kubectl) ---
OOM_COUNT=0
for ns in "${GKG_NS}" "${CH_NS}"; do
  while IFS= read -r reason; do
    [[ "${reason}" == "OOMKilled" ]] && OOM_COUNT=$((OOM_COUNT + 1))
  done < <($KC get pods -n "${ns}" \
    -o jsonpath='{range .items[*]}{range .status.containerStatuses[*]}{.lastState.terminated.reason}{"\n"}{end}{end}' 2>/dev/null || true)
done

# --- 5. Code indexing stats (Cloud Monitoring, all pods) ---
CODE_LANGS=$(cm "gkg_indexer_code_language_files_total/counter" | python3 -c "
import json,sys
d=json.load(sys.stdin)
langs={}
for ts in d.get('timeSeries',[]):
    lang=ts['metric']['labels'].get('language','?')
    vals=[p['value']['doubleValue'] for p in ts.get('points',[])]
    if vals: langs[lang]=langs.get(lang,0)+max(vals)
for l,c in sorted(langs.items(), key=lambda x:-x[1])[:8]:
    print(f'{l}={int(c)}', end=' ')
" 2>/dev/null || echo "")

CODE_NODES=$(cm "gkg_indexer_code_nodes_indexed_total/counter" | python3 -c "
import json,sys
d=json.load(sys.stdin)
kinds={}
for ts in d.get('timeSeries',[]):
    kind=ts['metric']['labels'].get('kind','?')
    vals=[p['value']['doubleValue'] for p in ts.get('points',[])]
    if vals: kinds[kind]=kinds.get(kind,0)+max(vals)
for k,c in sorted(kinds.items(), key=lambda x:-x[1]):
    print(f'{k}={int(c)}', end=' ')
" 2>/dev/null || echo "")

# --- 6. Graph + disk (CH) ---
GRAPH_ROWS=$(ch "SELECT formatReadableQuantity(sum(total_rows)) FROM system.tables WHERE database='gkg' AND total_rows > 0 FORMAT TSV")
DL_ROWS=$(ch "SELECT formatReadableQuantity(sum(total_rows)) FROM system.tables WHERE database='datalake' AND total_rows > 0 FORMAT TSV")
DISK=$(ch "SELECT concat(toString(round(100 * (total_bytes - available_bytes) / total_bytes)), '% of ', formatReadableSize(total_bytes)) FROM system.disks WHERE name='default' FORMAT TSV")
CH_PEAK=$(ch "SELECT formatReadableSize(max(memory_usage)) FROM system.query_log WHERE type='QueryFinish' AND current_database IN ('gkg','datalake') FORMAT TSV")

# --- Evaluate ---
pass() { python3 -c "print('PASS' if $1 else 'FAIL')"; }
P_P90=$(pass "${CH_P90:-0} <= ${T_P90}")
P_RATE=$(pass "${MSG_RATE} >= ${T_RATE}")
P_LAG=$(pass "${LAG_P99} <= ${T_LAG}")
P_OOM=$(pass "${OOM_COUNT} <= ${T_OOM}")

echo ""
echo "=== SLO Report: run=${RUN_ID} tier=${TIER} (${SLO_WINDOW_HOURS}h window) ==="
echo ""
printf "%-28s %10s %10s %6s  %s\n" "SLO" "Target" "Actual" "" "Source"
printf "%-28s %10s %10s %6s  %s\n" "---" "------" "------" "---" "------"
printf "%-28s %10s %10s %6s  %s\n" "query_p90_ms"            "<=${T_P90}"   "${CH_P90}"     "${P_P90}"  "CH query_log"
printf "%-28s %10s %10s %6s  %s\n" "query_success_rate"       ">=${T_RATE}" "${MSG_RATE}"   "${P_RATE}" "Cloud Monitoring"
printf "%-28s %10s %10s %6s  %s\n" "sdlc_watermark_lag_p99_s" "<=${T_LAG}"  "${LAG_P99}"   "${P_LAG}"  "Cloud Monitoring"
printf "%-28s %10s %10s %6s  %s\n" "oom_count"                "<=${T_OOM}"  "${OOM_COUNT}" "${P_OOM}"  "kubectl"
printf "%-28s %10s %10s %6s\n"     "code_backfill_hours_max"  "-" "-" "N/A"
printf "%-28s %10s %10s %6s\n"     "cpu_throttle_ratio_max"   "-" "-" "N/A"

echo ""
echo "--- Prometheus (Cloud Monitoring, all pods) ---"
echo "  watermark_lag: current=${LAG_CURRENT}s  steady-state p99=${LAG_P99}s (${LAG_STEADY_SAMPLES} samples)  peak=${LAG_MAX}s (${LAG_SAMPLES} samples)"
echo "  sdlc messages: ok=${SDLC_OK} err=${SDLC_ERR} rate=${MSG_RATE}"
echo "  code messages: ok=${CODE_OK} err=${CODE_ERR} (excluded from success rate)"
[[ -n "${CODE_LANGS}" ]] && echo "  code languages: ${CODE_LANGS}"
[[ -n "${CODE_NODES}" ]] && echo "  code nodes: ${CODE_NODES}"

echo ""
echo "--- ClickHouse ---"
echo "  queries: ${CH_TOTAL} total, ${CH_ERR} errors, p90=${CH_P90}ms"
[[ -n "${CH_PEAK}" ]] && echo "  peak query mem: ${CH_PEAK}"
echo "  graph: ${GRAPH_ROWS:-?}    datalake: ${DL_ROWS:-?}"
[[ -n "${DISK}" ]] && echo "  disk: ${DISK}"

echo ""
PASSED=0
for r in "${P_P90}" "${P_RATE}" "${P_LAG}" "${P_OOM}"; do [[ "${r}" == "PASS" ]] && PASSED=$((PASSED+1)); done
echo "=== ${PASSED}/4 SLOs passed ==="

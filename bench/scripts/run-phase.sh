#!/usr/bin/env bash
# Submit a phase Job to the cluster and wait for completion.
# All bench computation runs inside the cluster; the CI runner
# only submits, streams logs, and copies results out.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASE="${1:?usage: run-phase.sh <phase>}"
RUN_DIR="${BENCH_DIR}/results/${RUN_ID}"
mkdir -p "${RUN_DIR}"

: "${XTASK_IMAGE:?set XTASK_IMAGE to the bench container image}"
: "${SIPHON_STREAM:=e2e_siphon_event_stream}"
: "${SCHEMA_VERSION:=88}"
: "${RATE:=$(tier '.workload.steady_rows_per_sec')}"
: "${DURATION:=300}"
: "${QPS_LADDER:=$(tier '.workload.qps_ladder | join(",")')}"

NS_NATS="e2e-${RUN_ID}-nats"
NS_CH="e2e-${RUN_ID}-clickhouse"
NS_GKG="e2e-${RUN_ID}-gkg"
JOB_NS="${NS_GKG}"

# CH password from the StatefulSet (same pattern as import-dump-job.sh).
CH_PASSWORD=$($KC get statefulset clickhouse -n "${NS_CH}" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_PASSWORD")].value}')

$KC create secret generic ra-phase-ch-auth \
  -n "${JOB_NS}" \
  --from-literal=password="${CH_PASSWORD}" \
  --dry-run=client -o yaml | $KC apply -f -

# Upload config files (namespace_ids, manifest) as a ConfigMap.
CONFIG_ARGS=()
if [[ -f "${RUN_DIR}/namespace_ids.txt" ]]; then
  CONFIG_ARGS+=(--from-file=namespace_ids.txt="${RUN_DIR}/namespace_ids.txt")
fi
if [[ -f "${RUN_DIR}/manifest.tsv" ]]; then
  CONFIG_ARGS+=(--from-file=manifest.tsv="${RUN_DIR}/manifest.tsv")
fi
if [[ ${#CONFIG_ARGS[@]} -gt 0 ]]; then
  $KC create configmap ra-phase-config -n "${JOB_NS}" \
    "${CONFIG_ARGS[@]}" \
    --dry-run=client -o yaml | $KC apply -f -
fi

# Delete any previous phase Job with the same name.
$KC delete job "ra-phase-${PHASE}" -n "${JOB_NS}" --ignore-not-found=true 2>/dev/null

log "Submitting phase ${PHASE} job"

T_START=$(date -u +%s)

export PHASE XTASK_IMAGE SIPHON_STREAM SCHEMA_VERSION
export NATS_NAMESPACE="${NS_NATS}" CH_NAMESPACE="${NS_CH}"
export RATE DURATION QPS_LADDER STEP_DURATION="${DURATION}"

sed -e "s|\${PHASE}|${PHASE}|g" \
    -e "s|\${XTASK_IMAGE}|${XTASK_IMAGE}|g" \
    -e "s|\${NATS_NAMESPACE}|${NS_NATS}|g" \
    -e "s|\${CH_NAMESPACE}|${NS_CH}|g" \
    -e "s|\${SIPHON_STREAM}|${SIPHON_STREAM}|g" \
    -e "s|\${SCHEMA_VERSION}|${SCHEMA_VERSION}|g" \
    -e "s|\${RATE}|${RATE}|g" \
    -e "s|\${DURATION}|${DURATION}|g" \
    -e "s|\${QPS_LADDER}|${QPS_LADDER}|g" \
    -e "s|\${STEP_DURATION}|${DURATION}|g" \
  < "${BENCH_DIR}/manifests/phase-job.yaml" \
  | $KC apply -n "${JOB_NS}" -f -

# Stream logs.
$KC wait -n "${JOB_NS}" "job/ra-phase-${PHASE}" \
  --for=condition=ready --timeout=120s 2>/dev/null || true
$KC logs -n "${JOB_NS}" "job/ra-phase-${PHASE}" -f --tail=1000 2>/dev/null &
LOG_PID=$!

if $KC wait -n "${JOB_NS}" "job/ra-phase-${PHASE}" \
  --for=condition=complete --timeout=7200s 2>/dev/null; then
  log "Phase ${PHASE} completed"
else
  log "Phase ${PHASE} failed"
  $KC describe "job/ra-phase-${PHASE}" -n "${JOB_NS}" 2>/dev/null || true
  kill "${LOG_PID}" 2>/dev/null || true
  exit 1
fi

kill "${LOG_PID}" 2>/dev/null || true

T_END=$(date -u +%s)
log "Phase ${PHASE} elapsed: $((T_END - T_START))s"
echo "{\"phase\":\"${PHASE}\",\"t_start\":${T_START},\"t_end\":${T_END}}" >> "${RUN_DIR}/phases.jsonl"

# Copy results from the pod (if any were written).
POD=$($KC get pods -n "${JOB_NS}" -l "job-name=ra-phase-${PHASE}" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -n "${POD}" ]]; then
  $KC cp "${JOB_NS}/${POD}:/results/" "${RUN_DIR}/" 2>/dev/null || true
fi

# Clean up.
$KC delete secret ra-phase-ch-auth -n "${JOB_NS}" --ignore-not-found=true 2>/dev/null

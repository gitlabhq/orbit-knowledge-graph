#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASE="${1:?usage: run-phase.sh <phase>}"
RUN_DIR="${BENCH_DIR}/results/${RUN_ID}"
mkdir -p "${RUN_DIR}"

: "${RATE:=$(tier '.workload.steady_rows_per_sec')}"
: "${DURATION:=1800}"
: "${MIX:=${BENCH_DIR}/query-mixes/baseline}"
: "${QPS_LADDER:=$(tier '.workload.qps_ladder | join(",")')}"
: "${MANIFEST:=${RUN_DIR}/manifest.tsv}"
: "${NS_FILE:=${RUN_DIR}/namespace_ids.txt}"
: "${PROJ_FILE:=${RUN_DIR}/project_ids.txt}"

T_START=$(date -u +%s)
log "Phase ${PHASE} start (${T_START})"

case "${PHASE}" in
  p1)
    log "P1: SDLC backfill via enrollment trigger"
    cargo xtask bench publish-triggers \
      --stream "${SIPHON_STREAM:-e2e_siphon_event_stream}" \
      --nats-url "${NATS_URL:-nats://localhost:4222}" \
      --kind enrollment --ids "${NS_FILE}"
    log "Waiting for backfill to drain..."
    sleep 60
    ;;
  p2)
    log "P2: code backfill via code-task triggers"
    cargo xtask bench publish-triggers \
      --stream "${SIPHON_STREAM:-e2e_siphon_event_stream}" \
      --nats-url "${NATS_URL:-nats://localhost:4222}" \
      --kind code-task --ids "${PROJ_FILE}"
    log "Waiting for code queue to drain..."
    sleep 120
    ;;
  p3a)
    log "P3a: steady-state SDLC replay (GKG isolated, ${RATE} rows/s, ${DURATION}s)"
    cargo xtask bench replay-sdlc \
      --rate "${RATE}" --duration-secs "${DURATION}" \
      --manifest "${MANIFEST}" \
      --ch-url "${CH_URL:-http://localhost:8123}" \
      --results "${RUN_DIR}/p3a-replay-results.json"
    ;;
  p3b)
    log "P3b: steady-state via PG replay (full DIP, ${RATE} rows/s, ${DURATION}s)"
    cargo xtask bench replay-pg \
      --rate "${RATE}" --duration-secs "${DURATION}" \
      --pg-url "${PG_URL:?PG_URL required for P3b}" \
      --ch-url "${CH_URL:-http://localhost:8123}" \
      --results "${RUN_DIR}/p3b-replay-results.json"
    ;;
  p4)
    log "P4: query ladder (QPS: ${QPS_LADDER})"
    cargo xtask bench query-load \
      --mix "${MIX}" --qps "${QPS_LADDER}" \
      --duration-per-step-secs 60 \
      --ch-url "${CH_URL:-http://localhost:8123}" \
      --schema-version "${SCHEMA_VERSION:?set SCHEMA_VERSION}" \
      --results "${RUN_DIR}/p4-query-results.json"
    ;;
  p5)
    log "P5: mixed (P3a + P4 concurrent)"
    PHASE=p3a "${BASH_SOURCE[0]}" p3a &
    PID_P3=$!
    PHASE=p4 "${BASH_SOURCE[0]}" p4 &
    PID_P4=$!
    wait "${PID_P3}" "${PID_P4}"
    ;;
  *)
    log "Unknown phase: ${PHASE}"
    exit 1
    ;;
esac

T_END=$(date -u +%s)
log "Phase ${PHASE} end (${T_END}, elapsed $((T_END - T_START))s)"

echo "{\"phase\":\"${PHASE}\",\"t_start\":${T_START},\"t_end\":${T_END}}" >> "${RUN_DIR}/phases.jsonl"

"${BENCH_DIR}/scripts/snapshot-ch.sh" "${PHASE}" "${T_START}" "${T_END}" || true

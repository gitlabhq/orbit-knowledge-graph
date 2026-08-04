#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${PHASES:=p1 p2 p3a p4}"

heartbeat_start
trap heartbeat_stop EXIT

for phase in ${PHASES}; do
  "${BENCH_DIR}/scripts/run-phase.sh" "${phase}"
done

log "All phases complete"

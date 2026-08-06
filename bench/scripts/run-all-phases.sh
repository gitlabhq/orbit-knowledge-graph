#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

: "${PHASES:=p1 p4}"

for phase in ${PHASES}; do
  "${BENCH_DIR}/scripts/run-phase.sh" "${phase}"
done

log "All phases complete"

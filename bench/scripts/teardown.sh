#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

log "Teardown for run=${RUN_ID}"

for suffix in gkg nats clickhouse gitlab siphon; do
  $KC delete ns "e2e-${RUN_ID}-${suffix}" --wait=false 2>/dev/null || true
done

log "Teardown complete"

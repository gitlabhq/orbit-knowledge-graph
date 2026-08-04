#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

FAIL=0
check() {
  local name="$1"; shift
  if "$@" > /dev/null 2>&1; then
    log "  PASS: ${name}"
  else
    log "  FAIL: ${name}"
    FAIL=1
  fi
}

NS="e2e-${RUN_ID}-gkg"
CH_NS="e2e-${RUN_ID}-clickhouse"

log "Validation gate for run=${RUN_ID}"

check "gkg namespace exists" $KC get ns "${NS}"

check "graph schema version resolves" $KC exec -n "${CH_NS}" deploy/clickhouse -- \
  clickhouse-client --query "SELECT max(version) FROM gkg.gkg_schema_version WHERE status = 'active'"

check "datalake tables have watermark column" $KC exec -n "${CH_NS}" deploy/clickhouse -- \
  clickhouse-client --query \
    "SELECT count() FROM system.columns WHERE database = 'datalake' AND name = '_siphon_watermark' HAVING count() > 0"

check "gkg metrics scrapeable" $KC exec -n "${NS}" deploy/orbit-gkg-webserver -- \
  wget -q -O /dev/null http://localhost:9394/metrics

check "smoke DSL query returns" $KC exec -n "${NS}" deploy/orbit-gkg-webserver -- \
  wget -q -O /dev/null "http://localhost:50054/healthz"

if [[ ${FAIL} -ne 0 ]]; then
  log "Validation FAILED; stack is not a measurement environment."
  exit 1
fi

log "Validation passed"

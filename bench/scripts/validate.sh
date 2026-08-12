#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

NS="e2e-${RUN_ID}-gkg"
CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"

log "Waiting for GKG pods to be ready (up to 10 min)..."
$KC wait -n "${NS}" deploy -l app.kubernetes.io/name=gkg --for=condition=available --timeout=600s 2>/dev/null || true

log "Waiting for ClickHouse to be ready..."
$KC rollout status -n "${CH_NS}" statefulset/clickhouse --timeout=300s 2>/dev/null || true

CH_PASS=$($KC get statefulset clickhouse -n "${CH_NS}" \
  -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="CLICKHOUSE_PASSWORD")].value}' 2>/dev/null)

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
warn_check() {
  local name="$1"; shift
  if "$@" > /dev/null 2>&1; then
    log "  PASS: ${name}"
  else
    log "  WARN: ${name} (non-fatal)"
  fi
}

log "Validation gate for run=${RUN_ID}"

check "gkg namespace exists" $KC get ns "${NS}"

check "graph schema version resolves" $KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASS}" \
  --query "SELECT max(version) FROM gkg.gkg_schema_version WHERE status = 'active'"

check "datalake tables have watermark column" $KC exec -n "${CH_NS}" clickhouse-0 -- \
  clickhouse-client --password "${CH_PASS}" \
  --query "SELECT count() FROM system.columns WHERE database = 'datalake' AND name = '_siphon_watermark' HAVING count() > 0"

# These require metrics enabled and wget in the container image.
# Non-fatal until the tier overlay is wired into setup.sh.
warn_check "gkg metrics scrapeable" $KC exec -n "${NS}" deploy/gkg-webserver -- \
  wget -q -O /dev/null http://localhost:9394/metrics

warn_check "smoke healthz" $KC exec -n "${NS}" deploy/gkg-webserver -- \
  wget -q -O /dev/null "http://localhost:50054/healthz"

if [[ ${FAIL} -ne 0 ]]; then
  log "Validation FAILED; stack is not a measurement environment."
  log "Pod status:"
  $KC get pods -n "${NS}" 2>/dev/null || true
  $KC get pods -n "${CH_NS}" 2>/dev/null || true
  exit 1
fi

log "Validation passed"

#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

JOB_NAME="e2e-robot-runner"
RELEASE_NAME="e2e-robot-runner"
DIAG_DIR="${E2E_DIR}/diagnostics"
MARKER_DIR=$(mktemp -d)
trap 'rm -rf "$MARKER_DIR"' EXIT

log "E2E Tests (SHA: $E2E_SHA)"
mkdir -p "$DIAG_DIR"

# Cleanup previous run
helm uninstall "$RELEASE_NAME" -n "$NS_GKG" --kube-context "$KCTX" 2>/dev/null || true
$KC delete configmap e2e-robot-tests -n "$NS_GKG" --ignore-not-found 2>/dev/null

# Upload test files
log "Creating ConfigMap from test files"
$KC create configmap e2e-robot-tests -n "$NS_GKG" \
  --from-file="$E2E_DIR/tests/"

# Install robot-runner chart
log "Launching Robot Framework job"
helm install "$RELEASE_NAME" "$E2E_DIR/charts/robot-runner" \
  --namespace "$NS_GKG" \
  --kube-context "$KCTX" \
  --set "namespaces.gitlab=$NS_GITLAB" \
  --set "namespaces.gkg=$NS_GKG"

# Stream logs in background (retry until pod ready, follow once)
(while ! $KC logs -f job/"$JOB_NAME" -n "$NS_GKG" 2>/dev/null; do
  { [ -f "${MARKER_DIR}/pass" ] || [ -f "${MARKER_DIR}/fail" ]; } && break
  sleep 2
done) &
LOG_PID=$!

# Race two kubectl waits — first to fire determines result
log "Waiting for tests to complete (timeout: 1h)..."
($KC wait --for=condition=complete job/"$JOB_NAME" -n "$NS_GKG" --timeout=3600s 2>/dev/null \
  && touch "${MARKER_DIR}/pass") &
($KC wait --for=condition=failed job/"$JOB_NAME" -n "$NS_GKG" --timeout=3600s 2>/dev/null \
  && touch "${MARKER_DIR}/fail") &

while [ ! -f "${MARKER_DIR}/pass" ] && [ ! -f "${MARKER_DIR}/fail" ]; do sleep 1; done

kill $LOG_PID 2>/dev/null || true
jobs -p | xargs kill 2>/dev/null || true
wait 2>/dev/null || true

# Extract JUnit report from job logs (kubectl cp can't exec into completed pods)
log "Extracting test report"
$KC logs job/"$JOB_NAME" -n "$NS_GKG" 2>/dev/null | \
  sed -n '/---XUNIT_REPORT_START---/,/---XUNIT_REPORT_END---/p' | \
  sed '/---XUNIT_REPORT_/d' \
  > "$DIAG_DIR/junit.xml" || true

if [ -f "${MARKER_DIR}/pass" ]; then
  log "Tests passed"
  exit 0
fi

log "Tests failed"

log "Collecting diagnostics to $DIAG_DIR"
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_SIPHON" "$NS_GKG"; do
  ns_short="${ns#e2e-${E2E_SHA}-}"
  echo "--- $ns ---"
  $KC get pods -n "$ns" --no-headers 2>/dev/null | tee "$DIAG_DIR/${ns_short}-pods.txt" || true

  $KC get events -n "$ns" --sort-by=.lastTimestamp 2>/dev/null > "$DIAG_DIR/${ns_short}-events.txt" || true

  for pod in $($KC get pods -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    if [[ "$ns_short" == "siphon" || "$ns_short" == "gkg" ]]; then
      $KC logs "$pod" -n "$ns" --tail=200 > "$DIAG_DIR/${ns_short}-${pod}.log" 2>/dev/null || true
    fi

    restarts=$($KC get pod "$pod" -n "$ns" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)
    reason=$($KC get pod "$pod" -n "$ns" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}' 2>/dev/null)
    if [[ "$restarts" -gt 0 || -n "$reason" ]]; then
      echo "  $pod: restarts=$restarts reason=${reason:-unknown}"
      $KC logs "$pod" -n "$ns" --previous --tail=50 > "$DIAG_DIR/${ns_short}-${pod}-previous.log" 2>/dev/null || true
    fi
  done
  echo ""
done

exit 1

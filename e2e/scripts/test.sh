#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

JOB_NAME="e2e-robot-runner"
RELEASE_NAME="e2e-robot-runner"

log "E2E Tests (SHA: $E2E_SHA)"

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

# Wait for completion or failure
log "Waiting for tests to complete..."
while true; do
  CONDITIONS=$($KC get job "$JOB_NAME" -n "$NS_GKG" \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status}{"\n"}{end}' 2>/dev/null)
  if echo "$CONDITIONS" | grep -q "Complete=True"; then
    log "Tests passed"
    echo ""
    $KC logs job/"$JOB_NAME" -n "$NS_GKG" 2>/dev/null
    exit 0
  fi
  if echo "$CONDITIONS" | grep -q "Failed=True"; then
    break
  fi
  sleep 5
done

log "Tests failed"
echo ""
$KC logs job/"$JOB_NAME" -n "$NS_GKG" 2>/dev/null

DIAG_DIR="${E2E_DIR}/diagnostics"
mkdir -p "$DIAG_DIR"

log "Collecting diagnostics to $DIAG_DIR"
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_SIPHON" "$NS_GKG"; do
  ns_short="${ns#e2e-${E2E_SHA}-}"
  echo "--- $ns ---"
  $KC get pods -n "$ns" --no-headers 2>/dev/null | tee "$DIAG_DIR/${ns_short}-pods.txt" || true

  $KC get events -n "$ns" --sort-by=.lastTimestamp 2>/dev/null > "$DIAG_DIR/${ns_short}-events.txt" || true

  for pod in $($KC get pods -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    # Always dump current logs for siphon and gkg pods
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

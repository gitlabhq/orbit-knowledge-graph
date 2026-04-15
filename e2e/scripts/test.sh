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

echo ""
log "Diagnostics:"
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_GKG"; do
  echo "--- $ns ---"
  $KC get pods -n "$ns" --no-headers 2>/dev/null || echo "  namespace not found"
  echo ""
done

exit 1

#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

JOB_NAME="e2e-robot-runner"
RELEASE_NAME="e2e-robot-runner"
DIAG_DIR="${E2E_DIR}/diagnostics"
TIMEOUT_SECONDS=1800
POLL_INTERVAL=5

log "E2E Tests (SHA: $E2E_SHA)"
mkdir -p "$DIAG_DIR"

# Cleanup previous run
helm uninstall "$RELEASE_NAME" -n "$NS_GKG" --kube-context "$KCTX" 2>/dev/null || true
$KC delete configmap e2e-robot-tests -n "$NS_GKG" --ignore-not-found 2>/dev/null
$KC delete configmap e2e-robot-fixtures -n "$NS_GKG" --ignore-not-found 2>/dev/null

# Upload test files
log "Creating ConfigMap from test files"
$KC create configmap e2e-robot-tests -n "$NS_GKG" \
  --from-file="$E2E_DIR/tests/"

# Bundle fixtures (binary tarball — preserves directory structure and exec bits
# that --from-file= would flatten/strip). Robot job extracts at startup into
# /fixtures and the git.resource pushes them to per-test GitLab projects.
log "Bundling fixtures into ConfigMap"
mkdir -p "$GKG_ROOT/.tmp"
FIX_TMP="$(mktemp -d -p "$GKG_ROOT/.tmp" robot-fixtures.XXXXXX)"
trap 'rm -rf "$FIX_TMP"' EXIT
tar czf "$FIX_TMP/fixtures.tar.gz" -C "$E2E_DIR/fixtures" .
$KC create configmap e2e-robot-fixtures -n "$NS_GKG" \
  --from-file=fixtures.tar.gz="$FIX_TMP/fixtures.tar.gz"

# Install robot-runner chart
log "Launching Robot Framework job"
HELM_ARGS=(install "$RELEASE_NAME" "$E2E_DIR/charts/robot-runner"
  --namespace "$NS_GKG"
  --kube-context "$KCTX"
  --set "namespaces.gitlab=$NS_GITLAB"
  --set "namespaces.gkg=$NS_GKG")
if [ -n "${E2E_ROBOT_IMAGE:-}" ]; then
  HELM_ARGS+=(--set "image=$E2E_ROBOT_IMAGE")
fi
helm "${HELM_ARGS[@]}"

# Poll job status until terminal condition or timeout. Heartbeat keeps the
# pipeline trace alive without the complexity (and orphaned-kubectl bugs) of
# streaming `kubectl logs -f` from a background subshell.
log "Waiting for tests to complete (timeout: ${TIMEOUT_SECONDS}s)"
result=timeout
SECONDS=0
while [ "$SECONDS" -lt "$TIMEOUT_SECONDS" ]; do
  # Modern Job objects emit multiple True conditions (SuccessCriteriaMet +
  # Complete on success; FailureTarget + Failed on failure), so jsonpath
  # returns a space-joined list. Match Complete/Failed via substring.
  status=$($KC get job/"$JOB_NAME" -n "$NS_GKG" \
    -o jsonpath='{range .status.conditions[?(@.status=="True")]}{.type} {end}' 2>/dev/null || true)
  case " $status " in
    *" Complete "*) result=pass; break ;;
    *" Failed "*)   result=fail; break ;;
  esac
  log "Tests running... (${SECONDS}s elapsed)"
  sleep "$POLL_INTERVAL"
done

log "Robot Framework output:"
$KC logs job/"$JOB_NAME" -n "$NS_GKG" --tail=-1 2>&1 || true

# Extract JUnit report from job logs (kubectl cp can't exec into completed pods)
log "Extracting test report"
$KC logs job/"$JOB_NAME" -n "$NS_GKG" 2>/dev/null | \
  sed -n '/---XUNIT_REPORT_START---/,/---XUNIT_REPORT_END---/p' | \
  sed '/---XUNIT_REPORT_/d' \
  > "$DIAG_DIR/junit.xml" || true

log "Capturing GKG pod logs"
GKG_NS_SHORT="${NS_GKG#e2e-${E2E_SHA}-}"
$KC get pods -n "$NS_GKG" --no-headers 2>/dev/null \
  | tee "$DIAG_DIR/${GKG_NS_SHORT}-pods.txt" || true
$KC get events -n "$NS_GKG" --sort-by=.lastTimestamp 2>/dev/null \
  > "$DIAG_DIR/${GKG_NS_SHORT}-events.txt" || true
for pod in $($KC get pods -n "$NS_GKG" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
  $KC logs "$pod" -n "$NS_GKG" --all-containers --prefix 2>/dev/null \
    > "$DIAG_DIR/${GKG_NS_SHORT}-${pod}.log" || true
done

if [ "$result" = "pass" ]; then
  log "Tests passed"
  exit 0
fi

log "Tests $result"

# In CI the failure-gated after_script dumps diagnostics; don't dump twice.
if [ -z "${CI:-}" ]; then
  bash "$(dirname "${BASH_SOURCE[0]}")/dump-diagnostics.sh"
fi

exit 1

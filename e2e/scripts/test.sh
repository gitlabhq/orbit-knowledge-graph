#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

JOB_NAME="e2e-robot-runner"
RELEASE_NAME="e2e-robot-runner"
DIAG_DIR="${E2E_DIR}/diagnostics"
TIMEOUT_SECONDS=1800
POLL_INTERVAL=3

log "E2E Tests (SHA: $E2E_SHA)"
mkdir -p "$DIAG_DIR"

helm uninstall "$RELEASE_NAME" -n "$NS_GKG" --kube-context "$KCTX" 2>/dev/null || true
$KC delete configmap e2e-robot-tests -n "$NS_GKG" --ignore-not-found 2>/dev/null
$KC delete configmap e2e-robot-fixtures -n "$NS_GKG" --ignore-not-found 2>/dev/null

log "Creating ConfigMap from test files"
$KC create configmap e2e-robot-tests -n "$NS_GKG" \
  --from-file="$E2E_DIR/tests/"

log "Bundling fixtures into ConfigMap"
mkdir -p "$GKG_ROOT/.tmp"
FIX_TMP="$(mktemp -d -p "$GKG_ROOT/.tmp" robot-fixtures.XXXXXX)"
trap 'rm -rf "$FIX_TMP"' EXIT
tar czf "$FIX_TMP/fixtures.tar.gz" -C "$E2E_DIR/fixtures" .
$KC create configmap e2e-robot-fixtures -n "$NS_GKG" \
  --from-file=fixtures.tar.gz="$FIX_TMP/fixtures.tar.gz"

log "Launching Robot Framework job"
HELM_ARGS=(install "$RELEASE_NAME" "$E2E_DIR/charts/robot-runner"
  --namespace "$NS_GKG"
  --kube-context "$KCTX"
  --set "namespaces.gitlab=$NS_GITLAB"
  --set "namespaces.gkg=$NS_GKG"
  --set "indexingBudgetMultiplier=${E2E_INDEXING_BUDGET_MULTIPLIER:-1}"
  --set-string "codeGraphIncremental=${E2E_CODE_GRAPH_INCREMENTAL:-false}")
if [ -n "${E2E_ROBOT_IMAGE:-}" ]; then
  HELM_ARGS+=(--set "image=$E2E_ROBOT_IMAGE")
fi
if [ -n "${E2E_CLI_IMAGE:-}" ]; then
  HELM_ARGS+=(--set "cliImage=$E2E_CLI_IMAGE")
fi
helm "${HELM_ARGS[@]}"

"$E2E_DIR/scripts/ch-chaos.sh" indexing &
CHAOS_PID=$!
trap 'rm -rf "$FIX_TMP"; kill "$CHAOS_PID" 2>/dev/null || true' EXIT

log "Waiting for tests to complete (timeout: ${TIMEOUT_SECONDS}s)"
result=timeout
SECONDS=0
while [ "$SECONDS" -lt "$TIMEOUT_SECONDS" ]; do
  status=$($KC get job/"$JOB_NAME" -n "$NS_GKG" \
    -o jsonpath='{range .status.conditions[?(@.status=="True")]}{.type} {end}' 2>/dev/null || true)
  case " $status " in
    *" Complete "*) result=pass; break ;;
    *" Failed "*)   result=fail; break ;;
  esac
  log "Tests running... (${SECONDS}s elapsed)"
  sleep "$POLL_INTERVAL"
done

CHAOS_FAILED=0
if [ "$result" = "pass" ]; then
  wait "$CHAOS_PID" || CHAOS_FAILED=1
else
  kill "$CHAOS_PID" 2>/dev/null || true
fi

log "Robot Framework output:"
$KC logs job/"$JOB_NAME" -n "$NS_GKG" --tail=-1 2>&1 || true

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

log "Capturing Siphon and Sidekiq logs"
for pod in $($KC get pods -n "$NS_SIPHON" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
  $KC logs "$pod" -n "$NS_SIPHON" --all-containers 2>/dev/null \
    | gzip > "$DIAG_DIR/siphon-${pod}.log.gz" || true
done
for pod in $($KC get pods -n "$NS_GITLAB" -l app=sidekiq -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
  $KC logs "$pod" -n "$NS_GITLAB" --all-containers 2>/dev/null \
    | gzip > "$DIAG_DIR/gitlab-${pod}.log.gz" || true
done

if [ "$result" = "pass" ] && [ "$CHAOS_FAILED" -eq 0 ]; then
  log "Tests passed"
  exit 0
fi
[ "$CHAOS_FAILED" -eq 0 ] || log "ch-chaos.sh indexing failed"

log "Tests $result"

if [ -z "${CI:-}" ]; then
  bash "$(dirname "${BASH_SOURCE[0]}")/dump-diagnostics.sh"
fi

exit 1

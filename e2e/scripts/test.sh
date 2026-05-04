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
helm install "$RELEASE_NAME" "$E2E_DIR/charts/robot-runner" \
  --namespace "$NS_GKG" \
  --kube-context "$KCTX" \
  --set "namespaces.gitlab=$NS_GITLAB" \
  --set "namespaces.gkg=$NS_GKG"

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

log "Collecting diagnostics to $DIAG_DIR"

# --- ClickHouse state dump (datalake + gkg databases) -----------------------
# Captures schema, row counts, dictionary state, recent errors, and full
# contents of small high-value tables so failures can be diagnosed offline
# without re-running the test. The dump runs against the live cluster before
# teardown removes the namespaces.
log "Dumping ClickHouse state"
CH_POD=$($KC get pods -n "$NS_CH" -l app=clickhouse -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$CH_POD" ]]; then
  ch_query() {
    # Pipe SQL via stdin so we don't need to escape quotes in the query string.
    # Password lives in the pod's CLICKHOUSE_PASSWORD env var (set by the chart).
    printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
      sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"' 2>&1
  }

  # Tables + row counts + sizes per database
  for db in datalake gkg; do
    ch_query "SELECT name, total_rows, formatReadableSize(total_bytes) AS size, engine
              FROM system.tables WHERE database = '$db'
              ORDER BY name FORMAT PrettyCompact" \
      > "$DIAG_DIR/clickhouse-${db}-tables.txt" || true
  done

  # Dictionary state — cache hit rate, last refresh time, errors. Critical
  # for diagnosing dictGet-via-DEFAULT issues (namespace_traversal_paths_dict
  # etc.) that surface as stale '0/' traversal_path in derived tables.
  ch_query "SELECT name, status, last_successful_update_time, element_count,
                   found_rate, last_exception
            FROM system.dictionaries WHERE database = 'datalake'
            FORMAT Vertical" \
    > "$DIAG_DIR/clickhouse-dictionaries.txt" || true

  # Server-wide error counters — surfaces query failures, INSERT errors,
  # background merge issues that don't show up in pod logs.
  ch_query "SELECT name, value, last_error_time, last_error_message
            FROM system.errors WHERE value > 0
            ORDER BY last_error_time DESC LIMIT 50 FORMAT Vertical" \
    > "$DIAG_DIR/clickhouse-errors.txt" || true

  # Schema (CREATE TABLE statements) for both DBs
  ch_query "SELECT database, name, create_table_query
            FROM system.tables WHERE database IN ('datalake','gkg')
            ORDER BY database, name FORMAT Vertical" \
    > "$DIAG_DIR/clickhouse-schema.txt" || true

  # Full contents of every base table in datalake and gkg, discovered
  # dynamically. The graph DB tables are prefixed at runtime by the indexer
  # (e.g. v1_gl_project per config/SCHEMA_VERSION) so we can't hardcode names.
  # Filtering on engine != 'View' skips materialized-view definitions
  # themselves; their target tables are still captured. Capped per-table to
  # keep dumps manageable on chatty tables.
  CH_DUMP_TABLES=$(ch_query "SELECT database || '.' || name FROM system.tables
                             WHERE database IN ('datalake','gkg')
                               AND engine NOT IN ('View','Null','Dictionary')
                             ORDER BY database, name FORMAT TSV" 2>/dev/null || true)
  for tbl in $CH_DUMP_TABLES; do
    safe=$(echo "$tbl" | tr '.' '-')
    ch_query "SELECT * FROM $tbl FINAL ORDER BY ALL LIMIT 1000 FORMAT Vertical" \
      > "$DIAG_DIR/clickhouse-${safe}.txt" || true
  done
else
  log "No ClickHouse pod found in $NS_CH; skipping CH dump"
fi

# --- NATS jetstream state ---------------------------------------------------
log "Dumping NATS jetstream state"
NATS_BOX=$($KC get pods -n "$NS_NATS" -l app.kubernetes.io/name=nats-box -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$NATS_BOX" ]]; then
  $KC exec -n "$NS_NATS" "$NATS_BOX" -- nats stream list --all 2>&1 \
    > "$DIAG_DIR/nats-streams.txt" || true
  $KC exec -n "$NS_NATS" "$NATS_BOX" -- nats consumer list --all 2>&1 \
    > "$DIAG_DIR/nats-consumers.txt" || true
  $KC exec -n "$NS_NATS" "$NATS_BOX" -- nats stream info e2e_siphon_event_stream 2>&1 \
    > "$DIAG_DIR/nats-stream-info.txt" || true
fi

# --- Per-namespace pod state, events, and full logs (no tail truncation) ----
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_SIPHON" "$NS_GKG"; do
  ns_short="${ns#e2e-${E2E_SHA}-}"
  echo "--- $ns ---"
  $KC get pods -n "$ns" --no-headers 2>/dev/null | tee "$DIAG_DIR/${ns_short}-pods.txt" || true

  $KC get events -n "$ns" --sort-by=.lastTimestamp 2>/dev/null \
    > "$DIAG_DIR/${ns_short}-events.txt" || true

  for pod in $($KC get pods -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    # Capture full logs for every pod (no --tail limit). Sidecars included.
    $KC logs "$pod" -n "$ns" --all-containers --prefix 2>/dev/null \
      > "$DIAG_DIR/${ns_short}-${pod}.log" || true

    restarts=$($KC get pod "$pod" -n "$ns" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)
    reason=$($KC get pod "$pod" -n "$ns" -o jsonpath='{.status.containerStatuses[0].lastState.terminated.reason}' 2>/dev/null)
    if [[ "$restarts" -gt 0 || -n "$reason" ]]; then
      echo "  $pod: restarts=$restarts reason=${reason:-unknown}"
      $KC logs "$pod" -n "$ns" --all-containers --prefix --previous 2>/dev/null \
        > "$DIAG_DIR/${ns_short}-${pod}-previous.log" || true
    fi
  done
  echo ""
done

exit 1

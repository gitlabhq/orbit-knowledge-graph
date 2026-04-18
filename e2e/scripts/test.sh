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
  --set "namespaces.gkg=$NS_GKG" \
  --set "namespaces.clickhouse=$NS_CH"

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

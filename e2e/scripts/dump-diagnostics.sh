#!/usr/bin/env bash
# Snapshot e2e cluster state into $E2E_DIR/diagnostics/. Called both from
# test.sh on assertion failure (full ClickHouse + NATS dumps available) and
# from .gitlab-ci.yml after_script on helm install timeout (only k8s state
# available). Data sources are probed before use, so deploy-failure runs
# silently skip CH/NATS dumps.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
set +e  # never fail the caller

DIAG_DIR="${E2E_DIR}/diagnostics"
mkdir -p "$DIAG_DIR"

log "Collecting diagnostics to $DIAG_DIR"

# --- Helm releases ---------------------------------------------------------
# `--all` includes failed/pending-install releases that vanish from the
# default view; `helm status --show-resources` enumerates every k8s object
# the release tried to create, including ones that never became Ready.
helm list -A --all --kube-context "$KCTX" 2>&1 > "$DIAG_DIR/helm-list.txt"
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_SIPHON" "$NS_GKG"; do
  $KC get ns "$ns" >/dev/null 2>&1 || continue
  ns_short="${ns#e2e-${E2E_SHA}-}"
  for rel in $(helm list -n "$ns" --all --kube-context "$KCTX" -q 2>/dev/null); do
    helm status "$rel" -n "$ns" --kube-context "$KCTX" --show-resources 2>&1 \
      > "$DIAG_DIR/helm-${ns_short}-${rel}-status.txt"
  done
done

# --- Cluster-wide events + nodes -------------------------------------------
# Catches admission webhook denials, cluster-scoped resource conflicts,
# and node pressure (DiskPressure/MemoryPressure) that slow image pulls
# and push gkg release past --timeout=600s. `top nodes` may fail if
# metrics-server is absent; that's fine.
$KC get events -A --sort-by=.lastTimestamp 2>&1 | tail -n 200 \
  > "$DIAG_DIR/events-cluster.txt"
$KC get nodes -o wide 2>&1 > "$DIAG_DIR/nodes.txt"
$KC describe nodes 2>&1 > "$DIAG_DIR/nodes-describe.txt"
$KC top nodes 2>&1 > "$DIAG_DIR/nodes-top.txt"
$KC get ns -o name 2>/dev/null | grep '^namespace/e2e-' | sort \
  > "$DIAG_DIR/e2e-namespaces.txt"

# --- Per-namespace pods, events, workloads, logs ---------------------------
# `describe pod` is the highest-signal output for stuck rollouts: it
# surfaces ImagePullBackOff, ReadinessProbe failure messages, OOMKilled,
# scheduling failures, and PVC binding issues that don't show up in
# `kubectl logs`. Only run it for unready/restarted pods to keep volume
# manageable.
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_SIPHON" "$NS_GKG"; do
  $KC get ns "$ns" >/dev/null 2>&1 || continue
  ns_short="${ns#e2e-${E2E_SHA}-}"
  echo "--- $ns ---"

  $KC get pods -n "$ns" -o wide 2>/dev/null \
    | tee "$DIAG_DIR/${ns_short}-pods.txt"
  $KC get events -n "$ns" --sort-by=.lastTimestamp 2>&1 \
    > "$DIAG_DIR/${ns_short}-events.txt"
  $KC get deploy,sts,ds,job -n "$ns" -o wide 2>&1 \
    > "$DIAG_DIR/${ns_short}-workloads.txt"

  for pod in $($KC get pods -n "$ns" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    $KC logs "$pod" -n "$ns" --all-containers --prefix 2>/dev/null \
      > "$DIAG_DIR/${ns_short}-${pod}.log"

    ready=$($KC get pod "$pod" -n "$ns" \
      -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)
    restarts=$($KC get pod "$pod" -n "$ns" \
      -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null)
    if [[ "$ready" != "True" || "${restarts:-0}" -gt 0 ]]; then
      echo "  $pod: ready=$ready restarts=${restarts:-0}"
      $KC describe pod "$pod" -n "$ns" 2>&1 \
        > "$DIAG_DIR/${ns_short}-${pod}-describe.txt"
      $KC logs "$pod" -n "$ns" --all-containers --prefix --previous 2>/dev/null \
        > "$DIAG_DIR/${ns_short}-${pod}-previous.log"
    fi
  done
  echo ""
done

# --- ClickHouse state (skipped if CH pod is absent) ------------------------
# Captures schema, row counts, dictionary state, recent errors, and full
# contents of base tables so data-correctness failures can be diagnosed
# offline without re-running the test.
CH_POD=$($KC get pods -n "$NS_CH" -l app=clickhouse -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -n "$CH_POD" ]]; then
  log "Dumping ClickHouse state"
  ch_query() {
    # Pipe SQL via stdin so we don't need to escape quotes in the query
    # string. Password lives in the pod's CLICKHOUSE_PASSWORD env var.
    printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
      sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"' 2>&1
  }

  for db in datalake gkg; do
    ch_query "SELECT name, total_rows, formatReadableSize(total_bytes) AS size, engine
              FROM system.tables WHERE database = '$db'
              ORDER BY name FORMAT PrettyCompact" \
      > "$DIAG_DIR/clickhouse-${db}-tables.txt"
  done

  # Cache hit rate, last refresh time, errors. Critical for diagnosing
  # dictGet-via-DEFAULT issues (namespace_traversal_paths_dict etc.) that
  # surface as stale '0/' traversal_path in derived tables.
  ch_query "SELECT name, status, last_successful_update_time, element_count,
                   found_rate, last_exception
            FROM system.dictionaries WHERE database = 'datalake'
            FORMAT Vertical" \
    > "$DIAG_DIR/clickhouse-dictionaries.txt"

  ch_query "SELECT name, value, last_error_time, last_error_message
            FROM system.errors WHERE value > 0
            ORDER BY last_error_time DESC LIMIT 50 FORMAT Vertical" \
    > "$DIAG_DIR/clickhouse-errors.txt"

  ch_query "SELECT database, name, create_table_query
            FROM system.tables WHERE database IN ('datalake','gkg')
            ORDER BY database, name FORMAT Vertical" \
    > "$DIAG_DIR/clickhouse-schema.txt"

  # Graph DB tables are prefixed at runtime per config/SCHEMA_VERSION
  # (e.g. v1_gl_project), so discover them dynamically. Engine filter
  # skips materialized-view definitions; their target tables are still
  # captured. Capped per-table to keep dumps manageable on chatty tables.
  CH_DUMP_TABLES=$(ch_query "SELECT database || '.' || name FROM system.tables
                             WHERE database IN ('datalake','gkg')
                               AND engine NOT IN ('View','Null','Dictionary')
                             ORDER BY database, name FORMAT TSV" 2>/dev/null)
  for tbl in $CH_DUMP_TABLES; do
    safe=$(echo "$tbl" | tr '.' '-')
    ch_query "SELECT * FROM $tbl FINAL ORDER BY ALL LIMIT 1000 FORMAT Vertical" \
      > "$DIAG_DIR/clickhouse-${safe}.txt"
  done
fi

# --- NATS jetstream state (skipped if nats-box is absent) ------------------
NATS_BOX=$($KC get pods -n "$NS_NATS" -l app.kubernetes.io/name=nats-box -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -n "$NATS_BOX" ]]; then
  log "Dumping NATS jetstream state"
  $KC exec -n "$NS_NATS" "$NATS_BOX" -- nats stream list --all 2>&1 \
    > "$DIAG_DIR/nats-streams.txt"
  $KC exec -n "$NS_NATS" "$NATS_BOX" -- nats consumer list --all 2>&1 \
    > "$DIAG_DIR/nats-consumers.txt"
  $KC exec -n "$NS_NATS" "$NATS_BOX" -- nats stream info e2e_siphon_event_stream 2>&1 \
    > "$DIAG_DIR/nats-stream-info.txt"
fi

log "Diagnostics complete"

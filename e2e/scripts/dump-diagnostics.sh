#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
set +e

DIAG_DIR="${E2E_DIR}/diagnostics"
mkdir -p "$DIAG_DIR"

TRACE_LOG_TAIL=25

trace_failing_container_logs() {
  local ns="$1" pod="$2" failing c
  failing=$($KC get pod "$pod" -n "$ns" -o jsonpath='{range .status.initContainerStatuses[?(@.restartCount>0)]}{.name}{"\n"}{end}{range .status.containerStatuses[?(@.ready==false)]}{.name}{"\n"}{end}' 2>/dev/null)
  for c in $failing; do
    echo "  --- $pod/$c (last $TRACE_LOG_TAIL log lines) ---"
    $KC logs "$pod" -n "$ns" -c "$c" --tail="$TRACE_LOG_TAIL" --previous 2>/dev/null \
      || $KC logs "$pod" -n "$ns" -c "$c" --tail="$TRACE_LOG_TAIL" 2>/dev/null
  done
}

log "Collecting diagnostics to $DIAG_DIR"

helm list -A --all --kube-context "$KCTX" 2>&1 > "$DIAG_DIR/helm-list.txt"
for ns in "$NS_NATS" "$NS_CH" "$NS_GITLAB" "$NS_SIPHON" "$NS_GKG"; do
  $KC get ns "$ns" >/dev/null 2>&1 || continue
  ns_short="${ns#e2e-${E2E_SHA}-}"
  for rel in $(helm list -n "$ns" --all --kube-context "$KCTX" -q 2>/dev/null); do
    helm status "$rel" -n "$ns" --kube-context "$KCTX" --show-resources 2>&1 \
      > "$DIAG_DIR/helm-${ns_short}-${rel}-status.txt"
  done
done

$KC get events -A --sort-by=.lastTimestamp 2>&1 | tail -n 200 \
  > "$DIAG_DIR/events-cluster.txt"
$KC get nodes -o wide 2>&1 > "$DIAG_DIR/nodes.txt"
$KC describe nodes 2>&1 > "$DIAG_DIR/nodes-describe.txt"
$KC top nodes 2>&1 > "$DIAG_DIR/nodes-top.txt"
$KC get ns -o name 2>/dev/null | grep '^namespace/e2e-' | sort \
  > "$DIAG_DIR/e2e-namespaces.txt"

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

      trace_failing_container_logs "$ns" "$pod"
    fi
  done
  echo ""
done

CH_POD=$($KC get pods -n "$NS_CH" -l app=clickhouse -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
if [[ -n "$CH_POD" ]]; then
  log "Dumping ClickHouse state"
  ch_query() {
    printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "${2:-$CH_POD}" -- \
      sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"' 2>&1
  }

  for db in datalake gkg; do
    ch_query "SELECT name, total_rows, formatReadableSize(total_bytes) AS size, engine
              FROM system.tables WHERE database = '$db'
              ORDER BY name FORMAT PrettyCompact" \
      > "$DIAG_DIR/clickhouse-${db}-tables.txt"
  done

  ch_query "SELECT name, status, type, last_successful_update_time, element_count,
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

  CH_DUMP_TABLES=$(ch_query "SELECT database || '.' || name FROM system.tables
                             WHERE database IN ('datalake','gkg')
                               AND engine NOT IN ('View','Null','Dictionary')
                             ORDER BY database, name FORMAT TSV" 2>/dev/null)
  for tbl in $CH_DUMP_TABLES; do
    safe=$(echo "$tbl" | tr '.' '-')
    ch_query "SELECT * FROM $tbl FINAL ORDER BY ALL LIMIT 1000 FORMAT Vertical" \
      > "$DIAG_DIR/clickhouse-${safe}.txt"
  done

  for pod in $($KC get pods -n "$NS_CH" -l app=clickhouse -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    ch_query "SYSTEM FLUSH LOGS" "$pod" > /dev/null

    ch_query "SELECT event_time_microseconds, event_type, table, part_name, rows, duration_ms, error
              FROM system.part_log
              WHERE database = 'datalake' AND event_type IN ('NewPart', 'DownloadPart')
              ORDER BY event_time_microseconds FORMAT TSVWithNames" "$pod" \
      > "$DIAG_DIR/clickhouse-${pod}-part-log.tsv"

    ch_query "SELECT query_start_time_microseconds, event_time_microseconds, type, query_kind,
                     user, tables, written_rows, exception, query_id
              FROM system.query_log
              WHERE type != 'QueryStart' AND query_kind IN ('Insert', 'AsyncInsertFlush')
                AND has(databases, 'datalake')
              ORDER BY query_start_time_microseconds FORMAT TSVWithNames" "$pod" \
      > "$DIAG_DIR/clickhouse-${pod}-datalake-inserts.tsv"

    ch_query "SELECT query_start_time_microseconds, event_time_microseconds, type, user, tables,
                     extract(query, '_siphon_watermark > ''([^'']+)''') AS window_floor,
                     extract(query, '_siphon_watermark <= ''([^'']+)''') AS window_target,
                     extract(query, 'startsWith[^,]*traversal_path, ''([^'']*)''') AS traversal_path,
                     result_rows, query_id
              FROM system.query_log
              WHERE type != 'QueryStart' AND query_kind = 'Select' AND has(databases, 'datalake')
                AND query LIKE '%_siphon_watermark >%'
              ORDER BY query_start_time_microseconds FORMAT TSVWithNames" "$pod" \
      > "$DIAG_DIR/clickhouse-${pod}-watermark-reads.tsv"
  done
fi

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

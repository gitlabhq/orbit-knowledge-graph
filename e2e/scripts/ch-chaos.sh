#!/usr/bin/env bash
# Kill one ClickHouse replica while the stack is busy, then wait for it to rejoin.
# Usage: ch-chaos.sh backfill|indexing   (no-op unless E2E_CH_REPLICAS > 1)
# A DDL issued while a replica is down blocks for distributed_ddl_task_timeout and
# fails, so the first kill runs after the GitLab migrations and dictionary patches.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASE="${1:?usage: ch-chaos.sh backfill|indexing}"
[[ "${E2E_CH_REPLICAS:-1}" -gt 1 ]] || exit 0

wait_for_marker() {
  local attempts="$1" marker="$2"; shift 2
  for _ in $(seq 1 "$attempts"); do
    if "$@" 2>/dev/null | grep -c "$marker" >/dev/null; then return 0; fi
    sleep 5
  done
  log "chaos: marker '$marker' not seen, refusing to kill a replica"
  return 1
}

case "$PHASE" in
  backfill)
    POD=clickhouse-1
    ;;
  indexing)
    POD=clickhouse-2
    log "chaos: waiting for the robot pool to start"
    wait_for_marker 240 'PASSED Tests.01 Setup And Smoke' $KC logs -n "$NS_GKG" job/e2e-robot-runner --tail=-1
    sleep 60
    ;;
  *) echo "unknown phase: $PHASE"; exit 1 ;;
esac

log "chaos: deleting $POD at $(date -u +%T)"
$KC delete pod -n "$NS_CH" "$POD" --timeout=120s
for _ in $(seq 1 60); do
  $KC get pod -n "$NS_CH" "$POD" >/dev/null 2>&1 && break
  sleep 2
done
$KC wait -n "$NS_CH" --for=condition=Ready "pod/$POD" --timeout=300s
CATCH_UP_SQL="SELECT (SELECT count() FROM system.database_replicas WHERE is_readonly OR is_session_expired OR log_ptr < max_log_ptr) \
  + (SELECT count() FROM system.replicas WHERE absolute_delay > 0 OR queue_size > 0 OR is_readonly OR is_session_expired)"
for _ in $(seq 1 150); do
  lagging=$($KC exec -n "$NS_CH" "$POD" -- sh -c "clickhouse-client --user default --password \"\$CLICKHOUSE_PASSWORD\" --query \"$CATCH_UP_SQL\"" 2>/dev/null || echo unknown)
  [[ "$lagging" == "0" ]] && { log "chaos: $POD rejoined and caught up at $(date -u +%T)"; exit 0; }
  sleep 2
done
log "chaos: $POD did not catch up within 300s (lagging replicas: $lagging)"
exit 1

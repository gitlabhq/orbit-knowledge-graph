#!/usr/bin/env bash
# Kill one ClickHouse replica while the stack is busy, then wait for it to rejoin.
# Usage: ch-chaos.sh migration|indexing   (no-op unless E2E_CH_REPLICAS > 1)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASE="${1:?usage: ch-chaos.sh migration|indexing}"
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
  migration)
    POD=clickhouse-1
    log "chaos: waiting for the dispatcher to start the schema migration"
    wait_for_marker 240 '"creating table"' $KC logs -n "$NS_GKG" deploy/gkg-dispatcher --tail=200
    ;;
  indexing)
    POD=clickhouse-2
    log "chaos: waiting for the robot pool to start"
    wait_for_marker 120 'Setup And Smoke' $KC logs -n "$NS_GKG" job/e2e-robot-runner --tail=50
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
for _ in $(seq 1 150); do
  lagging=$($KC exec -n "$NS_CH" "$POD" -- sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --query "SELECT count() FROM system.replicas WHERE absolute_delay > 0 OR queue_size > 0 OR is_readonly"' 2>/dev/null || echo unknown)
  [[ "$lagging" == "0" ]] && { log "chaos: $POD rejoined and caught up at $(date -u +%T)"; exit 0; }
  sleep 2
done
log "chaos: $POD did not catch up within 300s (lagging replicas: $lagging)"
exit 1

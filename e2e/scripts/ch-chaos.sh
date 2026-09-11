#!/usr/bin/env bash
# Kill one ClickHouse replica while the stack is busy, then wait for it to rejoin.
# Usage: ch-chaos.sh migration|indexing   (no-op unless E2E_CH_REPLICAS > 1)
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASE="${1:?usage: ch-chaos.sh migration|indexing}"
[[ "${E2E_CH_REPLICAS:-1}" -gt 1 ]] || exit 0

case "$PHASE" in
  migration)
    POD=clickhouse-1
    log "chaos: waiting for the dispatcher to start the schema migration"
    for _ in $(seq 1 240); do
      $KC logs -n "$NS_GKG" deploy/gkg-dispatcher --tail=200 2>/dev/null | grep -q '"creating table"' && break
      sleep 5
    done
    ;;
  indexing)
    POD=clickhouse-2
    log "chaos: waiting for the robot pool to start"
    for _ in $(seq 1 120); do
      $KC logs -n "$NS_GKG" job/e2e-robot-runner --tail=50 2>/dev/null | grep -q 'Setup And Smoke' && break
      sleep 5
    done
    sleep 60
    ;;
  *) echo "unknown phase: $PHASE"; exit 1 ;;
esac

log "chaos: deleting $POD at $(date -u +%T)"
$KC delete pod -n "$NS_CH" "$POD" --wait=false
$KC wait -n "$NS_CH" --for=condition=Ready "pod/$POD" --timeout=300s
until [[ "$($KC exec -n "$NS_CH" "$POD" -- sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --query "SELECT count() FROM system.replicas WHERE absolute_delay > 0 OR queue_size > 0 OR is_readonly"' 2>/dev/null)" == "0" ]]; do
  sleep 2
done
log "chaos: $POD rejoined and caught up at $(date -u +%T)"

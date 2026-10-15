#!/usr/bin/env bash
# The siphon producer snapshots every CDC table at start and its readiness
# probe flaps for the whole phase. helm's wait can return in a short ready
# window, and the robot health gate then fails on an unhealthy producer.
# Block until the producer Deployment stays ready for a full window.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

DEPLOYMENT=postgres-producer
STABLE_SECONDS="${E2E_SIPHON_STABLE_SECONDS:-60}"
TIMEOUT_SECONDS="${E2E_SIPHON_READY_TIMEOUT_SECONDS:-900}"
POLL_INTERVAL="${E2E_SIPHON_POLL_SECONDS:-3}"

replicas() {
  $KC get deployment "$DEPLOYMENT" -n "$NS_SIPHON" \
    -o jsonpath='{.status.readyReplicas}/{.spec.replicas}' 2>/dev/null || echo "?/?"
}

log "Waiting for siphon $DEPLOYMENT to stay ready for ${STABLE_SECONDS}s (timeout: ${TIMEOUT_SECONDS}s)"
SECONDS=0
stable=0
status="?/?"
while [ "$SECONDS" -lt "$TIMEOUT_SECONDS" ]; do
  status=$(replicas)
  if [ -n "${status%/*}" ] && [ "${status%/*}" = "${status#*/}" ]; then
    stable=$((stable + POLL_INTERVAL))
  else
    stable=0
  fi
  if [ "$stable" -ge "$STABLE_SECONDS" ]; then
    log "siphon $DEPLOYMENT ready ($status) for ${stable}s after ${SECONDS}s"
    exit 0
  fi
  sleep "$POLL_INTERVAL"
done

log "siphon $DEPLOYMENT did not stay ready for ${STABLE_SECONDS}s within ${TIMEOUT_SECONDS}s (last ready/wanted: $status)"
$KC get deployment "$DEPLOYMENT" -n "$NS_SIPHON" 2>&1 || true
$KC get events -n "$NS_SIPHON" --sort-by=.lastTimestamp 2>/dev/null | tail -20 || true
exit 1

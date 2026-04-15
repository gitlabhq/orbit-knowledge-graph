#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 3: Waiting for infrastructure"
wait_for_pods "$NS_NATS" 120
wait_for_pods "$NS_CH" 180

log "Waiting for GitLab migrations..."
$KC wait --for=condition=complete job -l app=migrations \
  -n "$NS_GITLAB" --timeout=900s 2>/dev/null || {
  log "ERROR: GitLab migrations did not complete within 900s"
  $KC get jobs -n "$NS_GITLAB"
  exit 1
}
wait_for_pods "$NS_GITLAB" 600

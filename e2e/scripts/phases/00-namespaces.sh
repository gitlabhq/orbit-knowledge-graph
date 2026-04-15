#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 0: Creating namespaces"
for ns in "$NS_SECRETS" "$NS_GITLAB" "$NS_GKG"; do
  $KC create namespace "$ns" 2>/dev/null || true
done

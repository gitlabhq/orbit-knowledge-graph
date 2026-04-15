#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 6: Deploying Siphon and GKG"
cd "$E2E_DIR"
helmfile --file helmfile.yaml.gotmpl -l phase=pipeline sync

wait_for_pods "$NS_SIPHON" 180
wait_for_pods "$NS_GKG" 180

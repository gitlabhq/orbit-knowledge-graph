#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

PHASES_DIR="$E2E_DIR/scripts/phases"

log "E2E Setup (SHA: $E2E_SHA)"

source "$PHASES_DIR/00-namespaces.sh"
source "$PHASES_DIR/01-secrets.sh"
source "$PHASES_DIR/02-infra.sh"
source "$PHASES_DIR/03-wait-infra.sh"
source "$PHASES_DIR/04-pg-siphon.sh"
source "$PHASES_DIR/05-ch-schema.sh"
source "$PHASES_DIR/06-pipeline.sh"
source "$PHASES_DIR/07-seed-toolbox.sh"

log "Setup complete (SHA: $E2E_SHA)"
log "Namespaces: $NS_NATS $NS_CH $NS_GITLAB $NS_SIPHON $NS_GKG"
log "Run: E2E_SHA=$E2E_SHA scripts/test.sh"

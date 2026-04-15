#!/usr/bin/env bash
source "$(dirname "${BASH_SOURCE[0]}")/../lib.sh"

log "Phase 2: Deploying infrastructure (NATS, ClickHouse, GitLab)"
cd "$E2E_DIR"
helmfile --file helmfile.yaml.gotmpl -l phase=infra sync

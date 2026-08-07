#!/usr/bin/env bash
# Renders tier-specific values overlay for the GKG helm release.
# Output goes to stdout; caller redirects to a file.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

cat <<EOF
metrics:
  prometheus:
    enabled: true

indexer:
  replicas: $(tier ".gkg.indexer.replicas")
  resources:
    requests:
      cpu: $(tier ".gkg.indexer.requests.cpu")
      memory: $(tier ".gkg.indexer.requests.memory")
    limits:
      memory: $(tier ".gkg.indexer.limits.memory")

webserver:
  replicas: $(tier ".gkg.webserver.replicas")
  resources:
    requests:
      cpu: $(tier ".gkg.webserver.requests.cpu")
      memory: $(tier ".gkg.webserver.requests.memory")
    limits:
      memory: $(tier ".gkg.webserver.limits.memory")

dispatcher:
  replicas: $(tier ".gkg.dispatcher.replicas")
  resources:
    requests:
      cpu: $(tier ".gkg.dispatcher.requests.cpu")
      memory: $(tier ".gkg.dispatcher.requests.memory")
    limits:
      memory: $(tier ".gkg.dispatcher.limits.memory")

engine:
  max_concurrent_workers: $(tier ".gkg.concurrency.max_concurrent_workers")
  concurrency_groups:
    sdlc: $(tier ".gkg.concurrency.sdlc")
    code: $(tier ".gkg.concurrency.code")

global:
  nodeSelector:
    ra-run: "${RUN_ID}"
  tolerations:
    - key: ra-run
      operator: Equal
      value: "${RUN_ID}"
      effect: NoSchedule
EOF

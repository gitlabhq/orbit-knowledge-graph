#!/usr/bin/env bash
# Create an ephemeral GKE cluster for an RA bench run.
# The cluster is self-contained: cert-manager, root CA, and ClusterIssuer
# are bootstrapped so setup.sh works without any pre-existing infra.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CLUSTER_NAME="ra-bench-${RUN_ID}"
: "${PROJECT:=gl-knowledgegraph-prj-f2eec59d}"
: "${ZONE:=us-central1-a}"
MACHINE=$(tier ".nodes.machine")
COUNT=$(tier ".nodes.count")

log "Creating ephemeral cluster ${CLUSTER_NAME} (${MACHINE} x${COUNT})"

gcloud container clusters create "${CLUSTER_NAME}" \
  --project "${PROJECT}" \
  --zone "${ZONE}" \
  --machine-type "${MACHINE}" \
  --num-nodes "${COUNT}" \
  --disk-size 100 \
  --disk-type pd-balanced \
  --no-enable-autoupgrade \
  --labels "ttl-hours=12,run-id=${RUN_ID},purpose=ra-bench" \
  --quiet

# Point kubectl at the new cluster.
KCTX="gke_${PROJECT}_${ZONE}_${CLUSTER_NAME}"
export KCTX
gcloud container clusters get-credentials "${CLUSTER_NAME}" \
  --project "${PROJECT}" --zone "${ZONE}"

KC="kubectl --context=${KCTX}"
export KC

log "Installing cert-manager"
${KC} apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.17.2/cert-manager.yaml
${KC} wait -n cert-manager deploy/cert-manager-webhook \
  --for=condition=available --timeout=120s

log "Creating self-signed root CA and ClusterIssuer"
${KC} apply -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: root-ca
  namespace: cert-manager
spec:
  isCA: true
  commonName: e2e-root-ca
  secretName: root-ca-secret
  duration: 87600h
  issuerRef:
    name: selfsigned
    kind: ClusterIssuer
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: e2e-ca
spec:
  ca:
    secretName: root-ca-secret
EOF

log "Waiting for root CA to be ready"
${KC} wait -n cert-manager certificate/root-ca \
  --for=condition=ready --timeout=60s

log "Cluster ${CLUSTER_NAME} ready"
log "KCTX=${KCTX}"

# Write cluster context for downstream scripts.
echo "${KCTX}" > "${BENCH_DIR}/results/${RUN_ID}/kctx"

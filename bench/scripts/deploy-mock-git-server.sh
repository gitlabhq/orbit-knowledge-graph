#!/usr/bin/env bash
# Build, push, and deploy the mock git server for code indexing benchmarks.
# Usage: KCTX=... RUN_ID=bench7 bash bench/scripts/deploy-mock-git-server.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"
: "${MOCK_GIT_BUCKET:=gkg-code-corpus}"
: "${MOCK_GIT_REGISTRY:=gcr.io/gl-knowledgegraph-prj-f2eec59d}"
IMAGE="${MOCK_GIT_REGISTRY}/mock-git-server:latest"

# --- 1. Build and push ---
log "Building mock-git-server image"
docker build -t "${IMAGE}" "${BENCH_DIR}/mock-git-server"
docker push "${IMAGE}"

# --- 2. Create service account for GCS FUSE ---
$KC create sa mock-git-server-sa -n "${CH_NS}" 2>/dev/null || true
$KC annotate sa mock-git-server-sa -n "${CH_NS}" \
  "iam.gke.io/gcp-service-account=1079327125344-compute@developer.gserviceaccount.com" \
  --overwrite 2>/dev/null

# --- 3. Deploy ---
log "Deploying mock-git-server in ${CH_NS}"
export CH_NAMESPACE="${CH_NS}"
export MOCK_GIT_SERVER_IMAGE="${IMAGE}"
export MOCK_GIT_BUCKET
envsubst '${CH_NAMESPACE} ${MOCK_GIT_SERVER_IMAGE} ${MOCK_GIT_BUCKET}' \
  < "${BENCH_DIR}/manifests/mock-git-server.yaml" | $KC apply -f -

$KC rollout status -n "${CH_NS}" deploy/mock-git-server --timeout=120s
log "Mock git server ready"

# --- 4. Upgrade GKG chart to point at the mock ---
GKG_NS="e2e-${RUN_ID}-gkg"
MOCK_URL="http://mock-git-server.${CH_NS}.svc.cluster.local:8090"
log "Upgrading GKG release: gitlab.baseUrl=${MOCK_URL}"

HELM_ARGS=(upgrade gkg
  oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg
  --namespace "${GKG_NS}"
  --reuse-values
  --set "gitlab.baseUrl=${MOCK_URL}"
  --kube-context "${KCTX}")

helm "${HELM_ARGS[@]}"

$KC rollout restart -n "${GKG_NS}" deploy/gkg-indexer-default
$KC rollout status -n "${GKG_NS}" deploy/gkg-indexer-default --timeout=120s
log "Indexer restarted with mock git server"

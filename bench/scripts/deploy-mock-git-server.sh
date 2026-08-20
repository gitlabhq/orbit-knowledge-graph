#!/usr/bin/env bash
# Build, push, and deploy the mock git server for code indexing benchmarks.
# Also upgrades the GKG helm release to point at the mock and resets code
# indexing checkpoints so the indexer re-indexes all projects.
# Usage: KCTX=... RUN_ID=bench7 bash bench/scripts/deploy-mock-git-server.sh
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"
GKG_NS="e2e-${RUN_ID}-gkg"
: "${MOCK_GIT_BUCKET:=gkg-code-corpus}"
: "${MOCK_GIT_REGISTRY:=gcr.io/gl-knowledgegraph-prj-f2eec59d}"
IMAGE="${MOCK_GIT_REGISTRY}/mock-git-server:latest"

# --- 1. Build and push ---
log "Building mock-git-server image (linux/amd64)"
docker buildx build --platform linux/amd64 -t "${IMAGE}" --push "${BENCH_DIR}/mock-git-server"

# --- 2. Create service account + IAM binding for GCS FUSE ---
$KC create sa mock-git-server-sa -n "${CH_NS}" 2>/dev/null || true
$KC annotate sa mock-git-server-sa -n "${CH_NS}" \
  "iam.gke.io/gcp-service-account=1079327125344-compute@developer.gserviceaccount.com" \
  --overwrite 2>/dev/null
IFS=_ read -r _ GKE_PROJECT _ _ <<< "${KCTX}"
gcloud iam service-accounts add-iam-policy-binding \
  1079327125344-compute@developer.gserviceaccount.com \
  --project="${GKE_PROJECT}" \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:${GKE_PROJECT}.svc.id.goog[${CH_NS}/mock-git-server-sa]" \
  --quiet 2>/dev/null || true

# --- 3. Deploy mock server pod ---
log "Deploying mock-git-server in ${CH_NS}"
export CH_NAMESPACE="${CH_NS}"
export MOCK_GIT_SERVER_IMAGE="${IMAGE}"
export MOCK_GIT_BUCKET
envsubst '${CH_NAMESPACE} ${MOCK_GIT_SERVER_IMAGE} ${MOCK_GIT_BUCKET}' \
  < "${BENCH_DIR}/manifests/mock-git-server.yaml" | $KC apply -f -

$KC rollout status -n "${CH_NS}" deploy/mock-git-server --timeout=120s
log "Mock git server ready"

# --- 4. Upgrade GKG chart to point at the mock ---
MOCK_URL="http://mock-git-server.${CH_NS}.svc.cluster.local:8090"
log "Upgrading GKG release: gitlab.baseUrl=${MOCK_URL}"

# Pin to the installed chart version to avoid schema mismatches.
INSTALLED_VERSION=$($KC get secret -n "${GKG_NS}" -l owner=helm,name=gkg \
  -o jsonpath='{.items[0].metadata.labels.version}' 2>/dev/null || echo "")
if [[ -z "${INSTALLED_VERSION}" ]]; then
  # Fallback: parse from helm list
  INSTALLED_VERSION=$(helm list --namespace "${GKG_NS}" --kube-context "${KCTX}" \
    -o json 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)[0]['chart'].split('-')[-1])" 2>/dev/null || echo "")
fi

HELM_ARGS=(upgrade gkg
  oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg
  --namespace "${GKG_NS}"
  --reuse-values
  --set "gitlab.baseUrl=${MOCK_URL}"
  --kube-context "${KCTX}")

if [[ -n "${INSTALLED_VERSION}" ]]; then
  log "Pinning to installed chart version ${INSTALLED_VERSION}"
  HELM_ARGS+=(--version "${INSTALLED_VERSION}")
fi

helm "${HELM_ARGS[@]}"

# --- 5. Reset code indexing checkpoints ---
log "Resetting code indexing checkpoints"
CKPT_TABLE=$($KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q \
  "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%code_indexing_checkpoint' LIMIT 1 FORMAT TSV" 2>/dev/null)
if [[ -n "${CKPT_TABLE}" ]]; then
  $KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q \
    "TRUNCATE TABLE gkg.${CKPT_TABLE}" 2>/dev/null || true
fi

# --- 6. Restart indexer ---
$KC rollout restart -n "${GKG_NS}" deploy/gkg-indexer-default
$KC rollout status -n "${GKG_NS}" deploy/gkg-indexer-default --timeout=120s
log "Indexer restarted with mock git server"

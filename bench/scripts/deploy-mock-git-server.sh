#!/usr/bin/env bash
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${BENCH_DIR}/scripts/lib.sh"

CH_NS="${E2E_CH_NAMESPACE:-ra-ch-${RUN_ID}}"
GKG_NS="e2e-${RUN_ID}-gkg"
: "${MOCK_GIT_BUCKET:=$(cd "${TF_DIR}" && "$TF" output -raw code_corpus_bucket 2>/dev/null || bench '.buckets.code_corpus')}"
IMAGE="$(bench '.images.mock_git_server')"

log "Building mock-git-server image (linux/amd64)"
docker buildx build --platform linux/amd64 -t "${IMAGE}" --push "${BENCH_DIR}/mock-git-server"

GCP_SA=$(cd "${TF_DIR}" && "$TF" output -raw node_sa_email 2>/dev/null)
PROJECT=$(cd "${TF_DIR}" && "$TF" output -raw project 2>/dev/null)
$KC create sa mock-git-server-sa -n "${CH_NS}" 2>/dev/null || true
$KC annotate sa mock-git-server-sa -n "${CH_NS}" \
  "iam.gke.io/gcp-service-account=${GCP_SA}" \
  --overwrite 2>/dev/null
gcloud iam service-accounts add-iam-policy-binding \
  "${GCP_SA}" \
  --project="${PROJECT}" \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:${PROJECT}.svc.id.goog[${CH_NS}/mock-git-server-sa]" \
  --quiet 2>/dev/null || true

log "Deploying mock-git-server in ${CH_NS}"
export CH_NAMESPACE="${CH_NS}"
export MOCK_GIT_SERVER_IMAGE="${IMAGE}"
export MOCK_GIT_BUCKET
envsubst '${CH_NAMESPACE} ${MOCK_GIT_SERVER_IMAGE} ${MOCK_GIT_BUCKET}' \
  < "${BENCH_DIR}/manifests/mock-git-server.yaml" | $KC apply -f -

$KC rollout status -n "${CH_NS}" deploy/mock-git-server --timeout=120s
log "Mock git server ready"

MOCK_URL="http://mock-git-server.${CH_NS}.svc.cluster.local:8090"
log "Upgrading GKG release: gitlab.baseUrl=${MOCK_URL}"

INSTALLED_VERSION=$(helm list --namespace "${GKG_NS}" --kube-context "${KCTX}" \
  -f orbit -o json 2>/dev/null \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['chart'].rsplit('-',1)[-1]) if d else print('')" 2>/dev/null || echo "")

HELM_ARGS=(upgrade orbit
  oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/orbit
  --namespace "${GKG_NS}"
  --reuse-values
  --set "gitlab.baseUrl=${MOCK_URL}"
  --kube-context "${KCTX}")

if [[ -n "${INSTALLED_VERSION}" ]]; then
  log "Pinning to installed chart version ${INSTALLED_VERSION}"
  HELM_ARGS+=(--version "${INSTALLED_VERSION}")
fi

helm "${HELM_ARGS[@]}"

log "Resetting code indexing checkpoints"
CKPT_TABLE=$($KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q \
  "SELECT name FROM system.tables WHERE database='gkg' AND name LIKE '%code_indexing_checkpoint' LIMIT 1 FORMAT TSV" 2>/dev/null)
if [[ -n "${CKPT_TABLE}" ]]; then
  $KC exec -n "${CH_NS}" clickhouse-0 -- clickhouse-client -q \
    "TRUNCATE TABLE gkg.${CKPT_TABLE}" 2>/dev/null || true
fi

$KC rollout restart -n "${GKG_NS}" deploy/orbit-indexer-default
$KC rollout status -n "${GKG_NS}" deploy/orbit-indexer-default --timeout=120s
log "Indexer restarted with mock git server"

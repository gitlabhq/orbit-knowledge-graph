#!/usr/bin/env bash
# Lifecycle wrapper for bench Terraform. Reads all config from bench.yaml.
#
# Reads tier, state bucket, and all config from bench.yaml.
# Override tier with TIER env var or -var tier=... passthrough.
#
# Usage:
#   bench/scripts/infra.sh init
#   bench/scripts/infra.sh apply                          # tier from bench.yaml
#   bench/scripts/infra.sh apply -var cluster_name=custom
#   bench/scripts/infra.sh destroy
#   bench/scripts/infra.sh output cluster_name
#   bench/scripts/infra.sh plan
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"
TF_DIR="${BENCH_DIR}/infra"

bench() { yq eval "${1}" "${BENCH_DIR}/config/bench.yaml"; }

TF=$(mise which terraform 2>/dev/null \
  || command -v terraform 2>/dev/null \
  || echo "$(mise where terraform 2>/dev/null)/terraform")
if [[ ! -x "${TF}" ]]; then
  echo "ERROR: terraform not found. Install it or run 'mise install terraform'." >&2
  exit 1
fi

STATE_BUCKET=$(bench '.buckets.tf_state')
: "${TIER:=$(bench '.tier')}"

fetch_credentials() {
  echo "[infra] Fetching kubectl credentials"
  gcloud container clusters get-credentials \
    "$("$TF" -chdir="${TF_DIR}" output -raw cluster_name)" \
    --zone "$(bench '.zone')" \
    --project "$(bench '.project')" \
    2>/dev/null
}

ensure_root_ca() {
  local ctx
  ctx=$("$TF" -chdir="${TF_DIR}" output -raw kctx 2>/dev/null)
  if kubectl --context "${ctx}" -n cert-manager get secret root-ca-secret >/dev/null 2>&1; then
    echo "[infra] root CA already exists"
    return
  fi
  echo "[infra] Creating self-signed root CA"
  kubectl --context "${ctx}" apply -f - <<'YAML'
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: selfsigned-issuer
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
    name: selfsigned-issuer
    kind: ClusterIssuer
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: e2e-ca
spec:
  ca:
    secretName: root-ca-secret
YAML
  kubectl --context "${ctx}" -n cert-manager wait certificate/root-ca \
    --for=condition=Ready --timeout=60s
}

case "${1:-}" in
  init)
    shift
    if ! gcloud storage buckets describe "gs://${STATE_BUCKET}" --project="$(bench '.project')" >/dev/null 2>&1; then
      echo "ERROR: State bucket gs://${STATE_BUCKET} does not exist." >&2
      echo "       Run 'terraform apply' in bench/infra/bootstrap/ first." >&2
      exit 1
    fi
    "$TF" -chdir="${TF_DIR}" init \
      -backend-config="bucket=${STATE_BUCKET}" \
      -backend-config="prefix=bench" \
      "$@"
    ;;
  apply)
    shift
    # Two-phase apply: the kubernetes/helm providers need the cluster to
    # exist before they can plan bootstrap resources (cert-manager, CRDs).
    # Phase 1 targets the node pool, which pulls in all GCP dependencies.
    "$TF" -chdir="${TF_DIR}" apply -var "tier=${TIER}" \
      -target=google_container_node_pool.workload "$@"
    fetch_credentials
    # Phase 2 applies everything including bootstrap helm releases.
    "$TF" -chdir="${TF_DIR}" apply -var "tier=${TIER}" "$@"
    # Phase 3: cert-manager CRs (need CRDs from phase 2).
    ensure_root_ca
    ;;
  plan)
    shift
    "$TF" -chdir="${TF_DIR}" plan -var "tier=${TIER}" "$@"
    ;;
  destroy)
    shift
    "$TF" -chdir="${TF_DIR}" destroy "$@"
    ;;
  output)
    shift
    "$TF" -chdir="${TF_DIR}" output "$@"
    ;;
  reload)
    # Wipe the graph DB (keep datalake), re-apply grants, reset checkpoints,
    # and restart GKG. The datalake and its imported data are untouched.
    shift
    : "${RUN_ID:?RUN_ID is required for reload}"
    CH_NS="ra-ch-${RUN_ID}"
    GKG_NS="e2e-${RUN_ID}-gkg"
    KCTX=$("$TF" -chdir="${TF_DIR}" output -raw kctx 2>/dev/null)

    echo "[infra] Dropping and recreating graph DB (datalake untouched)"
    CH_PASS=$(kubectl --context="${KCTX}" get secret ra-ch-credentials -n "${CH_NS}" \
      -o jsonpath='{.data.default-password}' | base64 -d)
    kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
      clickhouse-client --password "${CH_PASS}" --multiquery \
      --query "DROP DATABASE IF EXISTS gkg; CREATE DATABASE gkg;"

    echo "[infra] Restarting GKG (schema migration re-creates tables on boot)"
    kubectl --context="${KCTX}" rollout restart -n "${GKG_NS}" \
      deploy/gkg-dispatcher deploy/gkg-indexer-default deploy/gkg-webserver
    kubectl --context="${KCTX}" rollout status -n "${GKG_NS}" \
      deploy/gkg-dispatcher --timeout=120s
    echo "[infra] Reload complete. GKG will re-create schema and re-index from the datalake."
    ;;
  deploy)
    # Build a GKG image from the local working tree, push it, and upgrade
    # the Helm release. Pass --reload to also wipe the graph DB.
    shift
    : "${RUN_ID:?RUN_ID is required for deploy}"
    RELOAD=false
    if [[ "${1:-}" == "--reload" ]]; then
      RELOAD=true
      shift
    fi

    KCTX=$("$TF" -chdir="${TF_DIR}" output -raw kctx 2>/dev/null)
    GKG_NS="e2e-${RUN_ID}-gkg"
    SHORT_SHA=$(git -C "${REPO_ROOT}" rev-parse --short HEAD)
    DIRTY=$(git -C "${REPO_ROOT}" diff --quiet HEAD -- 2>/dev/null && echo "" || echo "-dirty")
    IMAGE="registry.gitlab.com/gitlab-org/orbit/knowledge-graph/gkg"
    TAG="bench-${SHORT_SHA}${DIRTY}"

    echo "[infra] Building GKG image (debug, linux/amd64) tag=${TAG}"
    (
      cd "${REPO_ROOT}"
      trap 'rm -f gkg-server' EXIT
      CARGO_PROFILE_DEV_DEBUG=0 cargo build -p orbit-server --locked
      cp target/debug/gkg-server .
      docker buildx build --platform linux/amd64 \
        -t "${IMAGE}:${TAG}" -f e2e/Dockerfile.e2e --push .
    )

    echo "[infra] Upgrading GKG Helm release to ${TAG}"
    kubectl --context="${KCTX}" -n "${GKG_NS}" delete job \
      -l app.kubernetes.io/component=clickhouse-setup --ignore-not-found 2>/dev/null
    INSTALLED_VERSION=$(helm list --namespace "${GKG_NS}" --kube-context "${KCTX}" \
      -f gkg -o json 2>/dev/null \
      | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['chart'].rsplit('-',1)[-1]) if d else print('')" 2>/dev/null || echo "")

    HELM_ARGS=(upgrade gkg
      oci://registry.gitlab.com/gitlab-org/orbit/orbit-helm-charts/gkg
      --namespace "${GKG_NS}"
      --reuse-values
      --set "image.repository=${IMAGE}"
      --set "image.tag=${TAG}"
      --set "image.pullPolicy=Always"
      --no-hooks
      --kube-context "${KCTX}")
    [[ -n "${INSTALLED_VERSION}" ]] && HELM_ARGS+=(--version "${INSTALLED_VERSION}")
    helm "${HELM_ARGS[@]}"

    if [[ "${RELOAD}" == "true" ]]; then
      RUN_ID="${RUN_ID}" "$0" reload
    else
      kubectl --context="${KCTX}" rollout restart -n "${GKG_NS}" \
        deploy/gkg-dispatcher deploy/gkg-indexer-default deploy/gkg-webserver
      kubectl --context="${KCTX}" rollout status -n "${GKG_NS}" \
        deploy/gkg-dispatcher --timeout=120s
    fi
    echo "[infra] Deploy complete. Running image: ${IMAGE}:${TAG}"
    ;;
  *)
    echo "Usage: $0 init | apply | plan | destroy | reload | deploy [--reload] | output [args...]" >&2
    exit 1
    ;;
esac

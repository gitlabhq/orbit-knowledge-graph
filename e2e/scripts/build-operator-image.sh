#!/usr/bin/env bash
# Build a custom gitlab-operator image with the Orbit CRD spike and vendored
# gkg + gitlab-deps charts. Sourced by the sm-e2e-spike CI job.
#
# Exports E2E_OPERATOR_IMAGE and E2E_OPERATOR_TAG for the helmfile to consume.
# The image is pushed to the KG project's container registry so no cross-project
# token is needed.
#
# Temporary: this script exists for spike testing only. Once the Orbit CRD
# lands in the operator repo proper, this script and the sm-e2e-spike CI job
# should be removed.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

OPERATOR_BRANCH="${E2E_OPERATOR_BRANCH:-michaelusa/spike-orbit-crd}"
OPERATOR_REPO="${E2E_OPERATOR_REPO:-https://gitlab.com/gitlab-org/cloud-native/gitlab-operator.git}"

export E2E_OPERATOR_IMAGE="${CI_REGISTRY_IMAGE}/operator-spike"
export E2E_OPERATOR_TAG="${CI_COMMIT_SHORT_SHA}"

# Reuse on retry.
if docker manifest inspect "${E2E_OPERATOR_IMAGE}:${E2E_OPERATOR_TAG}" >/dev/null 2>&1; then
  log "Reusing existing operator image ${E2E_OPERATOR_IMAGE}:${E2E_OPERATOR_TAG}"
  return 0 2>/dev/null || exit 0
fi

WORKDIR=$(mktemp -d)
trap 'rm -rf "$WORKDIR"' EXIT

log "Cloning operator branch ${OPERATOR_BRANCH}"
git clone --depth 1 --branch "${OPERATOR_BRANCH}" "${OPERATOR_REPO}" "${WORKDIR}/operator"

cd "${WORKDIR}/operator"

# The operator Dockerfile expects charts in ./charts/. The retrieve scripts
# download them there. For the spike we need only the orbit charts since the
# operator image also needs the GitLab chart (it renders it for the GitLab CR).
# However, we are only testing the Orbit reconciler here, so we vendor just the
# orbit charts and a single GitLab chart version for the catalog to load.

log "Retrieving GitLab charts"
mkdir -p charts bin
# Use the helm binary from CI tools if available, otherwise fall back to PATH.
if command -v helm >/dev/null 2>&1; then
  ln -sf "$(command -v helm)" bin/helm
else
  curl -fsSL https://get.helm.sh/helm-v4.2.2-linux-amd64.tar.gz | tar xz -C bin --strip-components=1 linux-amd64/helm
fi

bash scripts/retrieve_gitlab_charts.sh
bash scripts/retrieve_orbit_charts.sh

log "Building operator image"
# The operator Dockerfile uses FROM --platform=${BUILDPLATFORM} which requires
# the buildx plugin. The e2e CI runner has legacy Docker without buildx.
# For the single-arch e2e build, strip the --platform flag and hardcode the
# TARGETOS/TARGETARCH ARGs. The result is identical to the real image on amd64.
sed 's/--platform=${BUILDPLATFORM} //' Dockerfile > Dockerfile.e2e
docker build \
  -t "${E2E_OPERATOR_IMAGE}:${E2E_OPERATOR_TAG}" \
  -f Dockerfile.e2e \
  --build-arg TARGETOS=linux \
  --build-arg TARGETARCH=amd64 \
  .

log "Pushing operator image"
docker push "${E2E_OPERATOR_IMAGE}:${E2E_OPERATOR_TAG}"

log "Operator image ready: ${E2E_OPERATOR_IMAGE}:${E2E_OPERATOR_TAG}"

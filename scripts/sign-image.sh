#!/bin/sh

# Signs a pushed image digest with keyless cosign and verifies the result.
# Runs only in the canonical project. Do not sign in forks, including private
# forks: a keyless signature publishes the signing project's path and ref to
# the public Rekor log, which would disclose them.

set -eu

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <image:tag>" >&2
  exit 1
fi

IMAGE_REF="$1"

if [ "${CI_PROJECT_ID:-}" != "${CANONICAL_PROJECT_ID:-}" ]; then
  echo "Skipping image signing: project ${CI_PROJECT_ID:-unset} is not the canonical project."
  exit 0
fi

: "${COSIGN_VERSION:?COSIGN_VERSION must be set}"
: "${COSIGN_SHA256_AMD64:?COSIGN_SHA256_AMD64 must be set}"
: "${SIGSTORE_ID_TOKEN:?SIGSTORE_ID_TOKEN must be set; the job needs an id_tokens entry with aud sigstore}"

export COSIGN_YES=true

install_cosign() {
  arch=$(uname -m)
  if [ "$arch" != "x86_64" ]; then
    echo "Only the linux-amd64 cosign checksum is pinned; this runner is $arch." >&2
    exit 1
  fi
  command -v curl >/dev/null 2>&1 || apk add --no-cache curl >/dev/null
  curl -fsSL -o /usr/local/bin/cosign \
    "https://github.com/sigstore/cosign/releases/download/${COSIGN_VERSION}/cosign-linux-amd64"
  echo "${COSIGN_SHA256_AMD64}  /usr/local/bin/cosign" | sha256sum -c -
  chmod +x /usr/local/bin/cosign
}

command -v cosign >/dev/null 2>&1 || install_cosign
cosign version

IMAGE_NAME="${IMAGE_REF%:*}"
IMAGE_TAG="${IMAGE_REF##*:}"
DIGEST=$(docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "${IMAGE_REF}")
DIGEST_REF="${IMAGE_NAME}@${DIGEST}"

if [ -n "${CI_COMMIT_TAG:-}" ]; then
  REF_PATH="refs/tags/${CI_COMMIT_TAG}"
else
  REF_PATH="refs/heads/${CI_COMMIT_REF_NAME}"
fi
CERTIFICATE_IDENTITY="${CI_SERVER_URL}/${CI_PROJECT_PATH}//${CI_CONFIG_PATH}@${REF_PATH}"

echo "Signing ${DIGEST_REF} (pushed as ${IMAGE_TAG})"
cosign sign --recursive \
  --annotations "com.gitlab/ci-pipeline-url=${CI_PIPELINE_URL}" \
  --annotations "com.gitlab/ci-job-url=${CI_JOB_URL}" \
  --annotations "com.gitlab/commit-sha=${CI_COMMIT_SHA}" \
  --annotations "com.gitlab/tag=${IMAGE_TAG}" \
  "${DIGEST_REF}"

echo "Verifying ${DIGEST_REF} against identity ${CERTIFICATE_IDENTITY}"
cosign verify \
  --certificate-identity "${CERTIFICATE_IDENTITY}" \
  --certificate-oidc-issuer "${CI_SERVER_URL}" \
  "${DIGEST_REF}"

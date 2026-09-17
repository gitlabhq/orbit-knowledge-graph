#!/bin/sh

# Signs image digests with keyless cosign and verifies each result.
# Runs only in the canonical project. Do not sign in forks, including private
# forks: a keyless signature publishes the signing project's path and ref to
# the public Rekor log, which would disclose them.
# The project ID is read from the signed OIDC token and compared with a
# literal, because CI variables can be overridden from project or pipeline
# settings.

set -eu

CANONICAL_PROJECT_ID=77960826
COSIGN_VERSION=v3.1.3
COSIGN_SHA256_AMD64=4629c757b7618056f8ddd7e2625ae9fdd94c0372a65049520bc7d9df9efc7f71

if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <image[:tag]@sha256:digest>..." >&2
  exit 1
fi

for ref in "$@"; do
  if ! printf '%s' "$ref" | grep -Eq '@sha256:[0-9a-f]{64}$'; then
    echo "Refusing to sign ${ref}: pass a full digest, a tag can be moved after the build." >&2
    exit 1
  fi
done

: "${SIGSTORE_ID_TOKEN:?SIGSTORE_ID_TOKEN must be set; the job needs an id_tokens entry with aud sigstore}"

token_project_id() {
  payload=$(printf '%s' "$SIGSTORE_ID_TOKEN" | cut -d. -f2 | tr '_-' '/+')
  case $(( ${#payload} % 4 )) in
    2) payload="${payload}==" ;;
    3) payload="${payload}=" ;;
  esac
  printf '%s' "$payload" | base64 -d 2>/dev/null | grep -oE '"project_id": *"[0-9]+"' | grep -oE '[0-9]+' | head -n1
}

project_id=$(token_project_id)
if [ -z "$project_id" ]; then
  echo "SIGSTORE_ID_TOKEN carries no project_id claim." >&2
  exit 1
fi
if [ "$project_id" != "$CANONICAL_PROJECT_ID" ]; then
  echo "Skipping image signing: project ${project_id} is not the canonical project."
  exit 0
fi

export COSIGN_YES=true

arch=$(uname -m)
if [ "$arch" != "x86_64" ]; then
  echo "Only the linux-amd64 cosign checksum is pinned; this runner is ${arch}." >&2
  exit 1
fi

COSIGN="$(mktemp -d)/cosign"
wget -qO "$COSIGN" \
  "https://github.com/sigstore/cosign/releases/download/${COSIGN_VERSION}/cosign-linux-amd64"
echo "${COSIGN_SHA256_AMD64}  ${COSIGN}" | sha256sum -c -
chmod +x "$COSIGN"
"$COSIGN" version

if [ -n "${CI_COMMIT_TAG:-}" ]; then
  REF_PATH="refs/tags/${CI_COMMIT_TAG}"
else
  REF_PATH="refs/heads/${CI_COMMIT_REF_NAME}"
fi
CERTIFICATE_IDENTITY="${CI_SERVER_URL}/${CI_PROJECT_PATH}//${CI_CONFIG_PATH}@${REF_PATH}"

sign_and_verify() {
  ref="$1"
  digest="${ref##*@}"
  named="${ref%@*}"
  image_tag="${named##*:}"
  case "$image_tag" in
    */*|"$named") image_name="$named"; image_tag="" ;;
    *) image_name="${named%:*}" ;;
  esac
  digest_ref="${image_name}@${digest}"

  echo "Signing ${digest_ref}${image_tag:+ (published as ${image_tag})}"
  "$COSIGN" sign \
    --annotations "com.gitlab/ci-pipeline-url=${CI_PIPELINE_URL}" \
    --annotations "com.gitlab/ci-job-url=${CI_JOB_URL}" \
    --annotations "com.gitlab/commit-sha=${CI_COMMIT_SHA}" \
    --annotations "com.gitlab/tag=${image_tag}" \
    "${digest_ref}"

  echo "Verifying ${digest_ref} against identity ${CERTIFICATE_IDENTITY}"
  "$COSIGN" verify \
    --certificate-identity "${CERTIFICATE_IDENTITY}" \
    --certificate-oidc-issuer "${CI_SERVER_URL}" \
    "${digest_ref}"
}

for ref in "$@"; do
  sign_and_verify "$ref"
done

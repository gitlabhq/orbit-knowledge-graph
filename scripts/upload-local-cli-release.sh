#!/usr/bin/env bash
set -euo pipefail

# Upload local CLI binary tarballs (and matching .sha256 files) to the
# project's Generic Package Registry, then attach them as asset links on the
# GitLab Release that semantic-release created on `main` for $CI_COMMIT_TAG.
#
# Required env (provided by GitLab CI on tag pipelines):
#   CI_COMMIT_TAG, CI_API_V4_URL, CI_PROJECT_ID, CI_JOB_TOKEN

: "${CI_COMMIT_TAG:?CI_COMMIT_TAG is required}"
: "${CI_API_V4_URL:?CI_API_V4_URL is required}"
: "${CI_PROJECT_ID:?CI_PROJECT_ID is required}"
: "${CI_JOB_TOKEN:?CI_JOB_TOKEN is required}"

VERSION="${CI_COMMIT_TAG#v}"
PACKAGE_NAME="orbit"
ARTIFACTS=(
  "orbit-linux-x86_64.tar.gz"
  "orbit-linux-aarch64.tar.gz"
  "orbit-darwin-x86_64.tar.gz"
  "orbit-darwin-aarch64.tar.gz"
)

for artifact in "${ARTIFACTS[@]}"; do
  if [ ! -f "$artifact" ]; then
    echo "missing build artifact: $artifact" >&2
    exit 1
  fi
done

echo "Generating sha256 checksums..."
for artifact in "${ARTIFACTS[@]}"; do
  sha256sum "$artifact" > "${artifact}.sha256"
  cat "${artifact}.sha256"
done

package_url() {
  echo "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/packages/generic/${PACKAGE_NAME}/${VERSION}/$1"
}

upload_file() {
  local file="$1"
  local url
  url=$(package_url "$file")
  echo "Uploading $file -> $url"
  curl --fail-with-body --silent --show-error \
    --header "JOB-TOKEN: ${CI_JOB_TOKEN}" \
    --upload-file "$file" \
    "$url"
  echo
}

# semantic-release's @semantic-release/git plugin pushes the tag, which fires
# this pipeline; @semantic-release/gitlab creates the release in a later step
# of the same job. Poll briefly so the asset-link calls don't 404.
wait_for_release() {
  local url="${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/releases/${CI_COMMIT_TAG}"
  for attempt in $(seq 1 30); do
    if curl --fail --silent --header "JOB-TOKEN: ${CI_JOB_TOKEN}" "$url" >/dev/null; then
      echo "release ${CI_COMMIT_TAG} is available"
      return 0
    fi
    echo "release ${CI_COMMIT_TAG} not yet available (attempt $attempt/30); waiting 10s"
    sleep 10
  done
  echo "release ${CI_COMMIT_TAG} did not appear in time" >&2
  return 1
}

add_release_link() {
  local file="$1"
  local link_type="$2"
  local url
  url=$(package_url "$file")
  echo "Linking $file ($link_type) on release ${CI_COMMIT_TAG}"
  curl --fail-with-body --silent --show-error \
    --request POST \
    --header "JOB-TOKEN: ${CI_JOB_TOKEN}" \
    --data "name=${file}" \
    --data-urlencode "url=${url}" \
    --data "link_type=${link_type}" \
    "${CI_API_V4_URL}/projects/${CI_PROJECT_ID}/releases/${CI_COMMIT_TAG}/assets/links" \
    > /dev/null
}

for artifact in "${ARTIFACTS[@]}"; do
  upload_file "$artifact"
  upload_file "${artifact}.sha256"
done

wait_for_release

for artifact in "${ARTIFACTS[@]}"; do
  add_release_link "$artifact" "package"
  add_release_link "${artifact}.sha256" "other"
done

echo "Done."

#!/usr/bin/env bash
# =============================================================================
# build-images.sh -- Build custom CNG images with feature branch Rails code
#
# Pulls stock CNG base images, overlays the feature branch Rails code from a
# local GitLab checkout, and tags the result for use with imagePullPolicy: Never.
#
# Usage:
#   ./build-images.sh [gitlab-source-dir]
#
# All configuration (registry, tag, prefix) comes from config.sh.
# =============================================================================
set -euo pipefail

# shellcheck source=config.sh
source "$(cd "$(dirname "$0")" && pwd)/config.sh"

# Allow overriding GITLAB_SRC via positional arg
GITLAB_SRC="${1:-${GITLAB_SRC}}"

# Validate source directory
if [ ! -f "${GITLAB_SRC}/Gemfile" ]; then
  echo "ERROR: ${GITLAB_SRC}/Gemfile not found."
  echo "Usage: $0 [path-to-gitlab-source]"
  exit 1
fi

echo "=== GKG E2E: Building custom CNG images ==="
echo "  Source:   ${GITLAB_SRC}"
echo "  Base tag: ${BASE_TAG}"
echo "  Registry: ${CNG_REGISTRY}"
echo "  Prefix:   ${LOCAL_PREFIX}"
echo ""

# Stage Rails code to a temp directory to avoid the GitLab .dockerignore
# which excludes nearly everything (it's designed for GitLab's CI builds).
STAGING_DIR=$(mktemp -d)
trap 'rm -rf "${STAGING_DIR}"' EXIT

echo "--- Staging Rails code to ${STAGING_DIR} ---"
for dir in app config db ee lib locale gems; do
  echo "    Copying ${dir}/"
  cp -a "${GITLAB_SRC}/${dir}" "${STAGING_DIR}/${dir}"
done
# vendor/gems/ contains source-tracked vendored gems referenced by Gemfile.
# We copy it as vendor_gems/ to the staging dir and use a separate COPY in the
# Dockerfile (to avoid the large vendor/bundle/ directory).
echo "    Copying vendor/gems/ -> vendor_gems/"
cp -a "${GITLAB_SRC}/vendor/gems" "${STAGING_DIR}/vendor_gems"
echo "    Copying Gemfile, Gemfile.lock"
cp "${GITLAB_SRC}/Gemfile" "${STAGING_DIR}/Gemfile"
cp "${GITLAB_SRC}/Gemfile.lock" "${STAGING_DIR}/Gemfile.lock"

# Create a permissive .dockerignore in the staging dir
echo ".git" > "${STAGING_DIR}/.dockerignore"

echo "    Staged $(du -sh "${STAGING_DIR}" | cut -f1) of Rails code"
echo ""

for component in "${CNG_COMPONENTS[@]}"; do
  echo "--- Building ${LOCAL_PREFIX}/${component}:${LOCAL_TAG} ---"
  echo "    Base: ${CNG_REGISTRY}/${component}:${BASE_TAG}"

  docker build \
    ${NO_CACHE:+--no-cache} \
    --build-arg "BASE_IMAGE=${CNG_REGISTRY}/${component}" \
    --build-arg "BASE_TAG=${BASE_TAG}" \
    -f "${SCRIPT_DIR}/Dockerfile.rails" \
    -t "${LOCAL_PREFIX}/${component}:${LOCAL_TAG}" \
    "${STAGING_DIR}"

  echo "    Done: ${LOCAL_PREFIX}/${component}:${LOCAL_TAG}"
  echo ""
done

echo "=== All images built ==="
echo ""
echo "Images available in docker daemon:"
docker images "${LOCAL_PREFIX}/*" --format "  {{.Repository}}:{{.Tag}}  ({{.Size}})"
echo ""
echo "These are referenced in gitlab-values.yaml with imagePullPolicy: Never."
echo "If using colima, they're already in colima's docker daemon."

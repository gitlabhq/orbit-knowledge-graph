#!/bin/sh

set -e

IMAGE_NAME="${IMAGE_NAME:-gkg}"

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <tag> [tag...]"
  exit 1
fi

if [ -n "$CI_REGISTRY_USER" ] && [ -n "$CI_REGISTRY_PASSWORD" ] && [ -n "$CI_REGISTRY" ]; then
  echo "$CI_REGISTRY_PASSWORD" | docker login -u "$CI_REGISTRY_USER" --password-stdin "$CI_REGISTRY"
fi

BUILD_ARGS=""
if [ -n "$GKG_VERSION" ]; then
  BUILD_ARGS="--build-arg GKG_VERSION=$GKG_VERSION"
fi

ARCH=$(uname -m)
case "$ARCH" in
  aarch64) PLATFORM="linux/arm64" ;;
  *)       PLATFORM="linux/amd64" ;;
esac

TAGS=""
for tag in "$@"; do
  TAGS="$TAGS -t $tag"
done

echo "Building for ${PLATFORM}:$TAGS"

docker buildx create --use 2>/dev/null || true

docker buildx build \
  --platform "$PLATFORM" \
  --push \
  --cache-from "type=registry,ref=${CI_REGISTRY_IMAGE}/cache/${IMAGE_NAME}:${PLATFORM##*/}" \
  --cache-to   "type=registry,mode=max,compression=zstd,oci-mediatypes=true,ref=${CI_REGISTRY_IMAGE}/cache/${IMAGE_NAME}:${PLATFORM##*/}" \
  --label "com.gitlab/ci-pipeline-url=${CI_PIPELINE_URL}" \
  --label "com.gitlab/ci-job-url=${CI_JOB_URL}" \
  --label "com.gitlab/commit-sha=${CI_COMMIT_SHA}" \
  --provenance=true \
  $BUILD_ARGS \
  $TAGS \
  .

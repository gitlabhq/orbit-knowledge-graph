#!/bin/sh

set -e

IMAGE_NAME="${IMAGE_NAME:-gkg}"

if [ "$#" -eq 0 ]; then
  echo "Usage: $0 <tag> [tag...]"
  exit 1
fi

TAGS=""
for tag in "$@"; do
  TAGS="$TAGS -t $tag"
done

if [ -n "$CI_REGISTRY_USER" ] && [ -n "$CI_REGISTRY_PASSWORD" ] && [ -n "$CI_REGISTRY" ]; then
  echo "$CI_REGISTRY_PASSWORD" | docker login -u "$CI_REGISTRY_USER" --password-stdin "$CI_REGISTRY"
fi

docker buildx create --use 2>/dev/null || true

BUILD_ARGS=""
if [ -n "$GKG_VERSION" ]; then
  BUILD_ARGS="--build-arg GKG_VERSION=$GKG_VERSION"
fi

echo "Building and pushing:$TAGS"

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  --push \
  --cache-from type=registry,ref=${CI_REGISTRY_IMAGE}/cache/${IMAGE_NAME}:develop \
  --cache-to   type=registry,mode=max,compression=zstd,oci-mediatypes=true,ref=${CI_REGISTRY_IMAGE}/cache/${IMAGE_NAME}:develop \
  --label "com.gitlab/ci-pipeline-url=${CI_PIPELINE_URL}" \
  --label "com.gitlab/ci-job-url=${CI_JOB_URL}" \
  --label "com.gitlab/commit-sha=${CI_COMMIT_SHA}" \
  --provenance=true \
  $BUILD_ARGS \
  $TAGS \
  .

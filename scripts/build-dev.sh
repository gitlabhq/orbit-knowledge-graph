#!/bin/bash
set -e

IMAGE_TAG="${1:-gkg-server:dev}"

# Check if running in CI with sccache
SCCACHE_ARGS=""
if [ -n "${SCCACHE_GCS_BUCKET}" ] && [ -f "${SCCACHE_GCS_KEY}" ]; then
    SCCACHE_ARGS="-e SCCACHE_GCS_BUCKET=${SCCACHE_GCS_BUCKET} \
        -e SCCACHE_GCS_KEY_PATH=/gcs-key.json \
        -e SCCACHE_GCS_RW_MODE=READ_WRITE \
        -e RUSTC_WRAPPER=sccache \
        -v ${SCCACHE_GCS_KEY}:/gcs-key.json:ro"
fi

# Build using rust-builder image with cached volumes (amd64)
docker run --rm --platform linux/amd64 \
    -v "$(pwd)":/build \
    -v gkg-cargo-cache:/usr/local/cargo/registry \
    -v gkg-target-cache:/build/target \
    ${SCCACHE_ARGS} \
    -w /build \
    registry.gitlab.com/gitlab-org/orbit/build-images/rust-builder:latest \
    cargo build -p gkg-server

# Copy binary from cache volume and build runtime image
docker run --rm --platform linux/amd64 \
    -v gkg-target-cache:/target \
    -v "$(pwd)":/out \
    busybox \
    cp /target/debug/gkg-server /out/gkg-server

docker build --platform linux/amd64 -t "$IMAGE_TAG" -f Dockerfile.dev .

rm -f gkg-server

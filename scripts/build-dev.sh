#!/bin/bash
set -e

IMAGE_TAG="${1:-gkg-server:dev}"

HOST_OS="$(uname -s)"
HOST_ARCH="$(uname -m)"

# Determine Docker platform based on host architecture
case "$HOST_ARCH" in
    arm64|aarch64)
        RUST_TARGET="aarch64-unknown-linux-gnu"
        DOCKER_PLATFORM="linux/arm64"
        ;;
    x86_64)
        RUST_TARGET="x86_64-unknown-linux-gnu"
        DOCKER_PLATFORM="linux/amd64"
        ;;
    *)
        echo "Unsupported architecture: $HOST_ARCH"
        exit 1
        ;;
esac

# CI mode: build in Docker with sccache, native arch for cache hits with other jobs
# Local mode: build on host for fast incremental compilation
if [ -n "$CI" ] || ! command -v cargo &> /dev/null; then
    echo "Building in Docker (CI mode) for linux/amd64"

    SCCACHE_ARGS=""
    if [ -n "${SCCACHE_GCS_BUCKET}" ] && [ -f "${SCCACHE_GCS_KEY}" ]; then
        SCCACHE_ARGS="-e SCCACHE_GCS_BUCKET=${SCCACHE_GCS_BUCKET} \
            -e SCCACHE_GCS_KEY_PATH=/gcs-key.json \
            -e SCCACHE_GCS_RW_MODE=READ_WRITE \
            -e RUSTC_WRAPPER=sccache \
            -v ${SCCACHE_GCS_KEY}:/gcs-key.json:ro"
    fi

    docker run --rm \
        -v "$(pwd)":/build \
        ${SCCACHE_ARGS} \
        -w /build \
        registry.gitlab.com/gitlab-org/orbit/build-images/rust-builder:latest \
        cargo build -p gkg-server

    CONTEXT_DIR=$(mktemp -d)
    trap "rm -rf $CONTEXT_DIR" EXIT
    cp target/debug/gkg-server "$CONTEXT_DIR/"

    DOCKER_PLATFORM="linux/amd64"
else
    echo "Building on host for $RUST_TARGET (host: $HOST_OS/$HOST_ARCH)"

    rustup target add "$RUST_TARGET" 2>/dev/null || true

    if [ "$HOST_OS" = "Darwin" ]; then
        if ! command -v cargo-zigbuild &> /dev/null; then
            echo "Installing cargo-zigbuild for cross-compilation..."
            cargo install cargo-zigbuild
        fi
        if ! command -v zig &> /dev/null; then
            echo "Error: zig is required for cross-compilation on macOS"
            echo "Install with: mise install"
            exit 1
        fi
        cargo zigbuild -p gkg-server --target "$RUST_TARGET"
    else
        cargo build -p gkg-server --target "$RUST_TARGET"
    fi

    CONTEXT_DIR=$(mktemp -d)
    trap "rm -rf $CONTEXT_DIR" EXIT
    cp "target/$RUST_TARGET/debug/gkg-server" "$CONTEXT_DIR/"
fi

docker build --platform "$DOCKER_PLATFORM" -t "$IMAGE_TAG" -f - "$CONTEXT_DIR" <<'EOF'
FROM registry.access.redhat.com/ubi9/ubi-micro:latest
COPY gkg-server /usr/local/bin/gkg-server
ENTRYPOINT ["gkg-server"]
EOF

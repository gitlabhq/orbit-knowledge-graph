#!/bin/bash
set -e

# macOS defaults to 256 FDs which is too few for linking this project
ulimit -n 10240 2>/dev/null || true

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

docker build --platform "$DOCKER_PLATFORM" -t "$IMAGE_TAG" \
    --build-arg BINARY="target/$RUST_TARGET/debug/gkg-server" \
    -f - . <<'EOF'
FROM registry.access.redhat.com/ubi10/ubi-minimal:latest
ARG BINARY
WORKDIR /app
COPY ${BINARY} /usr/local/bin/gkg-server
ENTRYPOINT ["gkg-server"]
EOF

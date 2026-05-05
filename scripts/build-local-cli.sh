#!/usr/bin/env bash
set -euo pipefail

# Build the `orbit` local CLI binary and package it as
# orbit-<platform>-<arch>.tar.gz in the repository root. PLATFORM/ARCH default
# to the host (linux/macOS amd64 or arm64). Supported triples:
# {x86_64,aarch64}-unknown-linux-gnu and {x86_64,aarch64}-apple-darwin.

PLATFORM="${PLATFORM:-$(uname -s)}"
PLATFORM=$(echo "$PLATFORM" | tr '[:upper:]' '[:lower:]')

ARCH="${ARCH:-$(uname -m)}"
case "$ARCH" in
    arm64) ARCH="aarch64" ;;
esac

case "$PLATFORM" in
    darwin)
        case "$ARCH" in
            aarch64) TARGET="aarch64-apple-darwin" ;;
            x86_64)  TARGET="x86_64-apple-darwin" ;;
        esac
        ;;
    linux)
        case "$ARCH" in
            aarch64) TARGET="aarch64-unknown-linux-gnu" ;;
            x86_64)  TARGET="x86_64-unknown-linux-gnu" ;;
        esac
        ;;
esac

if [ -z "${TARGET:-}" ]; then
    echo "unsupported platform/arch: $PLATFORM/$ARCH" >&2
    exit 1
fi

# Idempotent; no-op if the target is already installed.
rustup target add "$TARGET"

echo "Building orbit for $PLATFORM/$ARCH ($TARGET)"
cargo build --release --locked --bin orbit --target "$TARGET"

ARCHIVE="orbit-${PLATFORM}-${ARCH}.tar.gz"
tar -czvf "$ARCHIVE" -C "target/${TARGET}/release" orbit
echo "created $ARCHIVE"

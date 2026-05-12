#!/usr/bin/env bash
set -euo pipefail

# Build the `orbit` local CLI binary and package it as
# orbit-local-<platform>-<arch>.tar.gz in the repository root. The binary
# inside the archive is `orbit` (or `orbit.exe` on Windows); the `orbit-local-`
# prefix on the archive matches the orbit-local crate name and disambiguates
# from the gkg-server image release. PLATFORM/ARCH default to the host.
# Supported triples: {x86_64,aarch64}-unknown-linux-gnu,
# {x86_64,aarch64}-apple-darwin, and x86_64-pc-windows-msvc.

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
    windows)
        case "$ARCH" in
            x86_64) TARGET="x86_64-pc-windows-msvc" ;;
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
# Bundle libduckdb (compile from C++) so the released binary is self-contained.
cargo build --release --locked --bin orbit --target "$TARGET" --features duckdb-client/bundled

ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.tar.gz"
if [ "$PLATFORM" = "windows" ]; then
    tar -czvf "$ARCHIVE" -C "target/${TARGET}/release" orbit.exe
else
    tar -czvf "$ARCHIVE" -C "target/${TARGET}/release" orbit
fi
echo "created $ARCHIVE"

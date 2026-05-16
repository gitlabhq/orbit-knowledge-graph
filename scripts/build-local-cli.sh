#!/usr/bin/env bash
set -euo pipefail

# Build the `orbit` local CLI binary and package it as
# orbit-local-<platform>-<arch>.(tar.gz|zip) in the repository root.
# The binary inside the archive is `orbit` (or `orbit.exe` on Windows); the
# `orbit-local-` prefix on the archive matches the orbit-local crate name and
# disambiguates from the gkg-server image release. PLATFORM/ARCH default to
# the host (linux/macOS amd64 or arm64).
#
# Supported triples:
#   {x86_64,aarch64}-unknown-linux-gnu
#   {x86_64,aarch64}-apple-darwin
#   x86_64-pc-windows-gnullvm        (cross-compiled with llvm-mingw on Linux)

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
            x86_64) TARGET="x86_64-pc-windows-gnullvm" ;;
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

BIN_DIR="target/${TARGET}/release"

if [ "$PLATFORM" = "windows" ]; then
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.zip"
    (cd "$BIN_DIR" && zip "$OLDPWD/$ARCHIVE" orbit.exe)
else
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.tar.gz"
    tar -czvf "$ARCHIVE" -C "$BIN_DIR" orbit
fi

echo "created $ARCHIVE"

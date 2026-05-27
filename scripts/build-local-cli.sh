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
#   {x86_64,aarch64}-unknown-linux-{gnu,musl}
#   {x86_64,aarch64}-apple-darwin
#   x86_64-pc-windows-gnullvm        (cross-compiled with llvm-mingw on Linux)
#
# Linux builds default to the existing glibc target. Set LIBC=musl to build the
# fully static musl variant with cargo-zigbuild.

PLATFORM="${PLATFORM:-$(uname -s)}"
PLATFORM=$(echo "$PLATFORM" | tr '[:upper:]' '[:lower:]')

ARCH="${ARCH:-$(uname -m)}"
case "$ARCH" in
    arm64) ARCH="aarch64" ;;
esac

LIBC="${LIBC:-gnu}"

case "$PLATFORM" in
    darwin)
        case "$ARCH" in
            aarch64) TARGET="aarch64-apple-darwin" ;;
            x86_64)  TARGET="x86_64-apple-darwin" ;;
        esac
        ;;
    linux)
        case "$LIBC" in
            gnu|musl) ;;
            *)
                echo "unsupported Linux libc: $LIBC" >&2
                exit 1
                ;;
        esac
        case "$ARCH" in
            aarch64) TARGET="aarch64-unknown-linux-${LIBC}" ;;
            x86_64)  TARGET="x86_64-unknown-linux-${LIBC}" ;;
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

if [ "$PLATFORM" = "windows" ]; then
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.zip"
elif [ "$PLATFORM" = "linux" ] && [ "$LIBC" = "musl" ]; then
    ARCHIVE="orbit-local-${PLATFORM}-${LIBC}-${ARCH}.tar.gz"
else
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.tar.gz"
fi

if [ "${PRINT_TARGET:-0}" = "1" ]; then
    printf 'TARGET=%s\nARCHIVE=%s\n' "$TARGET" "$ARCHIVE"
    exit 0
fi

# Idempotent; no-op if the target is already installed.
rustup target add "$TARGET"

echo "Building orbit for $PLATFORM/$ARCH ($TARGET)"
# Bundle libduckdb (compile from C++) so the released binary is self-contained.
if [[ "$TARGET" == *-musl ]]; then
    command -v cargo-zigbuild >/dev/null || {
        echo "cargo-zigbuild is required for musl local CLI builds" >&2
        exit 1
    }
    cargo zigbuild --release --locked --bin orbit --target "$TARGET" --features duckdb-client/bundled
else
    cargo build --release --locked --bin orbit --target "$TARGET" --features duckdb-client/bundled
fi

BIN_DIR="target/${TARGET}/release"

if [ "$PLATFORM" = "windows" ]; then
    (cd "$BIN_DIR" && zip "$OLDPWD/$ARCHIVE" orbit.exe)
else
    if [ "$PLATFORM" = "linux" ] && [ "$LIBC" = "musl" ]; then
        if command -v file >/dev/null; then
            file "$BIN_DIR/orbit"
            file "$BIN_DIR/orbit" | grep -q "statically linked"
        fi
    fi
    tar -czvf "$ARCHIVE" -C "$BIN_DIR" orbit
fi

echo "created $ARCHIVE"

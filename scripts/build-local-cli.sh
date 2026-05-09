#!/usr/bin/env bash
set -euo pipefail

# Build the `orbit` local CLI binary and package it as
# orbit-local-<platform>-<arch>.{tar.gz,zip} in the repository root. The
# binary inside the archive is `orbit` (or `orbit.exe` on Windows); the
# `orbit-local-` prefix on the archive matches the orbit-local crate name
# and disambiguates from the gkg-server image release. PLATFORM/ARCH
# default to the host. Supported triples:
#   {x86_64,aarch64}-unknown-linux-gnu
#   {x86_64,aarch64}-apple-darwin
#   x86_64-pc-windows-msvc (cross-compiled from Linux via cargo-xwin)

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

if [ "$PLATFORM" = "windows" ]; then
    # cargo-xwin pulls Microsoft SDK headers/libs and drives clang-cl + lld so we
    # cross-compile MSVC-ABI binaries from Linux. libduckdb-sys downloads a
    # prebuilt libduckdb for the target (DUCKDB_DOWNLOAD_LIB=1) so we skip a
    # Windows C++ toolchain entirely. The duckdb.dll lives next to orbit.exe in
    # the zip so the binary is runnable out of the box.
    : "${DUCKDB_DOWNLOAD_LIB:=1}"
    export DUCKDB_DOWNLOAD_LIB
    cargo xwin build --release --locked --bin orbit --target "$TARGET"

    OUT_DIR="target/${TARGET}/release"
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.zip"
    DLL_PATH=$(find "$OUT_DIR" -maxdepth 4 -iname "duckdb.dll" -print -quit || true)
    STAGE=$(mktemp -d)
    cp "${OUT_DIR}/orbit.exe" "${STAGE}/orbit.exe"
    if [ -n "${DLL_PATH:-}" ]; then
        cp "$DLL_PATH" "${STAGE}/duckdb.dll"
    fi
    (cd "$STAGE" && zip -9 "${OLDPWD}/${ARCHIVE}" ./*)
    rm -rf "$STAGE"
else
    # Bundle libduckdb (compile from C++) so the released binary is self-contained.
    cargo build --release --locked --bin orbit --target "$TARGET" --features duckdb-client/bundled
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.tar.gz"
    tar -czvf "$ARCHIVE" -C "target/${TARGET}/release" orbit
fi

echo "created $ARCHIVE"

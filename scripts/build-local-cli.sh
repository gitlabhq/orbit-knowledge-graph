#!/usr/bin/env bash
set -euo pipefail

# Build the `orbit` local CLI binary and package it as
# orbit-local-<platform>-<arch>[-<method>].(tar.gz|zip) in the repository root.
# The binary inside the archive is `orbit` (or `orbit.exe` on Windows); the
# `orbit-local-` prefix on the archive matches the orbit-local crate name and
# disambiguates from the gkg-server image release. PLATFORM/ARCH default to
# the host (linux/macOS amd64 or arm64). For windows builds, WINDOWS_METHOD
# selects the cross-compile toolchain: xwin|zigbuild|mingw.
#
# Supported triples:
#   {x86_64,aarch64}-unknown-linux-gnu
#   {x86_64,aarch64}-apple-darwin
#   x86_64-pc-windows-msvc       (WINDOWS_METHOD=xwin)
#   x86_64-pc-windows-gnu        (WINDOWS_METHOD=zigbuild|mingw)

PLATFORM="${PLATFORM:-$(uname -s)}"
PLATFORM=$(echo "$PLATFORM" | tr '[:upper:]' '[:lower:]')

ARCH="${ARCH:-$(uname -m)}"
case "$ARCH" in
    arm64) ARCH="aarch64" ;;
esac

CARGO_BUILD=(cargo build)

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
        case "${WINDOWS_METHOD:?WINDOWS_METHOD must be set to xwin|zigbuild|mingw}" in
            xwin)
                TARGET="x86_64-pc-windows-msvc"
                CARGO_BUILD=(cargo xwin build)
                ;;
            zigbuild)
                # gnullvm avoids mingw's dlltool; zig provides the LLVM tooling.
                # Wrappers in scripts/ci/zig-windows-*.sh strip aws-lc-sys's
                # GCC-only `-Wp,*` flags before forwarding to zig.
                TARGET="x86_64-pc-windows-gnullvm"
                ;;
            mingw)
                TARGET="x86_64-pc-windows-gnu"
                ;;
            *)
                echo "unknown WINDOWS_METHOD: $WINDOWS_METHOD (want xwin|zigbuild|mingw)" >&2
                exit 1
                ;;
        esac
        ;;
esac

if [ -z "${TARGET:-}" ]; then
    echo "unsupported platform/arch: $PLATFORM/$ARCH" >&2
    exit 1
fi

# Idempotent; no-op if the target is already installed.
rustup target add "$TARGET"

echo "Building orbit for $PLATFORM/$ARCH ($TARGET) via ${CARGO_BUILD[*]}"
# Bundle libduckdb (compile from C++) so the released binary is self-contained.
"${CARGO_BUILD[@]}" --release --locked --bin orbit --target "$TARGET" --features duckdb-client/bundled

BIN_DIR="target/${TARGET}/release"

if [ "$PLATFORM" = "windows" ]; then
    BIN="orbit.exe"
    if ! file "$BIN_DIR/$BIN" | grep -q "PE32+"; then
        echo "smoke check failed: $BIN_DIR/$BIN is not a PE32+ binary" >&2
        file "$BIN_DIR/$BIN" >&2
        exit 1
    fi
    size_bytes=$(stat -c%s "$BIN_DIR/$BIN")
    echo "binary size: $size_bytes bytes"
    if [ "$size_bytes" -lt 50000000 ] || [ "$size_bytes" -gt 250000000 ]; then
        echo "smoke check failed: size $size_bytes outside 50MB..250MB range" >&2
        exit 1
    fi
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}-${WINDOWS_METHOD}.zip"
    (cd "$BIN_DIR" && zip "$OLDPWD/$ARCHIVE" "$BIN")
else
    BIN="orbit"
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.tar.gz"
    tar -czvf "$ARCHIVE" -C "$BIN_DIR" "$BIN"
fi

echo "created $ARCHIVE"

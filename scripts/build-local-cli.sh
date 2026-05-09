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
    # Reject any non-system DLL imports — mingw/libc++/libunwind runtimes
    # would mean the binary isn't self-contained on a stock Win10+ box.
    OBJDUMP=$(command -v x86_64-w64-mingw32-objdump || command -v llvm-objdump || echo objdump)
    echo "DLL imports:"
    "$OBJDUMP" -p "$BIN_DIR/$BIN" | grep "DLL Name" || true
    bad=$("$OBJDUMP" -p "$BIN_DIR/$BIN" \
        | awk '/DLL Name:/ { print tolower($NF) }' | sort -u \
        | grep -Ev '^(api-ms-win-[a-z0-9.-]+|msvcrt|ucrtbase|kernel32|advapi32|user32|ws2_32|bcrypt|bcryptprimitives|combase|ktmw32|ncrypt|rstrtmgr|secur32|crypt32|ntdll|userenv|shell32|ole32|oleaut32|gdi32|rpcrt4|psapi|powrprof|version|cfgmgr32|opengl32|imm32|imagehlp|msimg32|winspool|synchronization|dbghelp|wininet|winhttp|setupapi|iphlpapi)\.dll$' \
        || true)
    if [ -n "$bad" ]; then
        echo "smoke check failed: binary depends on non-system DLLs:" >&2
        echo "$bad" >&2
        exit 1
    fi
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.zip"
    (cd "$BIN_DIR" && zip "$OLDPWD/$ARCHIVE" "$BIN")
else
    BIN="orbit"
    ARCHIVE="orbit-local-${PLATFORM}-${ARCH}.tar.gz"
    tar -czvf "$ARCHIVE" -C "$BIN_DIR" "$BIN"
fi

echo "created $ARCHIVE"

#!/usr/bin/env bash
set -euo pipefail

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
    ARCHIVE="orbit-cli-${PLATFORM}-${ARCH}.zip"
elif [ "$PLATFORM" = "linux" ] && [ "$LIBC" = "musl" ]; then
    ARCHIVE="orbit-cli-${PLATFORM}-${LIBC}-${ARCH}.tar.gz"
else
    ARCHIVE="orbit-cli-${PLATFORM}-${ARCH}.tar.gz"
fi

if [ "${PRINT_TARGET:-0}" = "1" ]; then
    printf 'TARGET=%s\nARCHIVE=%s\n' "$TARGET" "$ARCHIVE"
    exit 0
fi

TOOLCHAIN=$(rustc -vV | awk '/^release:/ { print $2 }')
rustup target add --toolchain "$TOOLCHAIN" "$TARGET"

echo "Building orbit for $PLATFORM/$ARCH ($TARGET)"
if [ "$PLATFORM" = "linux" ]; then
    command -v cargo-zigbuild >/dev/null || {
        echo "cargo-zigbuild is required for Linux local CLI builds" >&2
        exit 1
    }
    ZIG_TARGET="$TARGET"
    [ "$LIBC" = "gnu" ] && ZIG_TARGET="${TARGET}.2.28"
    BUILD=(rustup run "$TOOLCHAIN" cargo zigbuild --release --locked -p orbit-cli --bin orbit --target "$ZIG_TARGET" --features duckdb-client/bundled)
else
    BUILD=(rustup run "$TOOLCHAIN" cargo build --release --locked -p orbit-cli --bin orbit --target "$TARGET" --features duckdb-client/bundled)
fi
"${BUILD[@]}"

if [ -n "${CI_COMMIT_TAG:-}" ]; then
    expected="[\"ORBIT_RELEASE_VERSION\",\"${CI_COMMIT_TAG#v}\"]"
    if ! "${BUILD[@]}" --message-format=json | grep -F "$expected" >/dev/null; then
        echo "error: orbit was not built with release version ${CI_COMMIT_TAG#v}" >&2
        exit 1
    fi
fi

BIN_DIR="target/${TARGET}/release"

if [ "$PLATFORM" = "windows" ]; then
    (cd "$BIN_DIR" && zip "$OLDPWD/$ARCHIVE" orbit.exe)
else
    if [ "$PLATFORM" = "linux" ] && [ "$LIBC" = "musl" ]; then
        if ! command -v file >/dev/null; then
            echo "error: file(1) is required to verify musl binaries are statically linked" >&2
            exit 1
        fi
        file "$BIN_DIR/orbit"
        if ! file "$BIN_DIR/orbit" | grep -q "statically linked"; then
            echo "error: musl build is not statically linked" >&2
            exit 1
        fi
    fi
    tar -czvf "$ARCHIVE" -C "$BIN_DIR" orbit
fi

echo "created $ARCHIVE"

#!/usr/bin/env bash
set -euo pipefail

if [ $# -ne 3 ]; then
    echo "Usage: $0 <archive> <platform> <binary>" >&2
    exit 1
fi

archive=$1
platform=$2
binary=$3

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

case "$platform" in
    macos)
        signer=sign-macos-binaries
        signer_args=(--rcodesign-args "--entitlements-xml-file ${script_dir}/macos-entitlements.plist")
        ;;
    windows)
        signer=sign-windows-binaries
        signer_args=()
        ;;
    *) echo "unsupported platform: $platform" >&2; exit 1 ;;
esac

archive_abs=$(readlink -f "$archive")
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

case "$archive" in
    *.zip)
        unzip -q "$archive_abs" -d "$work"
        ;;
    *.tar.gz|*.tgz)
        tar -xzvf "$archive_abs" -C "$work"
        ;;
    *)
        echo "unsupported archive: $archive" >&2; exit 1 ;;
esac

"$signer" ${signer_args[@]+"${signer_args[@]}"} "$work/$binary"
rm -f "$work/${binary}.unsigned"

case "$archive" in
    *.zip)
        rm -f "$archive_abs"
        (cd "$work" && zip -qr "$archive_abs" .)
        ;;
    *)
        (cd "$work" && tar -czvf "$archive_abs" -- *)
        ;;
esac

echo "signed and repacked $archive"

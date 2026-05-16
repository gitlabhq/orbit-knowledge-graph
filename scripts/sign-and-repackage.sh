#!/usr/bin/env bash
set -euo pipefail

# Re-emit an archive with its inner binary signed via the code-signer image.
# Must run inside the gitlab-com/gl-infra/common-ci-tasks-images/code-signer
# image, with .google-oidc:auth already attached. For .zip archives the
# environment must also provide `unzip` and `zip`.
#
# Usage: scripts/sign-and-repackage.sh <archive> <platform> <binary>
# Platforms: macos, windows
# Archives:  .tar.gz/.tgz, .zip

if [ $# -ne 3 ]; then
    echo "Usage: $0 <archive> <platform> <binary>" >&2
    exit 1
fi

archive=$1
platform=$2
binary=$3

case "$platform" in
    macos)   signer=sign-macos-binaries ;;
    windows) signer=sign-windows-binaries ;;
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

"$signer" "$work/$binary"
rm -f "$work/${binary}.unsigned"

case "$archive" in
    *.zip)
        rm -f "$archive_abs"
        (cd "$work" && zip -qr "$archive_abs" .)
        ;;
    *)
        tar -czvf "$archive_abs" -C "$work" .
        ;;
esac

echo "signed and repacked $archive"

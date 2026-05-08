#!/usr/bin/env bash
set -euo pipefail

# Re-emit a tarball with its inner binary signed via the code-signer image.
# Must run inside the gitlab-com/gl-infra/common-ci-tasks-images/code-signer
# image, with .google-oidc:auth already attached.
#
# Usage: scripts/sign-and-repackage.sh <tarball> <platform> <binary>
# Platforms: macos, windows

if [ $# -ne 3 ]; then
    echo "Usage: $0 <tarball> <platform> <binary>" >&2
    exit 1
fi

tarball=$1
platform=$2
binary=$3

case "$platform" in
    macos)   signer=sign-macos-binaries ;;
    windows) signer=sign-windows-binaries ;;
    *) echo "unsupported platform: $platform" >&2; exit 1 ;;
esac

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

tar -xzvf "$tarball" -C "$work"
"$signer" "$work/$binary"
rm -f "$work/${binary}.unsigned"
tar -czvf "$tarball" -C "$work" .

echo "signed and repacked $tarball"

#!/usr/bin/env bash
set -euo pipefail

PROTOC_VERSION="34.1"
PROTOC_SHA256="5c6057638aa382542e75f1c5a0802893e2311ad0c8b689e635dd4ac3c9eb8169"
ROOT="$HOME/protoc"
ZIP="protoc-${PROTOC_VERSION}-osx-universal_binary.zip"

tmpdir="$(mktemp -d)"
curl -fsSL --retry 3 -o "${tmpdir}/${ZIP}" \
    "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${ZIP}"
echo "${PROTOC_SHA256}  ${tmpdir}/${ZIP}" | shasum -a 256 -c -
mkdir -p "$ROOT"
unzip -qo "${tmpdir}/${ZIP}" -d "$ROOT"
rm -rf "$tmpdir"

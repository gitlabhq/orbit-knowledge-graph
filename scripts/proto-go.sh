#!/usr/bin/env bash
set -euo pipefail

PROTO_SRC="crates/gkg-server/proto/gkg.proto"
PROTO_DIR="crates/gkg-server/proto"
OUT_DIR="gkgpb"
MODULE="gitlab.com/gitlab-org/orbit/knowledge-graph/gkgpb"

PROTOC_GEN_GO_VERSION="v1.36.11"
PROTOC_GEN_GO_GRPC_VERSION="v1.6.1"

install_tools() {
  go install "google.golang.org/protobuf/cmd/protoc-gen-go@${PROTOC_GEN_GO_VERSION}"
  go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}"
}

run_protoc() {
  local dest="$1"
  protoc --go_out="$dest" --go_opt=paths=source_relative \
         --go_opt="Mgkg.proto=${MODULE}" \
         --go-grpc_out="$dest" --go-grpc_opt=paths=source_relative \
         --go-grpc_opt="Mgkg.proto=${MODULE}" \
         -I"$PROTO_DIR" \
         "$PROTO_SRC"
}

cmd_generate() {
  install_tools
  run_protoc "$OUT_DIR"
  echo "Go proto stubs generated in ${OUT_DIR}/"
}

cmd_check() {
  install_tools
  local tmpdir
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "${tmpdir:-}"' EXIT

  run_protoc "$tmpdir"

  if ! diff -q "${OUT_DIR}/gkg.pb.go" "${tmpdir}/gkg.pb.go" >/dev/null 2>&1 ||
     ! diff -q "${OUT_DIR}/gkg_grpc.pb.go" "${tmpdir}/gkg_grpc.pb.go" >/dev/null 2>&1; then
    echo "Go proto stubs are out of date. Run 'mise run proto:go' and commit." >&2
    exit 1
  fi

  echo "Go proto stubs are up to date."
}

case "${1:-}" in
  generate) cmd_generate ;;
  check)    cmd_check ;;
  *)        echo "Usage: $0 {generate|check}" >&2; exit 1 ;;
esac

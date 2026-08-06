#!/usr/bin/env bash
# Build and push the xtask bench container image.
set -euo pipefail

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${BENCH_DIR}/.." && pwd)"

: "${CI_REGISTRY_IMAGE:=registry.gitlab.com/gitlab-org/orbit/knowledge-graph}"
: "${CI_COMMIT_SHORT_SHA:=$(git -C "${REPO_ROOT}" rev-parse --short=7 HEAD)}"

export XTASK_IMAGE="${CI_REGISTRY_IMAGE}/bench-xtask:${CI_COMMIT_SHORT_SHA}"

echo "==> Building ${XTASK_IMAGE}"
docker build -t "${XTASK_IMAGE}" -f "${BENCH_DIR}/Dockerfile" "${REPO_ROOT}"

echo "==> Pushing ${XTASK_IMAGE}"
docker push "${XTASK_IMAGE}"

echo "==> XTASK_IMAGE=${XTASK_IMAGE}"

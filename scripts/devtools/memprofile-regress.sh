#!/usr/bin/env bash
# Correctness gate for a code-indexing memory change: unit tests for the crates
# the memory work touches, the code-indexing integration suite against a real
# ClickHouse, and the code-graph resolution suites, which assert exact edge sets
# and are what catches a pipeline restructure that silently resolves differently.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
export DOCKER_HOST="${DOCKER_HOST:-unix://${HOME}/.colima/default/docker.sock}"

status=0
echo "== unit =="
cargo test -p code-graph -p indexer -p orbit-utils --lib 2>&1 |
  grep -E "^test result|^error" || status=1
echo "== code indexing integration =="
cargo nextest run -p integration-tests --retries 0 -E 'test(indexer::code)' 2>&1 |
  tail -3 || status=1
echo "== code-graph resolution suites =="
cargo nextest run -p integration-tests-codegraph --retries 0 2>&1 | tail -3 || status=1
exit "$status"

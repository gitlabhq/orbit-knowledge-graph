#!/usr/bin/env bash
set -euo pipefail

# Discover all integration-tests test targets except "containers" (which
# needs Docker/testcontainers) and run them alongside lib + bin tests.
#
# Test targets are auto-discovered from tests/*.rs files in the
# integration-tests crate — no Cargo.toml or CI changes needed.
#
# Extra arguments are forwarded to cargo nextest (e.g. --profile ci).

command -v jq >/dev/null 2>&1 || { echo "error: jq is required but not found" >&2; exit 1; }

NON_DOCKER_TESTS=$(
  cargo metadata --no-deps --format-version 1 | \
  jq -r '.packages[]
    | select(.name == "integration-tests")
    | .targets[]
    | select((.kind | index("test")) and .name != "containers")
    | .name'
)

args=(cargo nextest run --lib --bins)
while IFS= read -r t; do
  [[ -n "$t" ]] && args+=(--test "$t")
done <<<"$NON_DOCKER_TESTS"
args+=("$@")

echo "+ ${args[*]}"
exec "${args[@]}"

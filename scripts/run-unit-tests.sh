#!/usr/bin/env bash
set -euo pipefail

# Discover all integration-tests test targets except "containers" (which
# needs Docker/testcontainers) and run them alongside lib + bin tests.
#
# Test targets are auto-discovered from tests/*.rs files in the
# integration-tests crate — no Cargo.toml or CI changes needed.
#
# Extra arguments are forwarded to cargo nextest (e.g. --profile ci).

NON_DOCKER_TESTS=$(
  cargo metadata --no-deps --format-version 1 | \
  jq -r '.packages[]
    | select(.name == "integration-tests")
    | .targets[]
    | select((.kind | index("test")) and .name != "containers")
    | .name'
)

TEST_FLAGS=""
for t in $NON_DOCKER_TESTS; do
  TEST_FLAGS="$TEST_FLAGS --test $t"
done

CMD="cargo nextest run $* --lib --bins $TEST_FLAGS"
echo "+ $CMD"
# shellcheck disable=SC2086
exec $CMD

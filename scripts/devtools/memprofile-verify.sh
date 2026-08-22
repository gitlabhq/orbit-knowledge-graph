#!/usr/bin/env bash
# Prove a code-indexing change writes identical graph data, on one corpus repo.
#
# Usage: memprofile-verify.sh <project-id> <name> [--with-budgets]
#
# Needs two driver binaries built from the two revisions under comparison:
#   /tmp/cip-base   built from the revision you are comparing against
#   /tmp/cip-opt    built from the revision under test
# Build each by checking out `crates/code-index-profiler` plus the workspace
# `Cargo.toml` onto that revision and running
# `cargo build -p code-index-profiler --profile memprofile`.
#
# The per-file CPU budgets are disabled by default. At production budgets a
# large repository is not reproducible run to run: a file that lands near its
# budget aborts or not depending on machine load, which changes its definition,
# import and edge rows and the `reason` on its file row. Measured on the Linux
# kernel, the same binary run twice aborted 17 files once and 15 the next time
# and produced different row counts, so determinism has to be established before
# identity can be tested. Pass --with-budgets to reproduce that instead.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

ID="${1:?usage: memprofile-verify.sh <project-id> <name> [--with-budgets]}"
NAME="${2:?usage: memprofile-verify.sh <project-id> <name> [--with-budgets]}"
MODE="${3:-}"
export RUST_LOG="${RUST_LOG:-warn}"

BUDGETS=(--per-file-timeout-ms 0 --per-file-parse-timeout-ms 0
         --per-file-walk-timeout-ms 0 --per-file-ssa-timeout-ms 0)
SUFFIX="det"
if [[ "$MODE" == "--with-budgets" ]]; then
  BUDGETS=()
  SUFFIX="budgeted"
fi

for variant in base opt; do
  log=".memprofile/${NAME}-${SUFFIX}-${variant}.log"
  "/tmp/cip-${variant}" --project-id "$ID" --label "${NAME}-${SUFFIX}-${variant}" \
    --clickhouse-database "gkg_v${variant}" --cache-dir .memprofile/scratch \
    "${BUDGETS[@]}" > "$log" 2>&1
  echo "  ${variant}: exit=$? parse-aborts=$(grep -c 'parse aborted' "$log")"
done

python3 scripts/devtools/memprofile-verify-output.py \
  --baseline-db gkg_vbase --candidate-db gkg_vopt

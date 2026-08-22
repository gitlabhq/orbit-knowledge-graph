#!/usr/bin/env bash
# Build, index both corpus repositories, and diff each against its baseline.
#
# Usage: memprofile-ab.sh <label>
#
# Expects `es-baseline` and `gl-baseline` runs to already exist. Change one
# thing, run this, and read the two comparison tables.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

LABEL="${1:?usage: memprofile-ab.sh <label>}"
CORPUS_ES="${MEMPROFILE_ES_PROJECT:-100001}"
CORPUS_GL="${MEMPROFILE_GL_PROJECT:-278964}"
export RUST_LOG="${RUST_LOG:-warn,codegraph_mem=debug}"

cargo build -p code-index-profiler --profile memprofile 2>&1 | tail -1

for spec in "${CORPUS_ES}:es" "${CORPUS_GL}:gl"; do
  id=${spec%%:*}
  name=${spec##*:}
  ./target/memprofile/code-index-profiler --project-id "$id" \
    --label "${name}-${LABEL}" --cache-dir .memprofile/scratch \
    > ".memprofile/${name}-${LABEL}.log" 2>&1
  echo "${name} exit=$?"
done

status=0
for name in es gl; do
  echo
  python3 scripts/devtools/memprofile-compare.py \
    ".memprofile/runs/${name}-baseline" --candidate ".memprofile/runs/${name}-${LABEL}" || status=1
done
exit "$status"

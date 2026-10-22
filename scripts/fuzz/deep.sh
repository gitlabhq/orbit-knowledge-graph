#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tests="$root/crates/fuzz/tests"
budget="${FUZZ_BUDGET_SECONDS:-600}"
sanitizer="${FUZZ_SANITIZER:-address}"
index="${CI_NODE_INDEX:-1}"
total="${CI_NODE_TOTAL:-1}"

shopt -s nullglob
targets=()
i=0
for dir in "$tests"/fuzz_*/; do
  if ((i % total == index - 1)); then
    targets+=("$(basename "$dir")")
  fi
  i=$((i + 1))
done

if ((${#targets[@]} == 0)); then
  echo "shard $index/$total has no targets"
  exit 0
fi

seconds=$((budget / ${#targets[@]}))
echo "shard $index/$total: ${targets[*]} (${seconds}s each)"

"$root/scripts/fuzz/unpack-corpus.sh" "${targets[@]}"

failed=()
for target in "${targets[@]}"; do
  echo "+ cargo +nightly bolero test $target -p orbit-fuzz --engine libfuzzer --sanitizer $sanitizer -T ${seconds}s"
  cargo +nightly bolero test "$target" -p orbit-fuzz --engine libfuzzer --sanitizer "$sanitizer" -T "${seconds}s" ||
    failed+=("$target")
done

if ((${#failed[@]})); then
  echo "fuzz targets failed: ${failed[*]}" >&2
  exit 1
fi

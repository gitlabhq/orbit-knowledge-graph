#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tests="$root/crates/fuzz/tests"
mode="${1:-}"
index="${CI_NODE_INDEX:-1}"
total="${CI_NODE_TOTAL:-1}"

if [ "$mode" != smoke ] && [ "$mode" != deep ]; then
  echo "usage: run.sh smoke|deep" >&2
  exit 1
fi

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

"$root/scripts/fuzz/corpus.sh" pull "${targets[@]}"

if [ "$mode" = smoke ]; then
  cargo test -p orbit-fuzz --no-run
else
  seconds=$((${FUZZ_BUDGET_SECONDS:-600} / ${#targets[@]}))
  echo "shard $index/$total: ${targets[*]} (${seconds}s each)"
  RUSTC="$(rustup which --toolchain nightly rustc)"
  export RUSTC
  export RUSTUP_TOOLCHAIN=nightly
fi

failed=()
for target in "${targets[@]}"; do
  if [ "$mode" = smoke ]; then
    args=(test -p orbit-fuzz --test "$target")
  else
    args=(bolero test "$target" -p orbit-fuzz --toolchain nightly --engine libfuzzer --sanitizer "${FUZZ_SANITIZER:-address}"
      --corpus-dir "$tests/$target/corpus" --crashes-dir "$tests/$target/crashes" -T "${seconds}s")
  fi
  echo "+ cargo ${args[*]}"
  if [ -n "${CI:-}" ]; then
    cargo "${args[@]}" > "$tests/$target/fuzz.log" 2>&1 || failed+=("$target")
  else
    cargo "${args[@]}" || failed+=("$target")
  fi
done

if ((${#failed[@]})); then
  "$root/scripts/fuzz/corpus.sh" crashes "${failed[@]}"
fi

if [ "${FUZZ_CORPUS_PUSH:-}" = 1 ]; then
  "$root/scripts/fuzz/corpus.sh" push "${targets[@]}"
fi

if ((${#failed[@]})); then
  echo "fuzz targets failed: ${failed[*]}" >&2
  exit 1
fi

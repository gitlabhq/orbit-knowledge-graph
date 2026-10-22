#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
tests="$root/crates/fuzz/tests"

if (($# == 0)); then
  shopt -s nullglob
  for dir in "$tests"/fuzz_*/; do
    set -- "$@" "$(basename "$dir")"
  done
fi

for target in "$@"; do
  dir="$tests/$target"
  archive="$dir/corpus.tar.gz"
  if [ ! -f "$archive" ]; then
    echo "error: $archive is missing; seed the target and run scripts/fuzz/pack-corpus.sh $target" >&2
    exit 1
  fi
  if head -c 64 "$archive" | grep -q 'git-lfs'; then
    echo "error: $archive is a Git LFS pointer; run git lfs pull" >&2
    exit 1
  fi
  rm -rf "$dir/corpus"
  tar -xzf "$archive" -C "$dir"
  find "$dir/corpus" -name '._*' -delete
  echo "$target: $(find "$dir/corpus" -type f | wc -l | tr -d ' ') corpus inputs"
done

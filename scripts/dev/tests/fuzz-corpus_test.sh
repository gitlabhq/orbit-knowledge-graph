#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/scripts/fuzz" "$work/bin"
cp "$root"/scripts/fuzz/*.sh "$work/scripts/fuzz/"
tests="$work/crates/fuzz/tests"
for t in fuzz_a fuzz_b fuzz_c; do
  mkdir -p "$tests/$t/corpus"
  printf 'seed %s' "$t" > "$tests/$t/corpus/seed"
done
touch "$tests/fuzz_a/corpus/._seed"

bash "$work/scripts/fuzz/pack-corpus.sh" > /dev/null
first="$(cksum < "$tests/fuzz_a/corpus.tar.gz")"
touch "$tests/fuzz_a/corpus/seed"
bash "$work/scripts/fuzz/pack-corpus.sh" fuzz_a > /dev/null
test "$(cksum < "$tests/fuzz_a/corpus.tar.gz")" = "$first"
test "$(tar -tzf "$tests/fuzz_a/corpus.tar.gz" | sed 's#/$##' | sort | tr '\n' ' ')" = "corpus corpus/seed "

touch "$tests/fuzz_a/corpus/stale"
bash "$work/scripts/fuzz/unpack-corpus.sh" > /dev/null
test "$(ls -A "$tests/fuzz_a/corpus")" = "seed"
test "$(cat "$tests/fuzz_b/corpus/seed")" = "seed fuzz_b"

cp "$tests/fuzz_c/corpus.tar.gz" "$work/fuzz_c.tar.gz"
printf 'version https://git-lfs.github.com/spec/v1\noid sha256:0\nsize 1\n' > "$tests/fuzz_c/corpus.tar.gz"
if bash "$work/scripts/fuzz/unpack-corpus.sh" fuzz_c 2> "$work/err"; then exit 1; fi
grep -q 'git lfs pull' "$work/err"
cp "$work/fuzz_c.tar.gz" "$tests/fuzz_c/corpus.tar.gz"

mkdir -p "$tests/fuzz_d"
if bash "$work/scripts/fuzz/unpack-corpus.sh" fuzz_d 2> "$work/err"; then exit 1; fi
grep -q 'is missing' "$work/err"
rmdir "$tests/fuzz_d"

cat > "$work/bin/cargo" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$CARGO_CALLS"
[ "$4" != fuzz_a ]
EOF
chmod +x "$work/bin/cargo"

PATH="$work/bin:$PATH" CARGO_CALLS="$work/calls" FUZZ_BUDGET_SECONDS=60 CI_NODE_INDEX=2 CI_NODE_TOTAL=2 \
  bash "$work/scripts/fuzz/deep.sh" > /dev/null
test "$(cat "$work/calls")" = "+nightly bolero test fuzz_b -p orbit-fuzz --engine libfuzzer --sanitizer address -T 60s"

rm "$work/calls"
if PATH="$work/bin:$PATH" CARGO_CALLS="$work/calls" FUZZ_BUDGET_SECONDS=60 CI_NODE_INDEX=1 CI_NODE_TOTAL=2 \
  bash "$work/scripts/fuzz/deep.sh" > /dev/null 2> "$work/err"; then
  exit 1
fi
test "$(cut -d' ' -f4,12 "$work/calls" | tr '\n' ' ')" = "fuzz_a 30s fuzz_c 30s "
grep -q 'fuzz targets failed: fuzz_a' "$work/err"

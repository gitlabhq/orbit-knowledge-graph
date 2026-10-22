#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

mkdir -p "$work/scripts/fuzz" "$work/bin" "$work/archive"
cp "$root"/scripts/fuzz/corpus.sh "$root"/scripts/fuzz/run.sh "$work/scripts/fuzz/"
tests="$work/crates/fuzz/tests"
mkdir -p "$tests/fuzz_a" "$tests/fuzz_b" "$tests/fuzz_c"
cat > "$work/crates/fuzz/seeds.yaml" <<'EOF'
fuzz_a:
  - labels: [one]
    input: seed a
  - labels: [two, lines]
    input: |
      line one
      line two
fuzz_b:
  - labels: [one]
    input: seed b
EOF
corpus="$work/scripts/fuzz/corpus.sh"
run="$work/scripts/fuzz/run.sh"
unset FUZZ_CORPUS_TOKEN FUZZ_CORPUS_JWT FUZZ_CORPUS_WIF_PROVIDER

mkdir -p "$tests/fuzz_b/corpus" && touch "$tests/fuzz_b/corpus/stale"
bash "$corpus" pull > /dev/null
test "$(cat "$tests/fuzz_a/corpus/seed-0")" = "seed a"
printf 'line one\nline two\n' | cmp - "$tests/fuzz_a/corpus/seed-1"
test "$(ls -A "$tests/fuzz_b/corpus")" = "seed-0"
test -z "$(ls -A "$tests/fuzz_c/corpus")"

if bash "$corpus" push fuzz_a 2> "$work/err"; then exit 1; fi
grep -q 'push needs' "$work/err"

reject() {
  tar -czf "$work/bad.tar.gz" -C "$work/archive" "$@"
  if bash "$corpus" check "$work/bad.tar.gz" 2> "$work/err"; then exit 1; fi
  grep -q 'not plain files' "$work/err"
  rm -rf "${work:?}/archive"/*
}

printf x > "$work/archive/a"
tar -czf "$work/good.tar.gz" -C "$work/archive" .
bash "$corpus" check "$work/good.tar.gz"
ln -s /etc "$work/archive/link" && reject .
printf x > "$work/archive/a" && ln "$work/archive/a" "$work/archive/b" && reject .
mkdir "$work/archive/sub" && touch "$work/archive/sub/x" && reject .
mkdir "$work/archive/many" && (cd "$work/archive/many" && seq -f '%040g' 4000 | xargs touch)
touch "$work/archive/bad name" && reject "./bad name" -C "$work/archive/many" .

cat > "$work/bin/cargo" <<'EOF'
#!/usr/bin/env bash
echo "$*" >> "$CARGO_CALLS"
case "$*" in
  "test -p orbit-fuzz --test fuzz_b" | "bolero test fuzz_a "*)
    [ "$1" = bolero ] && mkdir -p "${15}" && printf boom > "${15}/crash-abc"
    echo "SECRET-FUZZER-OUTPUT"
    exit 1 ;;
esac
EOF
cat > "$work/bin/rustup" <<'EOF'
#!/usr/bin/env bash
echo /nightly/rustc
EOF
chmod +x "$work/bin/cargo" "$work/bin/rustup"
export CI=1 PATH="$work/bin:$PATH" CARGO_CALLS="$work/cargo"

FUZZ_BUDGET_SECONDS=60 CI_NODE_INDEX=2 CI_NODE_TOTAL=2 bash "$run" deep > /dev/null
test "$(cat "$work/cargo")" = "bolero test fuzz_b -p orbit-fuzz --toolchain nightly --engine libfuzzer --sanitizer address --corpus-dir $tests/fuzz_b/corpus --crashes-dir $tests/fuzz_b/crashes -T 60s"

rm "$work/cargo"
if FUZZ_BUDGET_SECONDS=60 CI_NODE_INDEX=1 CI_NODE_TOTAL=2 bash "$run" deep > "$work/out" 2>&1; then exit 1; fi
test "$(cut -d' ' -f3,17 "$work/cargo" | tr '\n' ' ')" = "fuzz_a 30s fuzz_c 30s "
grep -q 'fuzz targets failed: fuzz_a' "$work/out"
grep -q 'fuzz_a: crashes and fuzz.log kept' "$work/out"
if grep -q SECRET-FUZZER-OUTPUT "$work/out"; then exit 1; fi
grep -q SECRET-FUZZER-OUTPUT "$tests/fuzz_a/fuzz.log"
test -f "$tests/fuzz_a/crashes/crash-abc"

rm "$work/cargo"
if bash "$run" smoke > "$work/out" 2>&1; then exit 1; fi
test "$(tr '\n' '|' < "$work/cargo")" = "test -p orbit-fuzz --no-run|test -p orbit-fuzz --test fuzz_a|test -p orbit-fuzz --test fuzz_b|test -p orbit-fuzz --test fuzz_c|"
grep -q 'fuzz targets failed: fuzz_b' "$work/out"
if grep -q SECRET-FUZZER-OUTPUT "$work/out"; then exit 1; fi
grep -q SECRET-FUZZER-OUTPUT "$tests/fuzz_b/fuzz.log"

if bash "$run" bogus 2> "$work/err"; then exit 1; fi
grep -q 'usage: run.sh' "$work/err"
if bash "$corpus" bogus 2> "$work/err"; then exit 1; fi
grep -q 'usage: corpus.sh' "$work/err"

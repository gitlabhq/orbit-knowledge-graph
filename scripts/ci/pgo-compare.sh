#!/usr/bin/env bash
set -Eeuo pipefail

# PGO prototype: build the orbit CLI with and without PGO for one Linux target
# (PLATFORM/ARCH/LIBC as in scripts/release/build-local-cli.sh), compare them on
# held-out repos on this runner, and write pgo-report/report.json.
#
# Env: PGO_RUNS (wall-time runs per variant and repo, default 7),
#      PGO_CPUS (taskset CPU list for wall-time runs, default 0-3; empty for none),
#      PGO_IR_RUNS (single-CPU callgrind runs per variant and repo, default 2; 0 to skip),
#      PGO_MIN_CONTROL_REDUCTION (fail if instructions on the trained okhttp repo drop
#      by less than this percent, default 5).

export PLATFORM="${PLATFORM:-linux}"
RUNS="${PGO_RUNS:-7}"
IR_RUNS="${PGO_IR_RUNS:-2}"
MIN_CONTROL_REDUCTION="${PGO_MIN_CONTROL_REDUCTION:-5}"
PIN=()
[ -n "${PGO_CPUS-0-3}" ] && command -v taskset >/dev/null && PIN=(taskset -c "${PGO_CPUS-0-3}")
REPORT_DIR="${PGO_REPORT_DIR:-$PWD/pgo-report}"
export PGO_REPORT_DIR="$REPORT_DIR"
WORK="${PGO_COMPARE_DIR:-$PWD/target/pgo-compare}"
STAGES="$REPORT_DIR/stages.tsv"
FAILED_STAGE=""

rm -rf "$REPORT_DIR" "$WORK"
mkdir -p "$REPORT_DIR" "$WORK/corpus" "$WORK/bin"
: >"$REPORT_DIR/compare.tsv"

# name url sha role. Training repos are indexed to collect the profile; held-out
# repos are only used to measure. okhttp doubles as the positive control.
CORPUS=(
    "okhttp https://github.com/square/okhttp.git 374def39eb276bf0ad724dc71c589241851f5b16 train"
    "django https://github.com/django/django.git 9e7cc2b628fe8fd3895986af9b7fc9525034c1b0 train"
    "laravel https://github.com/laravel/framework.git 6089f679d6d29e6071a6448ed5e96de02e57fedb heldout"
    "flask https://github.com/pallets/flask.git 735a4701d6d5e848241e7d7535db898efb62d400 heldout"
)

now() { date +%s; }

timed() {
    local stage="$1" started status=ok
    shift
    started=$(now)
    "$@" || status=failed
    printf '%s\t%s\t%s\n' "$stage" "$(($(now) - started))" "$status" >>"$STAGES"
    [ "$status" = ok ] || {
        FAILED_STAGE="$stage"
        return 1
    }
}

fetch_corpus() {
    local name url sha role entry
    for entry in "${CORPUS[@]}"; do
        read -r name url sha role <<<"$entry"
        git init -q "$WORK/corpus/$name" || return 1
        git -C "$WORK/corpus/$name" fetch -q --depth 1 "$url" "$sha" || return 1
        git -C "$WORK/corpus/$name" checkout -q FETCH_HEAD || return 1
        [ "$role" = train ] && TRAIN_DIRS+="${TRAIN_DIRS:+ }$WORK/corpus/$name"
    done
    mkdir "$WORK/corpus/knowledge-graph" || return 1
    git archive HEAD | tar -x -C "$WORK/corpus/knowledge-graph" || return 1
    return 0
}

extract_orbit() {
    mkdir -p "$WORK/bin/$2" || return 1
    tar -xzf "$1" -C "$WORK/bin/$2" orbit || return 1
}

# Same-depth target dirs keep build-script OUT_DIR paths alike in both variants.
build_base() {
    CARGO_TARGET_DIR="$WORK/base" RUSTC_WRAPPER="" ./scripts/release/build-local-cli.sh || return 1
    extract_orbit orbit-cli-linux-*.tar.gz base || return 1
    rm -f orbit-cli-linux-*.tar.gz
}

# What today's release job does: same build with sccache, if the template set it up.
build_base_sccache() {
    CARGO_TARGET_DIR="$WORK/base-sccache" ./scripts/release/build-local-cli.sh || return 1
    rm -f orbit-cli-linux-*.tar.gz
}

build_pgo() {
    RUSTC_WRAPPER="" PGO=1 PGO_TRAIN_DIRS="$TRAIN_DIRS" PGO_WORK_DIR="$WORK/pgo" \
        ./scripts/release/build-local-cli.sh || return 1
    extract_orbit orbit-cli-linux-*.tar.gz pgo || return 1
    mv orbit-cli-linux-*.tar.gz "$REPORT_DIR/"
}

# Prints the indexed file count; a crash or non-zero exit fails the sample.
index_once() {
    local out
    rm -rf "$WORK/data"
    out=$(ORBIT_TELEMETRY_ENABLED=false ORBIT_DATA_DIR="$WORK/data" "${PIN[@]}" "$WORK/bin/$1/orbit" workspace index "$2" 2>/dev/null) || return 1
    sed -n 's/^ *"files": \([0-9]*\),*$/\1/p' <<<"$out" | head -1
}

record() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" >>"$REPORT_DIR/compare.tsv"; }

measure_wall() {
    local repo="$1" variant started files
    local -A seen=()
    for _ in $(seq "$RUNS"); do
        for variant in base pgo; do
            started=$(date +%s.%N)
            files=$(index_once "$variant" "$WORK/corpus/$repo") || { echo "index failed: $variant $repo" >&2; return 1; }
            record wall "$repo" "$variant" "$(awk -v a="$started" -v b="$(date +%s.%N)" 'BEGIN { print b - a }')"
            [ -n "$files" ] && [ "$files" -gt 0 ] || { echo "no files indexed: $variant $repo" >&2; return 1; }
            seen[$variant]=$files
        done
        [ "${seen[base]}" = "${seen[pgo]}" ] || { echo "file counts differ on $repo: ${seen[base]} vs ${seen[pgo]}" >&2; return 1; }
    done
}

# Single CPU, so the instruction count does not depend on thread scheduling.
measure_ir() {
    local repo="$1" variant out
    for _ in $(seq "$IR_RUNS"); do
        for variant in base pgo; do
            rm -rf "$WORK/data"
            out=$(ORBIT_TELEMETRY_ENABLED=false ORBIT_DATA_DIR="$WORK/data" taskset -c 0 valgrind --tool=callgrind \
                --callgrind-out-file=/dev/null "$WORK/bin/$variant/orbit" workspace index "$WORK/corpus/$repo" 2>&1 >/dev/null) || return 1
            out=$(sed -n 's/.*Collected : \([0-9]*\).*/\1/p' <<<"$out")
            [ -n "$out" ] || return 1
            record ir "$repo" "$variant" "$out"
        done
    done
}

write_report() {
    local status="$1"
    jq -n \
        --arg target "$(PRINT_TARGET=1 ./scripts/release/build-local-cli.sh | sed -n 's/^TARGET=//p')" \
        --arg status "$status" --arg failed_stage "$FAILED_STAGE" \
        --arg runner "$(uname -m) $(nproc) cpus" --arg commit "${CI_COMMIT_SHA:-$(git rev-parse HEAD)}" \
        --argjson min_control "$MIN_CONTROL_REDUCTION" \
        --rawfile stages "$STAGES" --rawfile compare "$REPORT_DIR/compare.tsv" '
        def rows($s): $s | split("\n") | map(select(length > 0) | split("\t"));
        def med: sort | .[(length / 2 | floor)];
        def pct($b; $p): (($b - $p) / $b * 100 | . * 10 | round / 10);
        def by_repo($kind): rows($compare) | map(select(.[0] == $kind)) | group_by(.[1])
          | map({key: .[0][1], value: (group_by(.[2]) | map({key: .[0][2], value: map(.[3] | tonumber)}) | from_entries)})
          | from_entries;
        {
          target: $target, status: $status, failed_stage: $failed_stage, runner: $runner, commit: $commit,
          stage_seconds: (rows($stages) | map({key: .[0], value: {seconds: (.[1] | tonumber), status: .[2]}}) | from_entries),
          wall_seconds: (by_repo("wall") | map_values(
            (map_values({n: length, min: min, median: med, max: max}))
            | . + {median_speedup_pct: pct(.base.median; .pgo.median), min_speedup_pct: pct(.base.min; .pgo.min),
                   ranges_overlap: (.base.min <= .pgo.max and .pgo.min <= .base.max)})),
          instructions: (by_repo("ir") | map_values(
            (map_values({n: length, min: min, max: max}))
            | . + {reduction_pct: pct(.base.min; .pgo.min)})),
          control: {repo: "okhttp", min_instruction_reduction_pct: $min_control}
        }' >"$REPORT_DIR/report.json"
}

# The trained repo must speed up; otherwise the profile did not apply to the optimized build.
control_applied() {
    jq -e --argjson min "$MIN_CONTROL_REDUCTION" '.instructions.okhttp.reduction_pct >= $min' "$REPORT_DIR/report.json" >/dev/null
}

TRAIN_DIRS=""
trap 'write_report failed' ERR
timed corpus-fetch fetch_corpus
[ -n "${RUSTC_WRAPPER:-}" ] && timed base-build-sccache build_base_sccache
timed base-build build_base
build_pgo || { FAILED_STAGE=$(awk -F'\t' '$3 == "failed" { print $1 }' "$STAGES" | tail -n 1); false; }
for repo in okhttp laravel flask knowledge-graph; do
    timed "wall-$repo" measure_wall "$repo"
done
if [ "$IR_RUNS" -gt 0 ]; then
    for repo in okhttp flask; do
        timed "ir-$repo" measure_ir "$repo"
    done
fi
trap - ERR
write_report ok
if [ "$IR_RUNS" -gt 0 ] && ! control_applied; then
    write_report profile-not-applied
    cat "$REPORT_DIR/report.json"
    echo "error: instructions on the trained okhttp repo dropped by less than ${MIN_CONTROL_REDUCTION}%; the profile did not apply" >&2
    exit 1
fi
cat "$REPORT_DIR/report.json"

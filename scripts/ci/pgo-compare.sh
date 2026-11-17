#!/usr/bin/env bash
set -Eeuo pipefail

# PGO prototype: build the orbit CLI with and without PGO for one Linux target
# (PLATFORM/ARCH/LIBC as in scripts/release/build-local-cli.sh), compare them on
# held-out repos on this runner, and write pgo-report/report.json.
#
# Env: PGO_RUNS (wall-time runs per variant and repo, default 7),
#      PGO_CPUS (taskset CPU list for the measurements, default 0-3; empty for none),
#      PGO_IR_RUNS (callgrind runs per variant, default 2; needs valgrind, 0 to skip).

export PLATFORM="${PLATFORM:-linux}"
RUNS="${PGO_RUNS:-7}"
IR_RUNS="${PGO_IR_RUNS:-2}"
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
# repos are only used to measure.
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
    local name url sha role
    for entry in "${CORPUS[@]}"; do
        read -r name url sha role <<<"$entry"
        git init -q "$WORK/corpus/$name"
        git -C "$WORK/corpus/$name" fetch -q --depth 1 "$url" "$sha"
        git -C "$WORK/corpus/$name" checkout -q FETCH_HEAD
        [ "$role" = train ] && TRAIN_DIRS+="${TRAIN_DIRS:+ }$WORK/corpus/$name"
    done
    mkdir "$WORK/corpus/knowledge-graph"
    git archive HEAD | tar -x -C "$WORK/corpus/knowledge-graph"
}

extract_orbit() {
    mkdir -p "$WORK/bin/$2"
    tar -xzf "$1" -C "$WORK/bin/$2" orbit
}

build_base() {
    RUSTC_WRAPPER="" ./scripts/release/build-local-cli.sh
    extract_orbit orbit-cli-linux-*.tar.gz base
    rm -f orbit-cli-linux-*.tar.gz
}

build_pgo() {
    RUSTC_WRAPPER="" PGO=1 PGO_TRAIN_DIRS="$TRAIN_DIRS" PGO_WORK_DIR="$WORK/pgo" ./scripts/release/build-local-cli.sh
    extract_orbit orbit-cli-linux-*.tar.gz pgo
    mv orbit-cli-linux-*.tar.gz "$REPORT_DIR/"
}

index_once() {
    ORBIT_TELEMETRY_ENABLED=false ORBIT_DATA_DIR="$WORK/data" \
        "${PIN[@]}" "$WORK/bin/$1/orbit" workspace index "$2" >/dev/null 2>&1
}

median() { sort -n | awk '{ a[NR] = $1 } END { print a[int((NR + 1) / 2)] }'; }

measure_wall() {
    local repo="$1" variant i started
    local -A samples=([base]="" [pgo]="")
    for i in $(seq "$RUNS"); do
        for variant in base pgo; do
            rm -rf "$WORK/data"
            started=$(date +%s.%N)
            index_once "$variant" "$WORK/corpus/$repo"
            samples[$variant]+="$(awk -v a="$started" -v b="$(date +%s.%N)" 'BEGIN { print b - a }') "
        done
    done
    for variant in base pgo; do
        printf 'wall\t%s\t%s\t%s\n' "$repo" "$variant" "$(tr ' ' '\n' <<<"${samples[$variant]}" | grep . | median)" \
            >>"$REPORT_DIR/compare.tsv"
    done
}

measure_ir() {
    local repo="$1" variant i out
    for _ in $(seq "$IR_RUNS"); do
        for variant in base pgo; do
            rm -rf "$WORK/data"
            out=$(ORBIT_TELEMETRY_ENABLED=false ORBIT_DATA_DIR="$WORK/data" "${PIN[@]}" valgrind --tool=callgrind \
                --callgrind-out-file=/dev/null "$WORK/bin/$variant/orbit" workspace index "$WORK/corpus/$repo" 2>&1 >/dev/null |
                sed -n 's/.*Collected : \([0-9]*\).*/\1/p')
            printf 'ir\t%s\t%s\t%s\n' "$repo" "$variant" "$out" >>"$REPORT_DIR/compare.tsv"
        done
    done
}

write_report() {
    local status="$1"
    jq -n \
        --arg target "$(PRINT_TARGET=1 ./scripts/release/build-local-cli.sh | sed -n 's/^TARGET=//p')" \
        --arg status "$status" --arg failed_stage "$FAILED_STAGE" \
        --arg runner "$(uname -m) $(nproc) cpus" --arg commit "${CI_COMMIT_SHA:-$(git rev-parse HEAD)}" \
        --rawfile stages "$STAGES" --rawfile compare "$REPORT_DIR/compare.tsv" '
        def rows($s): $s | split("\n") | map(select(length > 0) | split("\t"));
        def med: sort | .[(length / 2 | floor)];
        {
          target: $target, status: $status, failed_stage: $failed_stage, runner: $runner, commit: $commit,
          stage_seconds: (rows($stages) | map({key: .[0], value: {seconds: (.[1] | tonumber), status: .[2]}}) | from_entries),
          wall_seconds: (rows($compare) | map(select(.[0] == "wall"))
            | group_by(.[1]) | map({key: .[0][1], value: (map({key: .[2], value: (.[3] | tonumber)}) | from_entries)})
            | from_entries
            | map_values(. + {speedup_pct: (if .base and .pgo then ((.base - .pgo) / .base * 100 | . * 10 | round / 10) else null end)})),
          instructions: (rows($compare) | map(select(.[0] == "ir" and (.[3] | length > 0)))
            | group_by(.[1]) | map({key: .[0][1], value: (
                group_by(.[2]) | map({key: .[0][2], value: (map(.[3] | tonumber) | {min: min, max: max, runs: length})}) | from_entries
              | . + {reduction_pct: (if .base and .pgo then ((.base.min - .pgo.min) / .base.min * 100 | . * 10 | round / 10) else null end)})})
            | from_entries)
        }' >"$REPORT_DIR/report.json"
}

TRAIN_DIRS=""
trap 'write_report failed' ERR
timed corpus-fetch fetch_corpus
timed base-build build_base
build_pgo || { FAILED_STAGE=$(awk -F'\t' '$3 == "failed" { print $1 }' "$STAGES" | tail -1); false; }
for repo in laravel flask knowledge-graph; do
    timed "wall-$repo" measure_wall "$repo"
done
if [ "$IR_RUNS" -gt 0 ] && command -v valgrind >/dev/null; then
    timed ir-flask measure_ir flask
fi
trap - ERR
write_report ok
cat "$REPORT_DIR/report.json"

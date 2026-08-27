#!/usr/bin/env bash
# Measure each candidate change on its own against one baseline, over one
# seeded datalake.
#
# Usage:
#   memprofile-sdlc-isolate.sh <baseline-binary> <label>=<binary>... [-- <profiler flags>]
#
# Every run gets an empty graph database, writes into it, has its output
# fingerprinted, and drops it before the next run starts. Nothing is retained
# between runs: a graph database left in place merges tens of millions of rows
# in the background, and then the server, not the indexer, is what runs out of
# memory — which silently truncates the next arm and makes its peak meaningless.
#
# The baseline's fingerprints are dumped to a file so its database can be
# dropped too, and every arm is compared against that file.
#
# Env: REPEATS (2), CH_PASSWORD (memprofile), CH_URL, ROWS_PER_TABLE, OUT_DIR

set -euo pipefail

BASELINE="${1:?baseline binary}"
shift

declare -a ARM_LABELS=()
declare -a ARM_BINARIES=()
while [[ $# -gt 0 && "$1" != "--" ]]; do
  [[ "$1" == *=* ]] || { echo "expected <label>=<binary>, got '$1'" >&2; exit 1; }
  ARM_LABELS+=("${1%%=*}")
  ARM_BINARIES+=("${1#*=}")
  shift
done
[[ "${1:-}" == "--" ]] && shift

REPEATS="${REPEATS:-2}"
CH_PASSWORD="${CH_PASSWORD:-memprofile}"
CH_URL="${CH_URL:-http://localhost:18123}"
ROWS_PER_TABLE="${ROWS_PER_TABLE:-2000000}"
OUT_DIR="${OUT_DIR:-.memprofile/sdlc}"
HERE="$(cd "$(dirname "$0")" && pwd)"
FINGERPRINT="$OUT_DIR/baseline.fingerprint.json"

export CH_PASSWORD
mkdir -p "$OUT_DIR"

ch() {
  curl -sf "$CH_URL/?user=default&password=$CH_PASSWORD" --data-binary "$1"
}

# Caches are dropped so every run reads the datalake from disk, and no arm
# benefits from the previous one having warmed it.
reset_clickhouse() {
  ch "SYSTEM DROP MARK CACHE" >/dev/null
  ch "SYSTEM DROP UNCOMPRESSED CACHE" >/dev/null
  ch "SYSTEM DROP QUERY CACHE" >/dev/null || true
}

run_one() {
  local binary="$1" label="$2" database="$3"
  shift 3
  ch "DROP DATABASE IF EXISTS $database" >/dev/null
  reset_clickhouse
  echo "== $label"
  RUST_LOG="${RUST_LOG:-warn}" "$binary" \
    --skip-seed \
    --url "$CH_URL" \
    --rows-per-table "$ROWS_PER_TABLE" \
    --label "$label" \
    --out-dir "$OUT_DIR" \
    --graph-database "$database" \
    "$@" >/dev/null
}

measure_arm() {
  local binary="$1" arm="$2"
  shift 2
  for run in $(seq 1 "$REPEATS"); do
    local database="sdlc_graph_${arm}"
    run_one "$binary" "${arm}-${run}" "$database" "$@"
    if [[ "$run" == "1" ]]; then
      if [[ "$arm" == "baseline" ]]; then
        "$HERE/memprofile-verify-output.py" --url "$CH_URL" --password "$CH_PASSWORD" \
          --baseline-db "$database" --dump "$FINGERPRINT" >/dev/null
        echo "   fingerprint dumped"
      else
        local verdict
        verdict=$("$HERE/memprofile-verify-output.py" --url "$CH_URL" \
          --password "$CH_PASSWORD" --candidate-db "$database" \
          --expect "$FINGERPRINT")
        printf '%s\n' "$verdict" > "$OUT_DIR/${arm}.identity.txt"
        if grep -q "byte-identical" <<<"$verdict"; then
          echo "   output identical"
        else
          echo "   OUTPUT DIFFERS (see $OUT_DIR/${arm}.identity.txt)"
        fi
      fi
    fi
    ch "DROP DATABASE IF EXISTS $database" >/dev/null
  done
}

measure_arm "$BASELINE" baseline "$@"
for index in "${!ARM_LABELS[@]}"; do
  measure_arm "${ARM_BINARIES[$index]}" "${ARM_LABELS[$index]}" "$@"
done

echo
python3 - "$OUT_DIR" "$REPEATS" "${ARM_LABELS[@]}" <<'PY'
import json, statistics, sys

out_dir, repeats, arms = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
GIB = 2 ** 30


def load(arm):
    reports = []
    for run in range(1, repeats + 1):
        try:
            reports.append(json.load(open(f"{out_dir}/{arm}-{run}.json")))
        except FileNotFoundError:
            pass
    return reports


def median_of(reports, path, scale):
    values = []
    for report in reports:
        value = report
        for key in path:
            value = value[key]
        values.append(value / scale)
    return statistics.median(values) if values else float("nan")


def identity(arm):
    try:
        text = open(f"{out_dir}/{arm}.identity.txt").read()
    except FileNotFoundError:
        return "-"
    return "yes" if "byte-identical" in text else "**NO**"


METRICS = [
    ("peak footprint GiB", ("peaks", "exact_max_footprint"), GIB, "{:.2f}"),
    ("peak live GiB", ("peaks", "exact_max_alloc_live"), GIB, "{:.2f}"),
    ("peak commit GiB", ("peaks", "exact_max_commit"), GIB, "{:.2f}"),
    ("alloc total GiB", ("alloc", "total_alloc_bytes"), GIB, "{:.0f}"),
    ("allocs M", ("alloc", "total_allocs"), 1e6, "{:.0f}"),
    ("namespace s", ("namespace_ms",), 1000, "{:.0f}"),
    ("graph rows M", ("graph_rows",), 1e6, "{:.1f}"),
]

baseline = load("baseline")
if not baseline:
    sys.exit("no baseline reports found")

failures = {arm: sum(len(r["failures"]) for r in load(arm)) for arm in ["baseline"] + arms}
for arm, count in failures.items():
    if count:
        print(f"WARNING: {arm} had {count} handler failure(s); its peak is not comparable")

header = ["metric", "baseline"] + [f"{arm}" for arm in arms]
print("| " + " | ".join(header) + " |")
print("|" + "---|" * (len(header) - 1) + "---:|")

for name, path, scale, fmt in METRICS:
    base = median_of(baseline, path, scale)
    cells = [name, fmt.format(base)]
    for arm in arms:
        reports = load(arm)
        if not reports:
            cells.append("-")
            continue
        value = median_of(reports, path, scale)
        delta = (value - base) / base * 100 if base else 0.0
        cells.append(f"{fmt.format(value)} ({delta:+.1f}%)")
    print("| " + " | ".join(cells) + " |")

print("| output identical | (reference) | " + " | ".join(identity(arm) for arm in arms) + " |")
PY

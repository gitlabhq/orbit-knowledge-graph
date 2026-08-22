#!/usr/bin/env bash
# Poll `footprint -p` for the running profiler and append each snapshot to a log.
#
# Usage: memprofile-footprint.sh <out-file> [interval-seconds] [process-name]
#
# The allocator counter only sees requested bytes. This is the other half:
# per-VM-region dirty pages, so thread stacks, page tables and allocator
# metadata show up as themselves instead of as an unexplained gap.

set -uo pipefail

OUT="${1:?usage: memprofile-footprint.sh <out-file> [interval] [process-name]}"
INTERVAL="${2:-1}"
PROC="${3:-code-index-profiler}"

: > "$OUT"
while true; do
  PID="$(pgrep -x "$PROC" | head -1)"
  if [[ -z "$PID" ]]; then
    if [[ -s "$OUT" ]]; then
      exit 0
    fi
    sleep 0.2
    continue
  fi
  {
    echo "=== t=$(date +%s.%N) pid=$PID"
    /usr/bin/footprint -p "$PID" 2>/dev/null
  } >> "$OUT"
  sleep "$INTERVAL"
done

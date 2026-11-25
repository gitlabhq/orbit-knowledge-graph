#!/usr/bin/env bash

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

MAX_AGE_HOURS="${1:-2}"
CUTOFF=$(date -u -d "${MAX_AGE_HOURS} hours ago" +%s 2>/dev/null \
      || date -u -v-"${MAX_AGE_HOURS}"H +%s)

CURRENT_PREFIX="e2e-${E2E_SHA:-none}-"

$KC get ns -o jsonpath='{range .items[*]}{.metadata.name} {.metadata.creationTimestamp}{"\n"}{end}' 2>/dev/null \
| while read -r name ts; do
  [[ "$name" == e2e-* ]] || continue
  [[ "$name" == "$CURRENT_PREFIX"* ]] && continue

  created=$(date -u -d "$ts" +%s 2>/dev/null \
         || date -u -jf "%Y-%m-%dT%H:%M:%SZ" "$ts" +%s 2>/dev/null \
         || true)

  if [ -z "$created" ] || ! [[ "$created" =~ ^[0-9]+$ ]]; then
    log "WARNING: could not parse timestamp for $name ($ts), skipping"
    continue
  fi

  if [ "$created" -lt "$CUTOFF" ]; then
    log "Deleting stale namespace: $name (created $ts)"
    $KC delete ns "$name" --wait=false 2>/dev/null || true
  fi
done

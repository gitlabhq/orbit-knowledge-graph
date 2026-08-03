#!/usr/bin/env bash
# Delete e2e namespaces older than a threshold to reclaim cluster capacity.
#
# The CI after_script teardown has a 5-minute hard timeout and can be killed
# by the runner, leaving namespaces (and their pods) behind indefinitely.
# On the 3-node e2e cluster, 2-3 orphaned runs exhaust all capacity.
#
# Usage: cleanup-stale-namespaces.sh [max-age-hours]
#   max-age-hours: namespaces older than this are deleted (default: 2)

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

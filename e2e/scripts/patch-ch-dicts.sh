#!/usr/bin/env bash

set -euo pipefail

# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"

DICTS=(
  "datalake.namespace_traversal_paths_dict"
  "datalake.project_traversal_paths_dict"
  "datalake.organization_traversal_paths_dict"
)

log "Patching CH traversal-path dictionaries to LAYOUT(DIRECT)"

CH_POD=clickhouse-0
ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
    sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"'
}

log "Waiting for traversal-path dictionaries to exist"
for _ in $(seq 1 120); do
  if ch_query "EXISTS DICTIONARY datalake.namespace_traversal_paths_dict" 2>/dev/null | grep -q '^1$'; then
    break
  fi
  sleep 5
done

DEFAULT_PASS=$($KC exec -n "$NS_CH" "$CH_POD" -- printenv CLICKHOUSE_PASSWORD)
[[ -n "$DEFAULT_PASS" ]] || { log "could not read CLICKHOUSE_PASSWORD from pod"; exit 1; }

for dict in "${DICTS[@]}"; do
  log "Patching $dict"
  ddl=$(ch_query "SHOW CREATE DICTIONARY $dict FORMAT TSVRaw" 2>/dev/null || true)
  if [[ -z "$ddl" ]]; then
    log "  $dict not found, skipping"
    continue
  fi
  patched=$(printf '%s' "$ddl" \
    | sed -E 's/^CREATE DICTIONARY/CREATE OR REPLACE DICTIONARY/' \
    | sed -E "s|LAYOUT\\(COMPLEX_KEY_CACHE\\([^()]*\\)\\)|LAYOUT(COMPLEX_KEY_DIRECT())|" \
    | sed -E "s|LAYOUT\\(CACHE\\([^()]*\\)\\)|LAYOUT(DIRECT())|" \
    | sed -E "s|LIFETIME\\([^)]*\\)||" \
    | sed -E "s|USER '[^']+' PASSWORD '\\[HIDDEN\\]'|USER 'default' PASSWORD '$DEFAULT_PASS'|")
  ch_query "$patched" >/dev/null
  if [[ -n "${E2E_CH_HOST:-}" ]]; then
    layout=$(ch_query "SELECT type FROM system.dictionaries WHERE database || '.' || name = '$dict'" 2>/dev/null || true)
    case "$layout" in
      *Direct*|*direct*) ;;
      *) log "  WARNING: $dict layout is '$layout', not DIRECT — cache race window remains" ;;
    esac
  else
    if ! ch_query "SELECT dictGetOrDefault('$dict', 'traversal_path', toUInt64(0), '')" >/dev/null; then
      log "  ERROR: $dict probe lookup failed after patch"
      exit 1
    fi
    layout=$(ch_query "SELECT type FROM system.dictionaries WHERE database || '.' || name = '$dict'" 2>/dev/null || true)
    case "$layout" in
      *Direct*|*direct*) log "  $dict layout: $layout" ;;
      *)
        log "  ERROR: $dict layout is '$layout', not DIRECT — cache race window remains"
        exit 1
        ;;
    esac
  fi
done

log "CH traversal-path dictionaries patched"

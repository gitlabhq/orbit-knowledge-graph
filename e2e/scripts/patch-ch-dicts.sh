#!/usr/bin/env bash
# Recreate the traversal-path dictionaries with LAYOUT(DIRECT). The upstream
# CACHE layout can cache a routes-vs-namespaces CDC race miss as '0/',
# poisoning every insert for that namespace until the entry expires, and the
# reconciler never repairs '0/' rows (it only targets empty paths). DIRECT
# removes the caching; the tiny e2e dataset makes per-lookup queries free.
# Tracked in https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/483

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

# 600s: this may launch before the ClickHouse pod even exists.
log "Waiting for traversal-path dictionaries to exist"
for _ in $(seq 1 120); do
  if ch_query "EXISTS DICTIONARY datalake.namespace_traversal_paths_dict" 2>/dev/null | grep -q '^1$'; then
    break
  fi
  sleep 5
done

# Pull the default-user password from the pod env so we can rewrite the
# SOURCE block of each dict. The original SOURCE uses USER 'gitlab' and
# SHOW CREATE redacts its password as literal `[HIDDEN]`; sending that DDL
# back creates a dict that looks LOADED but fails every lookup with
# AUTHENTICATION_FAILED. We swap the source user to `default` (which
# already has full perms) and inject its real password.
DEFAULT_PASS=$($KC exec -n "$NS_CH" "$CH_POD" -- printenv CLICKHOUSE_PASSWORD)
[[ -n "$DEFAULT_PASS" ]] || { log "could not read CLICKHOUSE_PASSWORD from pod"; exit 1; }

# SHOW CREATE round-trip preserves SOURCE/columns; DIRECT layouts reject
# LIFETIME. TSVRaw is required — default TSV escapes newlines inside the
# embedded SOURCE QUERY, breaking the DDL.
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
  layout=$(ch_query "SELECT type FROM system.dictionaries WHERE database || '.' || name = '$dict'" 2>/dev/null || true)
  case "$layout" in
    *Direct*|*direct*) ;;
    *) log "  WARNING: $dict layout is '$layout', not DIRECT — cache race window remains" ;;
  esac
done

log "CH traversal-path dictionaries patched"

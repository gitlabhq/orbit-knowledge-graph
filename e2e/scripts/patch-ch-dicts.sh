#!/usr/bin/env bash
# Recreate the traversal-path dictionaries with LAYOUT(DIRECT) so no lookup
# is ever cached: the routes-vs-namespaces race in siphon CDC can otherwise
# cache a miss as '0/' and poison every insert for that namespace until the
# cache entry expires (upstream LIFETIME 60-300s).
#
# Why we need this in e2e
# -----------------------
# GitLab's main.sql defines `namespace_traversal_paths_dict` (and siblings)
# with `LAYOUT(CACHE) LIFETIME(MIN 60 MAX 300)`. The cache layout fetches per
# key on miss and caches the result — including misses — for the whole
# lifetime window.
#
# In e2e, project creation produces nearly-simultaneous CDC events for
# `namespaces` (the new project_namespace) and `routes` (the project's URL
# entry). Both tables go to separate NATS streams, processed in parallel by
# the siphon consumer. If the routes worker wins by even a few milliseconds,
# inserting siphon_routes runs the dictGet on the not-yet-known namespace_id,
# caches '0/' as the answer, and any subsequent INSERT for the same
# namespace_id (including the test's issue creation) inherits that '0/'.
# Without intervention the row stays invisible to GKG until the cell expires
# — which is past the test budget.
#
# Production tolerates the caching because writes are constant, the
# reconciler eventually catches up, and individual entities being briefly
# stale is acceptable. e2e is fast/cold and synchronous: even a ~1s LIFETIME
# left a window that poisoned ~1 row per run under the 12-suite parallel
# load, and the reconciler never repairs a cached '0/' (it only targets
# empty paths). DIRECT queries the source per lookup, shrinking the race to
# the raw CDC ordering window; the tiny e2e dataset makes the extra
# source-query load negligible.
#
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

# Wait for GitLab CH migrations to finish creating the dictionaries. The 600s
# budget covers launching this script before helmfile sync completes (the
# ClickHouse pod may not even exist yet); ch_query failures just poll again.
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

# Re-create each dict with the existing definition but LAYOUT(DIRECT) and no
# LIFETIME (DIRECT layouts reject one). `SHOW CREATE DICTIONARY` round-trip
# preserves SOURCE / column types so we don't have to duplicate the full DDL
# here. FORMAT TSVRaw is critical: the default TabSeparated output escapes
# newlines inside string literals (e.g. the embedded SOURCE QUERY) as literal
# `\n`, which makes the round-tripped DDL un-parseable. TSVRaw emits strings
# verbatim.
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

#!/usr/bin/env bash
# Recreate the traversal-path dictionaries with a near-zero LIFETIME so that
# negative cache entries (caused by the routes-vs-namespaces race in siphon
# CDC) expire within a couple of seconds instead of the upstream 60-300s.
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
# Production tolerates the long lifetime because writes are constant, the
# reconciler eventually catches up, and individual entities being briefly
# stale is acceptable. e2e is fast/cold and synchronous, so we shrink the
# window to ~1s. The 5-namespace dataset makes the extra source-query load
# negligible.
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
NEW_LIFETIME="LIFETIME(MIN 0 MAX 1)"

log "Patching CH traversal-path dictionaries to $NEW_LIFETIME"

CH_POD=clickhouse-0
ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" "$CH_POD" -- \
    sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD"'
}

# Wait for GitLab CH migrations to finish creating the dictionaries.
log "Waiting for traversal-path dictionaries to exist"
for _ in $(seq 1 60); do
  if ch_query "EXISTS DICTIONARY datalake.namespace_traversal_paths_dict" 2>/dev/null | grep -q '^1$'; then
    break
  fi
  sleep 5
done

# Re-create each dict with the existing definition but a shorter LIFETIME.
# `SHOW CREATE DICTIONARY` round-trip preserves SOURCE / LAYOUT / column
# types so we don't have to duplicate the full DDL here — only the LIFETIME
# clause is rewritten.
for dict in "${DICTS[@]}"; do
  log "Patching $dict"
  ddl=$(ch_query "SHOW CREATE DICTIONARY $dict" 2>/dev/null || true)
  if [[ -z "$ddl" ]]; then
    log "  $dict not found, skipping"
    continue
  fi
  patched=$(echo "$ddl" \
    | sed -E 's/^CREATE DICTIONARY/CREATE OR REPLACE DICTIONARY/' \
    | sed -E "s/LIFETIME\\([^)]*\\)/$NEW_LIFETIME/")
  ch_query "$patched" >/dev/null
done

log "CH traversal-path dictionaries patched"

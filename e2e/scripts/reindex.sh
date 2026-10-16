#!/usr/bin/env bash
# Runs the reindex operator job the way an operator does, after the Robot suites:
# corrupt the graph, pause the dispatcher, drain the indexer queue, start the job
# with kubectl, resume, and check that the rebuilt graph matches the one before.
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

TIMEOUT_SECONDS="${E2E_REINDEX_TIMEOUT_SECONDS:-900}"
CRONJOB=orbit-reindex
JOB="orbit-reindex-$(date +%s)"
PHANTOM_ID_OFFSET=900000000

log "E2E reindex (SHA: $E2E_SHA)"

if ! $KC get cronjob "$CRONJOB" -n "$NS_GKG" >/dev/null 2>&1; then
  log "SKIP: cronjob/$CRONJOB not deployed; the Orbit chart predates operatorJobs"
  exit 0
fi

ch_query() {
  printf '%s\n' "$1" | $KC exec -i -n "$NS_CH" clickhouse-0 -- \
    sh -c 'clickhouse-client --user default --password "$CLICKHOUSE_PASSWORD" --multiquery'
}
nats_box() {
  $KC exec -n "$NS_NATS" deploy/nats-box -- nats "$@"
}
wait_for() {
  local description=$1 check=$2
  SECONDS=0
  until eval "$check"; do
    if [ "$SECONDS" -ge "$TIMEOUT_SECONDS" ]; then
      log "FAIL: timed out after ${TIMEOUT_SECONDS}s waiting for $description"
      return 1
    fi
    sleep 5
  done
  log "$description (${SECONDS}s)"
}
queue_messages() {
  nats_box stream info "$STREAM" --json \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["state"]["messages"])'
}
version_status() {
  ch_query "SELECT status FROM gkg.gkg_schema_version FINAL WHERE version = $VERSION FORMAT TSV"
}

VERSION=$(ch_query "SELECT version FROM gkg.gkg_schema_version FINAL WHERE status = 'active' FORMAT TSV")
[ -n "$VERSION" ] || { log "FAIL: no active schema version"; exit 1; }
STREAM=$(nats_box stream ls -n | grep '^GKG_INDEXER_V' | head -1)
log "Active schema v$VERSION, indexer stream $STREAM"

# The e2e datalake replicates only `database: main`, so source tables of other
# databases never exist and their pipelines would hold the completion gate shut.
EXISTING=$(ch_query "SELECT name FROM system.tables WHERE database = 'datalake' FORMAT TSV")
MISSING_DDL=$(EXISTING="$EXISTING" python3 - "$GKG_ROOT/fixtures/siphon.sql" <<'PY'
import os, re, sys
existing = set(os.environ["EXISTING"].split())
lines = [l for l in open(sys.argv[1]).read().splitlines() if not l.strip().startswith("--")]
for statement in (s.strip() for s in "\n".join(lines).split(";")):
    match = re.match(r"CREATE (TABLE|MATERIALIZED VIEW)(?: IF NOT EXISTS)? `?(\w+)`?", statement)
    if not match or match.group(2) in existing:
        continue
    statement = re.sub(r"\b(TO|FROM|JOIN) (siphon_\w+)", r"\1 datalake.\2", statement)
    statement = statement.replace(match.group(0), f"CREATE {match.group(1)} IF NOT EXISTS datalake.{match.group(2)}", 1)
    print(statement + ";")
PY
)
if [ -n "$MISSING_DDL" ]; then
  log "Creating $(grep -c '^CREATE' <<<"$MISSING_DDL") datalake objects that e2e does not replicate"
  ch_query "$MISSING_DDL"
fi

# Live rows per graph table inside the enabled namespaces, plus an id checksum
# where the table has ids. Disabled namespaces keep their rows for a grace
# period but are not rebuilt, so they are left out.
read -r -d '' SNAPSHOT_GENERATOR <<SQL || true
SELECT 'SELECT * FROM (' || arrayStringConcat(groupArray(
  'SELECT ''' || graph_table || ''' AS t, count() AS rows, ' || if(has_id, 'sum(cityHash64(id))', '0') ||
  ' AS ids FROM gkg.' || graph_table || ' FINAL WHERE NOT _deleted AND arrayExists(p -> startsWith(traversal_path, p), ' ||
  '(SELECT groupArray(traversal_path) FROM datalake.siphon_knowledge_graph_enabled_namespaces FINAL WHERE NOT _siphon_deleted))'
), ' UNION ALL ') || ') ORDER BY t FORMAT TSV'
FROM (
  SELECT table AS graph_table, has(groupArray(name), 'id') AS has_id
  FROM system.columns
  WHERE database = 'gkg' AND startsWith(table, 'v${VERSION}_gl_')
  GROUP BY table
  HAVING has(groupArray(name), 'traversal_path') AND has(groupArray(name), '_deleted')
)
WHERE graph_table IN (SELECT name FROM system.tables WHERE database = 'gkg' AND engine LIKE '%MergeTree')
FORMAT TSVRaw
SQL
SNAPSHOT_SQL=$(ch_query "$SNAPSHOT_GENERATOR")
[ -n "$SNAPSHOT_SQL" ] || { log "FAIL: found no v$VERSION graph tables to compare"; exit 1; }
snapshot() { ch_query "$SNAPSHOT_SQL"; }

log "Pausing the dispatcher and draining the indexer queue"
trap '$KC scale deployment/orbit-dispatcher -n "$NS_GKG" --replicas=1 >/dev/null' EXIT
$KC scale deployment/orbit-dispatcher -n "$NS_GKG" --replicas=0
$KC wait --for=delete pod -l app.kubernetes.io/component=dispatcher -n "$NS_GKG" --timeout=180s || true
wait_for "indexer queue drained" '[ "$(queue_messages)" = 0 ]' || exit 1

BEFORE=$(snapshot)
WORK_ITEM=$(ch_query "SELECT id FROM gkg.v${VERSION}_gl_work_item FINAL WHERE NOT _deleted ORDER BY id LIMIT 1 FORMAT TSV")
[ -n "$WORK_ITEM" ] || { log "FAIL: no work items indexed before the reindex"; exit 1; }

log "Corrupting the graph: a stale title on work item $WORK_ITEM and a phantom work item"
ch_query "
INSERT INTO gkg.v${VERSION}_gl_work_item SELECT * REPLACE ('corrupted before reindex' AS title, _version + INTERVAL 1 DAY AS _version)
  FROM (SELECT * FROM gkg.v${VERSION}_gl_work_item FINAL WHERE id = $WORK_ITEM);
INSERT INTO gkg.v${VERSION}_gl_work_item SELECT * REPLACE (id + $PHANTOM_ID_OFFSET AS id)
  FROM (SELECT * FROM gkg.v${VERSION}_gl_work_item FINAL WHERE id = $WORK_ITEM)"

log "Starting job/$JOB from cronjob/$CRONJOB"
$KC create job "$JOB" --from="cronjob/$CRONJOB" -n "$NS_GKG"
if ! $KC wait --for=condition=complete "job/$JOB" -n "$NS_GKG" --timeout=600s; then
  $KC logs "job/$JOB" -n "$NS_GKG" --tail=50 || true
  log "FAIL: job/$JOB did not complete"
  exit 1
fi

STATUS=$(version_status)
ROWS=$(ch_query "SELECT count() FROM gkg.v${VERSION}_gl_work_item FORMAT TSV")
if [ "$STATUS" != migrating ] || [ "$ROWS" != 0 ]; then
  log "FAIL: after the job v$VERSION is '$STATUS' with $ROWS work item rows; want 'migrating' with 0"
  exit 1
fi

log "Resuming the dispatcher"
$KC scale deployment/orbit-dispatcher -n "$NS_GKG" --replicas=1
trap - EXIT
wait_for "v$VERSION promoted to active" '[ "$(version_status)" = active ]' || exit 1
if ! wait_for "graph rebuilt to the snapshot taken before the reindex" '[ "$(snapshot)" = "$BEFORE" ]'; then
  diff <(echo "$BEFORE") <(snapshot) || true
  exit 1
fi

LEFTOVERS=$(ch_query "SELECT count() FROM gkg.v${VERSION}_gl_work_item FINAL WHERE NOT _deleted AND (title = 'corrupted before reindex' OR id = $WORK_ITEM + $PHANTOM_ID_OFFSET) FORMAT TSV")
if [ "$LEFTOVERS" != 0 ]; then
  log "FAIL: $LEFTOVERS corrupted rows survived the reindex"
  exit 1
fi

log "Reindex passed: $(wc -l <<<"$BEFORE") graph tables match the snapshot taken before the reindex"

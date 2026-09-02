#!/usr/bin/env bash
set -euo pipefail
: "${KCTX:?set KCTX to the bench cluster kube-context}"
: "${RUN_ID:?set RUN_ID (e.g. bench12)}"
CH_NS="ra-ch-${RUN_ID}"
CH_PASS=$(kubectl --context="${KCTX}" get secret ra-ch-credentials -n "${CH_NS}" \
  -o jsonpath='{.data.default-password}' | base64 -d)

ch() {
  kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASS}" -d gkg --query "$1" 2>&1
}
cht() {
  kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASS}" -d gkg --time --query "$1 FORMAT Null" 2>&1 | tail -1
}
pk_granules() {
  ch "EXPLAIN PLAN json=1, indexes=1 $1 FORMAT TSVRaw" | python3 -c "
import json, sys
plan = json.loads(sys.stdin.read())
out = []
def walk(n):
    if n.get('Node Type') == 'ReadFromMergeTree':
        pk = next((i for i in n.get('Indexes', []) if i['Type']=='PrimaryKey'), {})
        out.append(f\"{pk.get('Initial Granules','?')}->{pk.get('Selected Granules','?')}\")
    for c in n.get('Plans', []): walk(c)
walk(plan[0]['Plan'])
print(' '.join(out))
"
}

EDGE="FROM v93_gl_edge AS e FINAL
  INNER JOIN v93_gl_merge_request AS mr FINAL ON e.target_id = mr.id
  INNER JOIN v93_gl_user AS u FINAL ON e.source_id = u.id
  WHERE e.relationship_kind = 'REVIEWER' AND e.source_kind = 'User' AND e.target_kind = 'MergeRequest'
    AND startsWith(e.traversal_path, '1/9970/') AND NOT e._deleted AND NOT mr._deleted AND NOT u._deleted"
MR="FROM v93_mat_reviewer FINAL WHERE startsWith(traversal_path, '1/9970/') AND NOT _deleted"
US="FROM v93_mat_reviewer_by_user FINAL WHERE startsWith(traversal_path, '1/9970/') AND NOT _deleted"

row() {
  local label="$1" e="$2" m="$3" u="$4"
  cht "$e" >/dev/null; local et; et=$(cht "$e")
  cht "$m" >/dev/null; local mt; mt=$(cht "$m")
  cht "$u" >/dev/null; local ut; ut=$(cht "$u")
  printf "%-34s edge=%6ss | mat_by_mr=%6ss (%s) | mat_by_user=%6ss (%s)\n" \
    "$label" "$et" "$mt" "$(pk_granules "$m")" "$ut" "$(pk_granules "$u")"
}

echo "Single reviewer drilldowns (should favor mat_by_user):"
row "GitLabDuo: count by state" \
  "SELECT mr.state, count() ${EDGE} AND u.id = 21826781 GROUP BY mr.state" \
  "SELECT mr_state, count() ${MR} AND u_id = 21826781 GROUP BY mr_state" \
  "SELECT mr_state, count() ${US} AND u_id = 21826781 GROUP BY mr_state"
row "rare reviewer: count by state" \
  "SELECT mr.state, count() ${EDGE} AND u.id = 263716 GROUP BY mr.state" \
  "SELECT mr_state, count() ${MR} AND u_id = 263716 GROUP BY mr_state" \
  "SELECT mr_state, count() ${US} AND u_id = 263716 GROUP BY mr_state"
row "rare reviewer: list open MRs" \
  "SELECT mr.iid, mr.title ${EDGE} AND u.id = 263716 AND mr.state = 'opened' LIMIT 20" \
  "SELECT mr_iid, mr_title ${MR} AND u_id = 263716 AND mr_state = 'opened' LIMIT 20" \
  "SELECT mr_iid, mr_title ${US} AND u_id = 263716 AND mr_state = 'opened' LIMIT 20"

echo ""
echo "MR-side lookups (should favor mat_by_mr):"
row "reviewers of one MR" \
  "SELECT u.username ${EDGE} AND mr.id = 499948704" \
  "SELECT u_username ${MR} AND mr_id = 499948704" \
  "SELECT u_username ${US} AND mr_id = 499948704"
row "reviewers of MR id range" \
  "SELECT count() ${EDGE} AND mr.id BETWEEN 499900000 AND 499999999" \
  "SELECT count() ${MR} AND mr_id BETWEEN 499900000 AND 499999999" \
  "SELECT count() ${US} AND mr_id BETWEEN 499900000 AND 499999999"

echo ""
echo "Full scans (should be equal):"
row "count by state" \
  "SELECT mr.state, count() ${EDGE} GROUP BY mr.state" \
  "SELECT mr_state, count() ${MR} GROUP BY mr_state" \
  "SELECT mr_state, count() ${US} GROUP BY mr_state"

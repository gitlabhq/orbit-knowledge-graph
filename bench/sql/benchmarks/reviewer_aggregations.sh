#!/usr/bin/env bash
set -euo pipefail
: "${KCTX:?set KCTX to the bench cluster kube-context}"
: "${RUN_ID:?set RUN_ID (e.g. bench12)}"
CH_NS="ra-ch-${RUN_ID}"
CH_PASS=$(kubectl --context="${KCTX}" get secret ra-ch-credentials -n "${CH_NS}" \
  -o jsonpath='{.data.default-password}' | base64 -d)

ch() {
  kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASS}" -d gkg --time --query "$1 FORMAT Null" 2>&1 | tail -1
}

EDGE_FROM="FROM v93_gl_edge AS e FINAL
  INNER JOIN v93_gl_merge_request AS mr FINAL ON e.target_id = mr.id
  INNER JOIN v93_gl_user AS u FINAL ON e.source_id = u.id
  WHERE e.relationship_kind = 'REVIEWER' AND e.source_kind = 'User' AND e.target_kind = 'MergeRequest'
    AND startsWith(e.traversal_path, '1/9970/') AND NOT e._deleted AND NOT mr._deleted AND NOT u._deleted"
MAT_FROM="FROM v93_mat_reviewer FINAL WHERE startsWith(traversal_path, '1/9970/') AND NOT _deleted"

run() {
  local label="$1" edge_sql="$2" mat_sql="$3"
  ch "${edge_sql}" >/dev/null
  local et; et=$(ch "${edge_sql}")
  ch "${mat_sql}" >/dev/null
  local mt; mt=$(ch "${mat_sql}")
  printf "%-42s edge=%6ss  mat=%6ss  %5.1fx\n" "${label}" "${et}" "${mt}" "$(echo "${et}/${mt}" | bc -l)"
}

run "count by MR state" \
  "SELECT mr.state, count() ${EDGE_FROM} GROUP BY mr.state" \
  "SELECT mr_state, count() ${MAT_FROM} GROUP BY mr_state"

run "top 20 reviewers" \
  "SELECT u.username, count() AS c ${EDGE_FROM} GROUP BY u.username ORDER BY c DESC LIMIT 20" \
  "SELECT u_username, count() AS c ${MAT_FROM} GROUP BY u_username ORDER BY c DESC LIMIT 20"

run "reviews per project" \
  "SELECT mr.project_id, count() AS c ${EDGE_FROM} GROUP BY mr.project_id ORDER BY c DESC LIMIT 20" \
  "SELECT mr_project_id, count() AS c ${MAT_FROM} GROUP BY mr_project_id ORDER BY c DESC LIMIT 20"

run "distinct reviewers per project" \
  "SELECT mr.project_id, uniq(u.id) AS c ${EDGE_FROM} GROUP BY mr.project_id ORDER BY c DESC LIMIT 20" \
  "SELECT mr_project_id, uniq(u_id) AS c ${MAT_FROM} GROUP BY mr_project_id ORDER BY c DESC LIMIT 20"

run "reviews per month (time series)" \
  "SELECT toStartOfMonth(mr.created_at) AS m, count() ${EDGE_FROM} GROUP BY m ORDER BY m" \
  "SELECT toStartOfMonth(mr_created_at) AS m, count() ${MAT_FROM} GROUP BY m ORDER BY m"

run "reviewer x state matrix" \
  "SELECT u.username, mr.state, count() AS c ${EDGE_FROM} GROUP BY u.username, mr.state ORDER BY c DESC LIMIT 50" \
  "SELECT u_username, mr_state, count() AS c ${MAT_FROM} GROUP BY u_username, mr_state ORDER BY c DESC LIMIT 50"

run "avg hours to merge by reviewer" \
  "SELECT u.username, avg(dateDiff('hour', mr.created_at, mr.merged_at)) AS h ${EDGE_FROM} AND mr.merged_at IS NOT NULL GROUP BY u.username ORDER BY h LIMIT 20" \
  "SELECT u_username, avg(dateDiff('hour', mr_created_at, mr_merged_at)) AS h ${MAT_FROM} AND mr_merged_at IS NOT NULL GROUP BY u_username ORDER BY h LIMIT 20"

run "single project drilldown by state" \
  "SELECT mr.state, count() ${EDGE_FROM} AND mr.project_id = 278964 GROUP BY mr.state" \
  "SELECT mr_state, count() ${MAT_FROM} AND mr_project_id = 278964 GROUP BY mr_state"

run "single reviewer drilldown by state" \
  "SELECT mr.state, count() ${EDGE_FROM} AND u.id = 21826781 GROUP BY mr.state" \
  "SELECT mr_state, count() ${MAT_FROM} AND u_id = 21826781 GROUP BY mr_state"

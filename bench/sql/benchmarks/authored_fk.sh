#!/usr/bin/env bash
set -euo pipefail
: "${KCTX:?set KCTX to the bench cluster kube-context}"
: "${RUN_ID:?set RUN_ID (e.g. bench12)}"
CH_NS="ra-ch-${RUN_ID}"
CH_PASS=$(kubectl --context="${KCTX}" get secret ra-ch-credentials -n "${CH_NS}" \
  -o jsonpath='{.data.default-password}' | base64 -d)

cht() {
  kubectl --context="${KCTX}" exec -n "${CH_NS}" clickhouse-0 -- \
    clickhouse-client --password "${CH_PASS}" -d gkg --time --query "$1 FORMAT Null" 2>&1 | tail -1
}

# This mirrors what the compiler emits for the FK Star strategy.
FK="FROM v93_gl_merge_request AS mr FINAL
  INNER JOIN v93_gl_user AS u FINAL ON mr.author_id = u.id
  WHERE startsWith(mr.traversal_path, '1/9970/') AND NOT mr._deleted AND NOT u._deleted"
MAT="FROM v93_mat_authored FINAL WHERE startsWith(traversal_path, '1/9970/') AND NOT _deleted"

run() {
  local label="$1" f="$2" m="$3"
  cht "$f" >/dev/null; local ft; ft=$(cht "$f")
  cht "$m" >/dev/null; local mt; mt=$(cht "$m")
  printf "%-40s fk_join=%6ss  mat=%6ss  %5.1fx\n" "$label" "$ft" "$mt" "$(echo "$ft/$mt" | bc -l)"
}

run "count by MR state" \
  "SELECT mr.state, count() ${FK} GROUP BY mr.state" \
  "SELECT mr_state, count() ${MAT} GROUP BY mr_state"
run "top 20 authors" \
  "SELECT u.username, count() AS c ${FK} GROUP BY u.username ORDER BY c DESC LIMIT 20" \
  "SELECT u_username, count() AS c ${MAT} GROUP BY u_username ORDER BY c DESC LIMIT 20"
run "MRs per month" \
  "SELECT toStartOfMonth(mr.created_at) AS m, count() ${FK} GROUP BY m ORDER BY m" \
  "SELECT toStartOfMonth(mr_created_at) AS m, count() ${MAT} GROUP BY m ORDER BY m"
run "author x state matrix" \
  "SELECT u.username, mr.state, count() AS c ${FK} GROUP BY u.username, mr.state ORDER BY c DESC LIMIT 50" \
  "SELECT u_username, mr_state, count() AS c ${MAT} GROUP BY u_username, mr_state ORDER BY c DESC LIMIT 50"
run "avg hours to merge by author" \
  "SELECT u.username, avg(dateDiff('hour', mr.created_at, mr.merged_at)) AS h ${FK} AND mr.merged_at IS NOT NULL GROUP BY u.username ORDER BY h LIMIT 20" \
  "SELECT u_username, avg(dateDiff('hour', mr_created_at, mr_merged_at)) AS h ${MAT} AND mr_merged_at IS NOT NULL GROUP BY u_username ORDER BY h LIMIT 20"
run "open MRs with author (traversal)" \
  "SELECT mr.iid, mr.title, u.username ${FK} AND mr.state = 'opened' LIMIT 20" \
  "SELECT mr_iid, mr_title, u_username ${MAT} AND mr_state = 'opened' LIMIT 20"
run "MRs by one author" \
  "SELECT mr.iid, mr.title, mr.state ${FK} AND u.id = 21826781 LIMIT 20" \
  "SELECT mr_iid, mr_title, mr_state ${MAT} AND u_id = 21826781 LIMIT 20"
run "single project drilldown" \
  "SELECT mr.state, count() ${FK} AND mr.project_id = 278964 GROUP BY mr.state" \
  "SELECT mr_state, count() ${MAT} AND mr_project_id = 278964 GROUP BY mr_state"

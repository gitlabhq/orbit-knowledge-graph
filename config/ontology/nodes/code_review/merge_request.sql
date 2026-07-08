SELECT
  m.id AS id,
  m.iid AS iid,
  m.title AS title,
  m.description AS description,
  m.source_branch AS source_branch,
  m.target_branch AS target_branch,
  m.state_id AS state_id,
  m.merge_status AS merge_status,
  m.draft AS draft,
  m.squash AS squash,
  m.created_at AS created_at,
  m.updated_at AS updated_at,
  m.merge_commit_sha AS merge_commit_sha,
  m.discussion_locked AS discussion_locked,
  m.prepared_at AS prepared_at,
  m.target_project_id AS target_project_id,
  m.source_project_id AS source_project_id,
  m.head_pipeline_id AS head_pipeline_id,
  m.latest_merge_request_diff_id AS latest_merge_request_diff_id,
  m.author_id AS author_id,
  m.merge_user_id AS merge_user_id,
  m.updated_by_id AS updated_by_id,
  m.last_edited_by_id AS last_edited_by_id,
  m.milestone_id AS milestone_id,
  if(
    m.state_id = 3 AND m.decoded_sha IS NOT NULL,
    toInt64(bitAnd(sipHash64(m.target_project_id, m.decoded_sha), toUInt64(9223372036854775807))),
    NULL
  ) AS merged_commit_id,
  m.traversal_path AS traversal_path,
  metrics.metric_latest_build_started_at AS metric_latest_build_started_at,
  metrics.metric_latest_build_finished_at AS metric_latest_build_finished_at,
  metrics.metric_first_deployed_to_production_at AS metric_first_deployed_to_production_at,
  metrics.metric_merged_at AS metric_merged_at,
  metrics.metric_latest_closed_at AS metric_latest_closed_at,
  metrics.metric_latest_closed_by_id AS metric_latest_closed_by_id,
  metrics.metric_first_comment_at AS metric_first_comment_at,
  metrics.metric_first_commit_at AS metric_first_commit_at,
  metrics.metric_last_commit_at AS metric_last_commit_at,
  metrics.metric_diff_size AS metric_diff_size,
  metrics.metric_modified_paths_size AS metric_modified_paths_size,
  metrics.metric_commits_count AS metric_commits_count,
  metrics.metric_first_approved_at AS metric_first_approved_at,
  metrics.metric_first_reassigned_at AS metric_first_reassigned_at,
  metrics.metric_added_lines AS metric_added_lines,
  metrics.metric_removed_lines AS metric_removed_lines,
  ifNull(metrics.metric_first_contribution, false) AS metric_first_contribution,
  metrics.metric_reviewer_first_assigned_at AS metric_reviewer_first_assigned_at,
  ifNull(reviewers.reviewers, CAST([], 'Array(Tuple(user_id UInt64, state Int16, created_at DateTime64(6, ''UTC'')))')) AS reviewers,
  ifNull(approvals.approvals, CAST([], 'Array(Tuple(user_id UInt64, created_at DateTime64(6, ''UTC'')))')) AS approvals,
  m.{{watermark_column}} AS _version,
  m.{{deleted_column}} AS _deleted
FROM (
  SELECT
    *,
    coalesce(
      if(
        startsWith(merged_commit_sha, '\\x'),
        unhex(substring(merged_commit_sha, 3)),
        nullIf(merged_commit_sha, '')
      ),
      nullIf(merge_commit_sha, ''),
      if(
        startsWith(squash_commit_sha, '\\x'),
        substring(squash_commit_sha, 3),
        lower(hex(nullIf(squash_commit_sha, '')))
      )
    ) AS decoded_sha
  FROM merge_requests
) AS m
LEFT JOIN (
  SELECT
    traversal_path AS _mt_tp,
    merge_request_id AS _mt_mr,
    argMax(latest_build_started_at, {{watermark_column}}) AS metric_latest_build_started_at,
    argMax(latest_build_finished_at, {{watermark_column}}) AS metric_latest_build_finished_at,
    argMax(first_deployed_to_production_at, {{watermark_column}}) AS metric_first_deployed_to_production_at,
    argMax(merged_at, {{watermark_column}}) AS metric_merged_at,
    argMax(latest_closed_at, {{watermark_column}}) AS metric_latest_closed_at,
    argMax(latest_closed_by_id, {{watermark_column}}) AS metric_latest_closed_by_id,
    argMax(first_comment_at, {{watermark_column}}) AS metric_first_comment_at,
    argMax(first_commit_at, {{watermark_column}}) AS metric_first_commit_at,
    argMax(last_commit_at, {{watermark_column}}) AS metric_last_commit_at,
    argMax(diff_size, {{watermark_column}}) AS metric_diff_size,
    argMax(modified_paths_size, {{watermark_column}}) AS metric_modified_paths_size,
    argMax(commits_count, {{watermark_column}}) AS metric_commits_count,
    argMax(first_approved_at, {{watermark_column}}) AS metric_first_approved_at,
    argMax(first_reassigned_at, {{watermark_column}}) AS metric_first_reassigned_at,
    argMax(added_lines, {{watermark_column}}) AS metric_added_lines,
    argMax(removed_lines, {{watermark_column}}) AS metric_removed_lines,
    argMax(first_contribution, {{watermark_column}}) AS metric_first_contribution,
    argMax(reviewer_first_assigned_at, {{watermark_column}}) AS metric_reviewer_first_assigned_at,
    argMax({{deleted_column}}, {{watermark_column}}) AS deleted
  FROM siphon_merge_request_metrics
  WHERE startsWith(traversal_path, {traversal_path:String})
    AND merge_request_id IN (
    SELECT
      id
    FROM merge_requests
    WHERE startsWith(traversal_path, {traversal_path:String})
      AND {{watermark_column}} > {last_watermark:String}
      AND {{watermark_column}} <= {watermark:String} )
  GROUP BY traversal_path, merge_request_id
  HAVING deleted = false ) AS metrics ON m.traversal_path = metrics._mt_tp
  AND m.id = metrics._mt_mr
LEFT JOIN (
  SELECT
    traversal_path AS _rv_tp,
    merge_request_id AS _rv_mr,
    CAST( groupArrayIf((toUInt64(user_id), state, created_at), deleted = false), 'Array(Tuple(user_id UInt64, state Int16, created_at DateTime64(6, ''UTC'')))' ) AS reviewers
  FROM (
    SELECT
      traversal_path,
      merge_request_id,
      id,
      argMax(user_id, {{watermark_column}}) AS user_id,
      argMax(state, {{watermark_column}}) AS state,
      argMax(created_at, {{watermark_column}}) AS created_at,
      argMax({{deleted_column}}, {{watermark_column}}) AS deleted
    FROM siphon_merge_request_reviewers
    WHERE startsWith(traversal_path, {traversal_path:String})
      AND merge_request_id IN (
      SELECT
        id
      FROM merge_requests
      WHERE startsWith(traversal_path, {traversal_path:String})
        AND {{watermark_column}} > {last_watermark:String}
        AND {{watermark_column}} <= {watermark:String} )
    GROUP BY traversal_path, merge_request_id, id )
  GROUP BY traversal_path, merge_request_id ) AS reviewers ON m.traversal_path = reviewers._rv_tp
  AND m.id = reviewers._rv_mr
LEFT JOIN (
  SELECT
    traversal_path AS _ap_tp,
    merge_request_id AS _ap_mr,
    CAST( groupArrayIf((toUInt64(user_id), created_at), deleted = false), 'Array(Tuple(user_id UInt64, created_at DateTime64(6, ''UTC'')))' ) AS approvals
  FROM (
    SELECT
      traversal_path,
      merge_request_id,
      id,
      argMax(user_id, {{watermark_column}}) AS user_id,
      argMax(created_at, {{watermark_column}}) AS created_at,
      argMax({{deleted_column}}, {{watermark_column}}) AS deleted
    FROM siphon_approvals
    WHERE startsWith(traversal_path, {traversal_path:String})
      AND merge_request_id IN (
      SELECT
        id
      FROM merge_requests
      WHERE startsWith(traversal_path, {traversal_path:String})
        AND {{watermark_column}} > {last_watermark:String}
        AND {{watermark_column}} <= {watermark:String} )
    GROUP BY traversal_path, merge_request_id, id )
  GROUP BY traversal_path, merge_request_id ) AS approvals ON m.traversal_path = approvals._ap_tp
  AND m.id = approvals._ap_mr
WHERE startsWith(m.traversal_path, {traversal_path:String}) {{filters}}
ORDER BY traversal_path, id
LIMIT {{batch_size}}

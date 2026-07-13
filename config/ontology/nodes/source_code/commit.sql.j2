SELECT
  toInt64(bitAnd(sipHash64(m.target_project_id, m.decoded_sha), toUInt64(9223372036854775807))) AS id,
  m.decoded_sha AS sha,
  m.target_project_id AS project_id,
  m.traversal_path AS traversal_path,
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
WHERE m.state_id = 3
  AND m.decoded_sha IS NOT NULL
  AND startsWith(m.traversal_path, {traversal_path:String}) {{filters}}
ORDER BY traversal_path, project_id, id
LIMIT {{batch_size}}

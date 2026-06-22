SELECT
    id,
    note,
    noteable_type,
    noteable_id,
    line_code,
    created_at,
    updated_at,
    discussion_id,
    resolved_at,
    internal,
    confidential,
    commit_id,
    st_diff,
    project_id,
    author_id,
    traversal_path,
    {{watermark_column}} AS _version,
    {{deleted_column}} AS _deleted
FROM siphon_notes
WHERE system = false {{filters}}
ORDER BY traversal_path, noteable_type, noteable_id, id
LIMIT {{batch_size}}

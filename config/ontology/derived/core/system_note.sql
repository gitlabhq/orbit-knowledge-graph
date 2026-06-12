SELECT
    sn.id AS id,
    sn.note AS note,
    sn.noteable_id AS noteable_id,
    sn.noteable_type AS noteable_type,
    sn.author_id AS author_id,
    sn.project_id AS project_id,
    sn.created_at AS created_at,
    sn.traversal_path AS traversal_path,
    sn.{{watermark_column}} AS {{watermark_column}},
    sn.{{deleted_column}} AS {{deleted_column}}
FROM siphon_notes AS sn
WHERE sn.system = true
  AND sn.{{deleted_column}} = false
  AND startsWith(sn.traversal_path, {traversal_path:String})

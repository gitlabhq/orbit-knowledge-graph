WITH
  _batch AS (SELECT
    id,
    note,
    noteable_id,
    noteable_type,
    author_id,
    project_id,
    created_at,
    traversal_path,
    {{watermark_column}} AS _version,
    {{deleted_column}} AS _deleted
  FROM siphon_notes
  WHERE startsWith(traversal_path, {traversal_path:String}) AND (system = true AND {{deleted_column}} = false) {{filters}}
  ORDER BY traversal_path, id
  LIMIT {{batch_size}}),
  _e0 AS (SELECT
    note_id AS id,
    argMax(action, {{watermark_column}}) AS action
  FROM siphon_system_note_metadata
  WHERE note_id IN (SELECT
      DISTINCT id
    FROM _batch)
    AND startsWith(traversal_path, {traversal_path:String})
  GROUP BY note_id
  HAVING argMax({{deleted_column}}, {{watermark_column}}) = false)
SELECT
  _batch.id AS id,
  _batch.note AS note,
  _batch.noteable_id AS noteable_id,
  _batch.noteable_type AS noteable_type,
  _batch.author_id AS author_id,
  _batch.project_id AS project_id,
  _batch.created_at AS created_at,
  _batch.traversal_path AS traversal_path,
  _batch._version AS _version,
  _batch._deleted AS _deleted,
  _e0.action AS action
FROM _batch
LEFT JOIN _e0 ON _batch.id = _e0.id

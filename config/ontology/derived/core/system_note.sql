-- The metadata join is bounded to the page (`note_id IN (SELECT id FROM
-- _batch)`); inlining it above the LIMIT would build a hash table over the
-- whole namespace's metadata per batch (#830).
WITH _batch AS (
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
      AND startsWith(sn.traversal_path, {traversal_path:String}) {{filters}}
    ORDER BY traversal_path, id
    LIMIT {{batch_size}}
),
_e0 AS (
    SELECT
        note_id AS id,
        argMax(action, {{watermark_column}}) AS action
    FROM siphon_system_note_metadata
    WHERE note_id IN (SELECT DISTINCT id FROM _batch)
      AND {{deleted_column}} = false
    GROUP BY note_id
)
SELECT
    _batch.id AS id,
    _batch.note AS note,
    _batch.noteable_id AS noteable_id,
    _batch.noteable_type AS noteable_type,
    _batch.author_id AS author_id,
    _batch.project_id AS project_id,
    _batch.created_at AS created_at,
    _batch.traversal_path AS traversal_path,
    _batch.{{watermark_column}} AS _version,
    _batch.{{deleted_column}} AS _deleted,
    _e0.action AS action
FROM _batch
LEFT JOIN _e0 ON _batch.id = _e0.id

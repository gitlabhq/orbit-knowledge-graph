SELECT
    src.*,
    src.{{watermark_column}} AS _version,
    src.{{deleted_column}} AS _deleted
FROM (
    SELECT
        namespace.id AS id,
        namespace.name AS name,
        ifNull(details.description, '') AS description,
        namespace.visibility_level AS visibility_level,
        if(isNotNull(routes.source_id), routes.path, namespace.path) AS full_path,
        namespace.parent_id AS parent_id,
        namespace.owner_id AS owner_id,
        namespace.created_at AS created_at,
        namespace.updated_at AS updated_at,
        traversal_paths.traversal_path AS traversal_path,
        namespace.{{watermark_column}} AS {{watermark_column}},
        (namespace.{{deleted_column}} OR namespace.type != 'Group') AS {{deleted_column}}
    FROM siphon_namespaces AS namespace
    INNER JOIN (
        SELECT id, traversal_path
        FROM namespace_traversal_paths
        WHERE startsWith(traversal_path, {traversal_path:String})
    ) AS traversal_paths ON namespace.id = traversal_paths.id
    LEFT JOIN (
        SELECT namespace_id, argMax(description, {{watermark_column}}) AS description
        FROM siphon_namespace_details
        WHERE startsWith(traversal_path, {traversal_path:String})
        GROUP BY namespace_id
    ) AS details ON namespace.id = details.namespace_id
    LEFT JOIN (
        SELECT toNullable(source_id) AS source_id, argMax(path, {{watermark_column}}) AS path
        FROM siphon_routes
        WHERE startsWith(traversal_path, {traversal_path:String})
          AND source_type = 'Namespace' AND NOT {{deleted_column}}
        GROUP BY source_id
    ) AS routes ON namespace.id = routes.source_id
    WHERE namespace.id IN (
        SELECT id FROM namespace_traversal_paths
        WHERE startsWith(traversal_path, {traversal_path:String})
    )
) AS src
WHERE 1=1 {{filters}}
ORDER BY traversal_path, id
LIMIT {{batch_size}}

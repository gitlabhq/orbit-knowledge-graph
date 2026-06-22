SELECT
    src.*,
    src.{{watermark_column}} AS _version,
    src.{{deleted_column}} AS _deleted
FROM (
    SELECT
        project.id AS id,
        project.name AS name,
        project.description AS description,
        project.visibility_level AS visibility_level,
        if(isNotNull(route.source_id), route.path, project.path) AS full_path,
        project.namespace_id AS namespace_id,
        project.creator_id AS creator_id,
        project.created_at AS created_at,
        project.updated_at AS updated_at,
        project.archived AS archived,
        project.star_count AS star_count,
        project.last_activity_at AS last_activity_at,
        traversal_paths.traversal_path AS traversal_path,
        project.{{watermark_column}} AS {{watermark_column}},
        project.{{deleted_column}} AS {{deleted_column}}
    FROM siphon_projects AS project
    INNER JOIN (
        SELECT id, traversal_path
        FROM project_namespace_traversal_paths
        WHERE startsWith(traversal_path, {traversal_path:String})
    ) AS traversal_paths ON project.id = traversal_paths.id
    LEFT JOIN (
        SELECT toNullable(source_id) AS source_id, argMax(path, {{watermark_column}}) AS path
        FROM siphon_routes
        WHERE startsWith(traversal_path, {traversal_path:String})
          AND source_type = 'Project' AND NOT {{deleted_column}}
        GROUP BY source_id
    ) AS route ON project.id = route.source_id
    WHERE project.id IN (
        SELECT id FROM project_namespace_traversal_paths
        WHERE startsWith(traversal_path, {traversal_path:String})
    )
) AS src
WHERE 1=1 {{filters}}
ORDER BY traversal_path, id
{{limit}}

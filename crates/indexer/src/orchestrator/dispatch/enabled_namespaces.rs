use orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
use query_data_model::DataModelError;
use query_data_model::catalog::sources::DataSource;

/// The lake table that lists the namespaces enabled for indexing.
pub fn enabled_namespaces_table(source: &DataSource) -> Result<&str, DataModelError> {
    source.enabled_namespaces.as_deref().ok_or_else(|| {
        DataModelError::Invalid(format!(
            "source {} lists no enabled namespaces",
            source.name
        ))
    })
}

pub fn resolved_enabled_namespaces_sql(source: &DataSource) -> Result<String, DataModelError> {
    let (version, deleted) = (&source.version, &source.deleted);
    let (paths, enabled) = (
        source.table("namespace_paths")?,
        enabled_namespaces_table(source)?,
    );
    Ok(format!(
        "SELECT id AS root_namespace_id, \
argMax(traversal_path, version) AS traversal_path \
FROM {paths} \
WHERE id IN ( \
SELECT root_namespace_id \
FROM {enabled} \
GROUP BY root_namespace_id, id \
HAVING argMax({deleted}, {version}) = false \
) \
GROUP BY id \
HAVING argMax(deleted, version) = false \
AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}$')"
    ))
}

pub fn resolved_paths_for_namespace_ids_sql(source: &DataSource) -> Result<String, DataModelError> {
    let paths = source.table("namespace_paths")?;
    Ok(format!(
        "SELECT id, argMax(traversal_path, version) AS traversal_path \
FROM {paths} \
WHERE id IN {{ids:Array(Int64)}} \
GROUP BY id \
HAVING argMax(deleted, version) = false \
AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}$')"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::siphon_source as siphon;

    fn both() -> [String; 2] {
        [
            resolved_enabled_namespaces_sql(&siphon()).unwrap(),
            resolved_paths_for_namespace_ids_sql(&siphon()).unwrap(),
        ]
    }

    #[test]
    fn sql_resolves_paths_from_the_datalake_table_not_the_enrollment_row() {
        for sql in both() {
            assert!(sql.contains("FROM namespace_traversal_paths"));
            assert!(sql.contains("argMax(traversal_path, version)"));
            assert!(!sql.contains("dictGet"));
        }
    }

    #[test]
    fn sql_keeps_only_the_latest_state_per_row() {
        let sql = resolved_enabled_namespaces_sql(&siphon()).unwrap();
        assert!(sql.contains("FROM siphon_knowledge_graph_enabled_namespaces"));
        assert!(sql.contains("HAVING argMax(_siphon_deleted, _siphon_replicated_at) = false"));
        assert!(sql.contains("GROUP BY root_namespace_id, id"));
        assert!(sql.contains("HAVING argMax(deleted, version) = false"));
    }

    #[test]
    fn sql_filters_to_top_level_paths() {
        for sql in both() {
            assert!(
                sql.contains(TOP_LEVEL_PREFIX_REGEX),
                "a '' or '0/' path would prefix-match every project during backfill"
            );
        }
    }
}

use std::sync::LazyLock;

use orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;

pub const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";

pub const NAMESPACE_PATHS_TABLE: &str = "namespace_traversal_paths";

static RESOLVED_ENABLED_NAMESPACES_SQL: LazyLock<String> = LazyLock::new(|| {
    let version = ontology::siphon_version_column();
    let deleted = ontology::siphon_deleted_column();
    format!(
        "SELECT id AS root_namespace_id, \
argMax(traversal_path, version) AS traversal_path \
FROM {NAMESPACE_PATHS_TABLE} \
WHERE id IN ( \
SELECT root_namespace_id \
FROM {ENABLED_NAMESPACE_TABLE} \
GROUP BY root_namespace_id, id \
HAVING argMax({deleted}, {version}) = false \
) \
GROUP BY id \
HAVING argMax(deleted, version) = false \
AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}')"
    )
});

static RESOLVED_PATHS_FOR_NAMESPACE_IDS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT id, argMax(traversal_path, version) AS traversal_path \
FROM {NAMESPACE_PATHS_TABLE} \
WHERE id IN {{ids:Array(Int64)}} \
GROUP BY id \
HAVING argMax(deleted, version) = false \
AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}')"
    )
});

pub fn resolved_enabled_namespaces_sql() -> &'static str {
    &RESOLVED_ENABLED_NAMESPACES_SQL
}

pub fn resolved_paths_for_namespace_ids_sql() -> &'static str {
    &RESOLVED_PATHS_FOR_NAMESPACE_IDS_SQL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_resolves_paths_from_the_datalake_table_not_the_enrollment_row() {
        for sql in [
            resolved_enabled_namespaces_sql(),
            resolved_paths_for_namespace_ids_sql(),
        ] {
            assert!(sql.contains("FROM namespace_traversal_paths"));
            assert!(sql.contains("argMax(traversal_path, version)"));
            assert!(!sql.contains("dictGet"));
        }
    }

    #[test]
    fn sql_keeps_only_the_latest_state_per_row() {
        let sql = resolved_enabled_namespaces_sql();
        assert!(sql.contains("HAVING argMax(_siphon_deleted, _siphon_replicated_at) = false"));
        assert!(sql.contains("GROUP BY root_namespace_id, id"));
        assert!(sql.contains("HAVING argMax(deleted, version) = false"));
    }

    #[test]
    fn sql_filters_to_top_level_paths() {
        for sql in [
            resolved_enabled_namespaces_sql(),
            resolved_paths_for_namespace_ids_sql(),
        ] {
            assert!(
                sql.contains(TOP_LEVEL_PREFIX_REGEX),
                "a '' or '0/' path would prefix-match every project during backfill"
            );
        }
    }
}

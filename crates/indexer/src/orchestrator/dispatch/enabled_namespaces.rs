use std::sync::LazyLock;

use gkg_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;

pub const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";

pub const NAMESPACE_PATHS_TABLE: &str = "namespace_traversal_paths";

static RESOLVED_ENABLED_NAMESPACES_SQL: LazyLock<String> = LazyLock::new(|| {
    let watermark = ontology::siphon_watermark_column();
    let deleted = ontology::siphon_deleted_column();
    format!(
        "SELECT id AS root_namespace_id, \
argMax(traversal_path, version) AS traversal_path \
FROM {NAMESPACE_PATHS_TABLE} \
WHERE id IN ( \
SELECT root_namespace_id \
FROM {ENABLED_NAMESPACE_TABLE} \
GROUP BY root_namespace_id, id \
HAVING argMax({deleted}, {watermark}) = false \
) \
GROUP BY id \
HAVING argMax(deleted, version) = false \
AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}')"
    )
});

pub fn resolved_enabled_namespaces_sql() -> &'static str {
    &RESOLVED_ENABLED_NAMESPACES_SQL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_resolves_paths_from_the_datalake_table_not_the_enrollment_row() {
        let sql = resolved_enabled_namespaces_sql();
        assert!(sql.contains("FROM namespace_traversal_paths"));
        assert!(sql.contains("argMax(traversal_path, version)"));
        assert!(!sql.contains("dictGet"));
    }

    #[test]
    fn sql_keeps_only_the_latest_state_per_row() {
        let sql = resolved_enabled_namespaces_sql();
        assert!(sql.contains("HAVING argMax(_siphon_deleted, _siphon_watermark) = false"));
        assert!(sql.contains("GROUP BY root_namespace_id, id"));
        assert!(sql.contains("HAVING argMax(deleted, version) = false"));
    }

    #[test]
    fn sql_filters_to_top_level_paths() {
        assert!(
            resolved_enabled_namespaces_sql().contains(TOP_LEVEL_PREFIX_REGEX),
            "a '' or '0/' path would prefix-match every project during backfill"
        );
    }
}

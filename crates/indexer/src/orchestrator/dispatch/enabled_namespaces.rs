use std::sync::LazyLock;

use gkg_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;

pub const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";

const NAMESPACE_PATH_DICTIONARY: &str = "namespace_traversal_paths_dict";

static RESOLVED_ENABLED_NAMESPACES_SQL: LazyLock<String> = LazyLock::new(|| {
    let watermark = ontology::siphon_watermark_column();
    let deleted = ontology::siphon_deleted_column();
    format!(
        "SELECT DISTINCT \
root_namespace_id, \
dictGetOrDefault('{NAMESPACE_PATH_DICTIONARY}', 'traversal_path', toUInt64(root_namespace_id), stored_traversal_path) AS traversal_path \
FROM ( \
SELECT root_namespace_id, id, \
argMax(traversal_path, {watermark}) AS stored_traversal_path, \
argMax({deleted}, {watermark}) AS is_deleted \
FROM {ENABLED_NAMESPACE_TABLE} \
GROUP BY root_namespace_id, id \
) \
WHERE is_deleted = false \
AND match(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}')"
    )
});

pub fn resolved_enabled_namespaces_sql() -> &'static str {
    &RESOLVED_ENABLED_NAMESPACES_SQL
}

#[cfg(test)]
mod tests {
    use super::*;
    use ontology::{Ontology, PathResolution};

    #[test]
    fn dictionary_const_matches_the_ontology_declaration() {
        let ontology = Ontology::load_embedded().unwrap();
        let declared = ontology
            .reindex_sources()
            .into_iter()
            .find_map(|source| match source.traversal_path {
                PathResolution::Dictionary { dictionary, .. }
                    if source.table == "siphon_namespaces" =>
                {
                    Some(dictionary)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(NAMESPACE_PATH_DICTIONARY, declared);
    }

    #[test]
    fn sql_resolves_path_through_the_dictionary_with_stored_fallback() {
        assert!(resolved_enabled_namespaces_sql().contains(
            "dictGetOrDefault('namespace_traversal_paths_dict', 'traversal_path', toUInt64(root_namespace_id), stored_traversal_path)"
        ));
    }

    #[test]
    fn sql_keeps_only_the_latest_state_per_enrollment_row() {
        let sql = resolved_enabled_namespaces_sql();
        assert!(sql.contains("argMax(traversal_path, _siphon_watermark)"));
        assert!(sql.contains("argMax(_siphon_deleted, _siphon_watermark)"));
        assert!(sql.contains("GROUP BY root_namespace_id, id"));
        assert!(sql.contains("WHERE is_deleted = false"));
    }

    #[test]
    fn sql_filters_to_top_level_paths() {
        assert!(
            resolved_enabled_namespaces_sql().contains(TOP_LEVEL_PREFIX_REGEX),
            "a '' or '0/' path would prefix-match every project during backfill"
        );
    }
}

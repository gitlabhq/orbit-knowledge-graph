//! The enabled-namespace set shared by every dispatch surface that reads
//! `siphon_knowledge_graph_enabled_namespaces`.

use std::sync::LazyLock;

use gkg_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
use ontology::{Ontology, PathResolution};

pub const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";

const NAMESPACE_PATH_SOURCE_TABLE: &str = "siphon_namespaces";

/// The stored `traversal_path` is a ClickHouse insert-time DEFAULT
/// (dictGetOrDefault with a '0/' fallback). When Siphon replicates the
/// enrollment row before the namespace row, the stored value stays '0/'
/// forever and every dispatch filter drops it — the namespace never indexes.
/// Re-resolving through the dictionary at query time (stored value as
/// fallback) makes such rows dispatchable as soon as the dictionary catches
/// up. The argMax dedup exists because the stored path is part of the
/// ReplacingMergeTree key: one enrollment can leave rows under several path
/// keys, and only the latest state per Postgres row may count — without it,
/// unenrolled namespaces keep dispatching until their tombstone merges.
static ENABLED_NAMESPACES_SQL: LazyLock<String> = LazyLock::new(|| {
    let ontology = Ontology::load_embedded().expect("embedded ontology is build-time validated");
    let dictionary = namespace_path_dictionary(&ontology);
    let watermark = ontology::siphon_watermark_column();
    let deleted = ontology::siphon_deleted_column();
    format!(
        "SELECT DISTINCT \
root_namespace_id, \
dictGetOrDefault('{dictionary}', 'traversal_path', toUInt64(root_namespace_id), stored_traversal_path) AS traversal_path \
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

pub fn enabled_namespaces_sql() -> &'static str {
    &ENABLED_NAMESPACES_SQL
}

fn namespace_path_dictionary(ontology: &Ontology) -> String {
    ontology
        .reindex_sources()
        .into_iter()
        .find_map(|source| match source.traversal_path {
            PathResolution::Dictionary { dictionary, .. }
                if source.table == NAMESPACE_PATH_SOURCE_TABLE =>
            {
                Some(dictionary)
            }
            _ => None,
        })
        .expect("ontology declares a traversal-path dictionary for siphon_namespaces")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dictionary_name_comes_from_the_ontology() {
        let ontology = Ontology::load_embedded().unwrap();
        assert_eq!(
            namespace_path_dictionary(&ontology),
            "namespace_traversal_paths_dict"
        );
    }

    #[test]
    fn sql_resolves_path_through_the_dictionary_with_stored_fallback() {
        let sql = enabled_namespaces_sql();
        assert!(sql.contains(
            "dictGetOrDefault('namespace_traversal_paths_dict', 'traversal_path', toUInt64(root_namespace_id), stored_traversal_path)"
        ));
    }

    #[test]
    fn sql_keeps_only_the_latest_state_per_enrollment_row() {
        let sql = enabled_namespaces_sql();
        assert!(sql.contains("argMax(traversal_path, _siphon_watermark)"));
        assert!(sql.contains("argMax(_siphon_deleted, _siphon_watermark)"));
        assert!(sql.contains("GROUP BY root_namespace_id, id"));
        assert!(sql.contains("WHERE is_deleted = false"));
    }

    #[test]
    fn sql_filters_to_top_level_paths() {
        assert!(enabled_namespaces_sql().contains(TOP_LEVEL_PREFIX_REGEX));
    }
}

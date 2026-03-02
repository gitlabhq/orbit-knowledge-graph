//! Centralized constants for the ontology crate.
//!
//! `GL_TABLE_PREFIX` is a compile-time constant whose value is validated
//! against the embedded ontology YAML by [`validate_ontology_constants`].
//! `EDGE_TABLE` is derived from it via `concatcp!` and also validated.

use const_format::concatcp;

/// Primary key field name used by default.
pub const DEFAULT_PRIMARY_KEY: &str = "id";

/// Reserved columns that exist on all nodes.
pub const NODE_RESERVED_COLUMNS: &[&str] = &["id"];

/// Reserved columns on the edge table (matches EdgeEntity schema).
pub const EDGE_RESERVED_COLUMNS: &[&str] = &[
    "traversal_path",
    "relationship_kind",
    "source_id",
    "source_kind",
    "target_id",
    "target_kind",
];

/// Prefix for all ClickHouse graph tables (e.g., `gl_user`, `gl_project`).
pub const GL_TABLE_PREFIX: &str = "gl_";

/// Edge table name in ClickHouse.
pub const EDGE_TABLE: &str = concatcp!(GL_TABLE_PREFIX, "edge");

/// Version column name in datalake tables.
pub const VERSION_COLUMN: &str = "_version";

/// Deleted flag column name in datalake tables.
pub const DELETED_COLUMN: &str = "_deleted";

/// Traversal path column name for namespace scoping.
pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

/// Panics if compile-time constants diverge from the embedded ontology YAML.
///
/// Call this at startup and in a `#[test]` so CI catches stale constants.
pub fn validate_ontology_constants() {
    let ontology = crate::Ontology::load_embedded().expect("embedded ontology must be valid");

    assert_eq!(
        ontology.table_prefix(),
        GL_TABLE_PREFIX,
        "GL_TABLE_PREFIX const (\"{GL_TABLE_PREFIX}\") doesn't match \
         embedded ontology (\"{}\") — update the const in constants.rs",
        ontology.table_prefix(),
    );

    assert_eq!(
        EDGE_TABLE,
        ontology.edge_table(),
        "EDGE_TABLE const (\"{EDGE_TABLE}\") doesn't match \
         embedded ontology (\"{}\") — update the const in constants.rs",
        ontology.edge_table(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ontology_constants_match_yaml() {
        validate_ontology_constants();
    }
}

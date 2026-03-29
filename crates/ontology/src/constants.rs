//! Centralized constants for the ontology crate.
//!
//! `GL_TABLE_PREFIX` is a compile-time constant whose value is validated
//! against the embedded ontology YAML by [`validate_ontology_constants`].
//! `EDGE_TABLE` is derived from it via `concatcp!` and also validated.

use const_format::concatcp;

/// Primary key field name used by default.
pub const DEFAULT_PRIMARY_KEY: &str = "id";

/// Compile-time default for `settings.internal_column_prefix`.
/// Used by `concatcp!` for static column name derivation. The runtime value
/// is loaded from YAML via [`Ontology::internal_column_prefix()`] and
/// validated against this const at startup.
pub const INTERNAL_COLUMN_PREFIX: &str = "_gkg_";

/// Reserved columns that exist on all nodes.
pub const NODE_RESERVED_COLUMNS: &[&str] = &["id"];

/// Reserved columns on the edge table (matches EdgeEntity schema).
pub const EDGE_RESERVED_COLUMNS: &[&str] = &[
    TRAVERSAL_PATH_COLUMN,
    RELATIONSHIP_KIND_COLUMN,
    SOURCE_ID_COLUMN,
    SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN,
    TARGET_KIND_COLUMN,
];

/// Edge column: type of relationship between two entities.
pub const RELATIONSHIP_KIND_COLUMN: &str = "relationship_kind";

/// Edge column: ID of the source entity.
pub const SOURCE_ID_COLUMN: &str = "source_id";

/// Edge column: entity type of the source.
pub const SOURCE_KIND_COLUMN: &str = "source_kind";

/// Edge column: ID of the target entity.
pub const TARGET_ID_COLUMN: &str = "target_id";

/// Edge column: entity type of the target.
pub const TARGET_KIND_COLUMN: &str = "target_kind";

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
pub fn validate_ontology_constants(ontology: &crate::Ontology) {
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

    for (i, col) in ontology.edge_columns().iter().enumerate() {
        assert_eq!(
            col.name, EDGE_RESERVED_COLUMNS[i],
            "edge_columns[{i}].name in YAML (\"{}\") doesn't match \
             EDGE_RESERVED_COLUMNS[{i}] (\"{}\") — update YAML or constants.rs",
            col.name, EDGE_RESERVED_COLUMNS[i],
        );
    }
    assert_eq!(
        ontology.edge_columns().len(),
        EDGE_RESERVED_COLUMNS.len(),
        "edge_columns count {} doesn't match EDGE_RESERVED_COLUMNS length {}",
        ontology.edge_columns().len(),
        EDGE_RESERVED_COLUMNS.len(),
    );

    assert_eq!(
        ontology.internal_column_prefix(),
        INTERNAL_COLUMN_PREFIX,
        "INTERNAL_COLUMN_PREFIX const (\"{INTERNAL_COLUMN_PREFIX}\") doesn't match \
         embedded ontology (\"{}\") — update the const in constants.rs",
        ontology.internal_column_prefix(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ontology_constants_match_yaml() {
        let ontology = crate::Ontology::load_embedded().expect("embedded ontology must be valid");
        validate_ontology_constants(&ontology);
    }
}

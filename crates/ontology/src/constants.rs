//! `GL_TABLE_PREFIX` is validated against the embedded ontology YAML by
//! [`validate_ontology_constants`]. `EDGE_TABLE` is derived from it via
//! `concatcp!` and also validated.

use const_format::concatcp;

pub const DEFAULT_PRIMARY_KEY: &str = "id";

pub const NODE_RESERVED_COLUMNS: &[&str] = &["id"];

/// Must match EdgeEntity schema.
pub const EDGE_RESERVED_COLUMNS: &[&str] = &[
    TRAVERSAL_PATH_COLUMN,
    RELATIONSHIP_KIND_COLUMN,
    SOURCE_ID_COLUMN,
    SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN,
    TARGET_KIND_COLUMN,
];

pub const RELATIONSHIP_KIND_COLUMN: &str = "relationship_kind";

pub const SOURCE_ID_COLUMN: &str = "source_id";

pub const SOURCE_KIND_COLUMN: &str = "source_kind";

pub const TARGET_ID_COLUMN: &str = "target_id";

pub const TARGET_KIND_COLUMN: &str = "target_kind";

pub const GL_TABLE_PREFIX: &str = "gl_";

pub const EDGE_TABLE: &str = concatcp!(GL_TABLE_PREFIX, "edge");

pub const VERSION_COLUMN: &str = "_version";

pub const DELETED_COLUMN: &str = "_deleted";

pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

use std::sync::LazyLock;

static EMBEDDED_ONTOLOGY: LazyLock<crate::Ontology> =
    LazyLock::new(|| crate::Ontology::load_embedded().expect("embedded ontology must be valid"));

/// Siphon datalake watermark column, derived from `schema.yaml`'s
/// `settings.etl.default_watermark` at runtime.
pub fn siphon_watermark_column() -> &'static str {
    &EMBEDDED_ONTOLOGY.etl_settings.watermark
}

/// Siphon datalake soft-delete flag column, derived from `schema.yaml`'s
/// `settings.etl.default_deleted` at runtime.
pub fn siphon_deleted_column() -> &'static str {
    &EMBEDDED_ONTOLOGY.etl_settings.deleted
}

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ontology_constants_match_yaml() {
        let ontology = crate::Ontology::load_embedded().expect("embedded ontology must be valid");
        validate_ontology_constants(&ontology);
    }

    #[test]
    fn siphon_columns_derived_from_yaml() {
        let ontology = crate::Ontology::load_embedded().expect("embedded ontology must be valid");
        assert_eq!(
            siphon_watermark_column(),
            ontology.default_watermark_column()
        );
        assert_eq!(siphon_deleted_column(), ontology.default_deleted_column());
    }
}

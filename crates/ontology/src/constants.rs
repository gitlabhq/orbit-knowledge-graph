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

pub const EDGE_KIND_SUFFIX: &str = "kind";

pub const EDGE_SRC_SUFFIX: &str = "src";

pub const EDGE_SRC_KIND_SUFFIX: &str = "src_kind";

pub const EDGE_DST_SUFFIX: &str = "dst";

pub const EDGE_DST_KIND_SUFFIX: &str = "dst_kind";

pub const EDGE_PATH_NODES_SUFFIX: &str = "path_nodes";

pub const EDGE_DEPTH_SUFFIX: &str = "depth";

pub const EDGE_OUTPUT_SUFFIXES: &[&str] = &[
    EDGE_KIND_SUFFIX,
    EDGE_SRC_SUFFIX,
    EDGE_SRC_KIND_SUFFIX,
    EDGE_DST_SUFFIX,
    EDGE_DST_KIND_SUFFIX,
    EDGE_PATH_NODES_SUFFIX,
    EDGE_DEPTH_SUFFIX,
];

pub const SOURCE_TAGS_COLUMN: &str = "source_tags";

pub const TARGET_TAGS_COLUMN: &str = "target_tags";

pub const TEXT_INDEX_TYPE: &str = "text";

pub const GL_TABLE_PREFIX: &str = "gl_";

pub const EDGE_TABLE: &str = concatcp!(GL_TABLE_PREFIX, "edge");

pub const VERSION_COLUMN: &str = "_version";

pub const DELETED_COLUMN: &str = "_deleted";

pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

use std::sync::LazyLock;

static EMBEDDED_ONTOLOGY: LazyLock<crate::Ontology> =
    LazyLock::new(|| crate::Ontology::load_embedded().expect("embedded ontology must be valid"));

pub fn siphon_version_column() -> &'static str {
    &EMBEDDED_ONTOLOGY.etl_settings.version
}

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

    let columns = ontology.edge_columns();
    for (i, reserved) in EDGE_RESERVED_COLUMNS.iter().enumerate() {
        assert_eq!(
            columns.get(i).map(|col| col.name.as_str()),
            Some(*reserved),
            "edge_columns[{i}] in YAML doesn't match EDGE_RESERVED_COLUMNS[{i}] (\"{reserved}\") — update YAML or constants.rs",
        );
    }
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

// Re-export so existing `crate::constants::` paths keep working.
pub use ontology::constants::{GL_TABLE_PREFIX, INTERNAL_COLUMN_PREFIX, TRAVERSAL_PATH_COLUMN};

macro_rules! internal_col {
    ($fn_name:ident, $suffix:literal) => {
        pub fn $fn_name() -> &'static str {
            const_format::concatcp!(INTERNAL_COLUMN_PREFIX, $suffix)
        }
    };
}

internal_col!(path_column, "path");
internal_col!(edge_kinds_column, "edge_kinds");
internal_col!(neighbor_id_column, "neighbor_id");
internal_col!(neighbor_type_column, "neighbor_type");
internal_col!(relationship_type_column, "relationship_type");
internal_col!(neighbor_is_outgoing_column, "neighbor_is_outgoing");

// _gkg_{alias}_pk  — always the entity's primary key (for hydration lookups)
pub fn primary_key_column(alias: &str) -> String {
    format!("{INTERNAL_COLUMN_PREFIX}{alias}_pk")
}

pub fn redaction_id_column(alias: &str) -> String {
    format!("{INTERNAL_COLUMN_PREFIX}{alias}_id")
}

pub(crate) fn redaction_type_column(alias: &str) -> String {
    format!("{INTERNAL_COLUMN_PREFIX}{alias}_type")
}

/// Hidden column carrying the entity's `traversal_path` value from the base
/// query into the hydration pipeline. Used to narrow hydration scans via
/// `startsWith(traversal_path, tp)`, pruning granules through the primary key.
pub fn traversal_path_column(alias: &str) -> String {
    format!("{INTERNAL_COLUMN_PREFIX}{alias}_tp")
}

/// Node alias used in synthetic hydration search queries.
/// `parse_property_batches` strips this prefix so consumers see clean keys.
pub const HYDRATION_NODE_ALIAS: &str = "hydrate";

/// Upper bound on rows fetched per entity type during dynamic hydration.
pub const MAX_DYNAMIC_HYDRATION_RESULTS: usize = 1000;

pub const EDGE_PATH_SUFFIX: &str = "path";
pub const EDGE_TYPE_SUFFIX: &str = "type";
pub const EDGE_SRC_SUFFIX: &str = "src";
pub const EDGE_SRC_TYPE_SUFFIX: &str = "src_type";
pub const EDGE_DST_SUFFIX: &str = "dst";
pub const EDGE_DST_TYPE_SUFFIX: &str = "dst_type";

/// Output alias suffixes for edge columns in traversal queries.
/// Matches `EDGE_RESERVED_COLUMNS` order from the ontology.
pub const EDGE_ALIAS_SUFFIXES: &[&str] = &[
    EDGE_PATH_SUFFIX,
    EDGE_TYPE_SUFFIX,
    EDGE_SRC_SUFFIX,
    EDGE_SRC_TYPE_SUFFIX,
    EDGE_DST_SUFFIX,
    EDGE_DST_TYPE_SUFFIX,
];

pub(crate) const DEPTH_COLUMN: &str = "depth";

pub(crate) const PATH_NODES_COLUMN: &str = "path_nodes";

/// Raw CTE-internal column before projection; distinct from `EDGE_KINDS_COLUMN`
/// (`_gkg_edge_kinds`), which is the output alias.
pub(crate) const FRONTIER_EDGE_KINDS_COLUMN: &str = "edge_kinds";

pub(crate) const ANCHOR_ID_COLUMN: &str = "anchor_id";

pub(crate) const END_ID_COLUMN: &str = "end_id";

pub(crate) const END_KIND_COLUMN: &str = "end_kind";

pub(crate) const FORWARD_CTE: &str = "forward";

pub(crate) const BACKWARD_CTE: &str = "backward";

pub(crate) const FORWARD_ALIAS: &str = "f";

pub(crate) const BACKWARD_ALIAS: &str = "b";

pub(crate) const PATHS_ALIAS: &str = "paths";

const NODE_FILTER_CTE_PREFIX: &str = "_nf_";

/// CTE name for a node-filter: `_nf_{alias}`.
pub(crate) fn node_filter_cte(alias: &str) -> String {
    format!("{NODE_FILTER_CTE_PREFIX}{alias}")
}

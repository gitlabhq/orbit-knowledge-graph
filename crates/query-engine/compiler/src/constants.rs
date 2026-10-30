// Re-export so existing `crate::constants::` paths keep working.
pub use ontology::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN};

// Per-node columns embed a user alias and end in one of these suffixes. No other
// internal name may, or an alias could reproduce it; the const asserts enforce that.
const PER_NODE_SUFFIXES: [&str; 5] = ["pk", "id", "type", "tp", "props"];

const fn takes_per_node_suffix(name: &str) -> bool {
    let mut index = 0;
    while index < PER_NODE_SUFFIXES.len() {
        if ends_with_word(name.as_bytes(), PER_NODE_SUFFIXES[index].as_bytes()) {
            return true;
        }
        index += 1;
    }
    false
}

const fn ends_with_word(name: &[u8], word: &[u8]) -> bool {
    if name.len() < word.len() {
        return false;
    }
    let start = name.len() - word.len();
    if start > 0 && name[start - 1] != b'_' {
        return false;
    }
    let mut index = 0;
    while index < word.len() {
        if name[start + index] != word[index] {
            return false;
        }
        index += 1;
    }
    true
}

/// Loaded once at startup from the embedded ontology YAML.
/// Every name the compiler invents (column, table alias, CTE) starts with this prefix,
/// and validation rejects user identifiers that use it, so the two never collide.
pub fn internal_column_prefix() -> &'static str {
    use std::sync::OnceLock;
    static PREFIX: OnceLock<String> = OnceLock::new();
    PREFIX.get_or_init(|| {
        ontology::Ontology::load_embedded()
            .expect("embedded ontology must load")
            .internal_column_prefix()
            .to_string()
    })
}

macro_rules! internal_name {
    ($vis:vis $fn_name:ident, $suffix:literal) => {
        const _: () = assert!(
            !takes_per_node_suffix($suffix),
            concat!("internal name ", $suffix, " ends in a per-node suffix")
        );
        $vis fn $fn_name() -> &'static str {
            static V: std::sync::OnceLock<String> = std::sync::OnceLock::new();
            V.get_or_init(|| format!("{}{}", internal_column_prefix(), $suffix))
        }
    };
}

internal_name!(pub path_column, "path");
internal_name!(pub edge_kinds_column, "edge_kinds");
internal_name!(pub neighbor_id_column, "neighbor_node");
internal_name!(pub neighbor_type_column, "neighbor_kind");
internal_name!(pub relationship_type_column, "relationship_kind");
internal_name!(pub neighbor_is_outgoing_column, "neighbor_is_outgoing");
internal_name!(pub hydration_node_alias, "hydrate");
internal_name!(pub neighbor_properties_column, "neighbor_properties");
internal_name!(pub neighbor_owner_column, "neighbor_owner");

internal_name!(pub(crate) depth_column, "depth");
internal_name!(pub(crate) path_nodes_column, "path_nodes");
internal_name!(pub(crate) frontier_edge_kinds_column, "frontier_edge_kinds");
internal_name!(pub(crate) anchor_node_column, "anchor_node");
internal_name!(pub(crate) end_node_column, "end_node");
internal_name!(pub(crate) end_kind_column, "end_kind");
internal_name!(pub(crate) path_start_column, "path_start");
internal_name!(pub(crate) path_end_column, "path_end");
internal_name!(pub(crate) forward_cte, "forward");
internal_name!(pub(crate) backward_cte, "backward");
internal_name!(pub(crate) paths_alias, "paths");
internal_name!(pub(crate) path_scope_cte, "path_scope");
internal_name!(pub(crate) path_scope_start_alias, "path_scope_start");
internal_name!(pub(crate) path_scope_end_alias, "path_scope_end");
internal_name!(pub(crate) neighbor_edge_alias, "neighbor_edge");
internal_name!(pub(crate) center_rows_cte, "center_rows");
internal_name!(pub(crate) scope_lookup_alias, "scope");
internal_name!(pub(crate) id_source_alias, "id_source");
internal_name!(pub(crate) page_cte, "page");
internal_name!(pub(crate) page_nodes_cte, "page_nodes");
internal_name!(pub(crate) node_properties_cte, "node_properties");
internal_name!(pub(crate) node_reads_alias, "node_reads");
internal_name!(pub(crate) node_row_column, "node_row");
internal_name!(pub(crate) row_position_column, "row_position");
internal_name!(pub(crate) page_node_alias, "page_node");
internal_name!(pub(crate) node_kind_column, "node_kind");
internal_name!(pub(crate) node_ref_column, "node_ref");
internal_name!(pub(crate) node_key_column, "node_key");
internal_name!(pub(crate) node_value_column, "node_value");
internal_name!(pub(crate) lookup_cte, "lookup");

// _orbit_{alias}_pk  — always the entity's primary key (for hydration lookups)
pub fn primary_key_column(alias: &str) -> String {
    format!("{}{alias}_pk", internal_column_prefix())
}

pub fn redaction_id_column(alias: &str) -> String {
    format!("{}{alias}_id", internal_column_prefix())
}

pub(crate) fn redaction_type_column(alias: &str) -> String {
    format!("{}{alias}_type", internal_column_prefix())
}

pub fn properties_column(alias: &str) -> String {
    format!("{}{alias}_props", internal_column_prefix())
}

/// Hidden column carrying the entity's `traversal_path` value from the base
/// query into the hydration pipeline. Used to narrow hydration scans via
/// `startsWith(traversal_path, tp)`, pruning granules through the primary key.
pub fn traversal_path_column(alias: &str) -> String {
    format!("{}{alias}_tp", internal_column_prefix())
}

pub(crate) fn edge_relation_alias(index: usize) -> String {
    format!("{}edge_{index}", internal_column_prefix())
}

pub(crate) fn edge_column_prefix(index: usize, variable_length: bool) -> String {
    let hop = if variable_length { "hop_" } else { "" };
    format!("{}{hop}e{index}_", internal_column_prefix())
}

/// Table alias of one hop in a multi-hop expansion; the first hop is depth 1.
pub(crate) fn hop_alias(depth: u32) -> String {
    format!("{}e{depth}", internal_column_prefix())
}

pub(crate) fn reached_depth_alias(depth: u32) -> String {
    format!("{}depth_{depth}", internal_column_prefix())
}

pub(crate) fn node_filter_cte(alias: &str) -> String {
    format!("{}nf_{alias}", internal_column_prefix())
}

/// Table alias of one arm in a UNION over several edge tables.
/// Stays reserved because `alias` is.
pub(crate) fn union_arm_alias(alias: &str) -> String {
    format!("{alias}_arm")
}

/// Upper bound on rows fetched per entity type during dynamic hydration.
pub const MAX_DYNAMIC_HYDRATION_RESULTS: usize = 1000;

pub const EDGE_KIND_SUFFIX: &str = "kind";
pub const EDGE_SRC_SUFFIX: &str = "src";
pub const EDGE_SRC_KIND_SUFFIX: &str = "src_kind";
pub const EDGE_DST_SUFFIX: &str = "dst";
pub const EDGE_DST_KIND_SUFFIX: &str = "dst_kind";
pub const EDGE_PATH_NODES_SUFFIX: &str = "path_nodes";

const _: () = {
    let suffixes = [
        EDGE_KIND_SUFFIX,
        EDGE_SRC_SUFFIX,
        EDGE_SRC_KIND_SUFFIX,
        EDGE_DST_SUFFIX,
        EDGE_DST_KIND_SUFFIX,
        EDGE_PATH_NODES_SUFFIX,
    ];
    let mut index = 0;
    while index < suffixes.len() {
        assert!(
            !takes_per_node_suffix(suffixes[index]),
            "edge column suffix ends in a per-node suffix"
        );
        index += 1;
    }
};

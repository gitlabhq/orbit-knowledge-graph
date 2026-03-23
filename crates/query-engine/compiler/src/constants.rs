// ENFORCEMENT CONSTANTS

use const_format::concatcp;

// Re-export so existing `crate::constants::` paths keep working.
pub use ontology::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN};

pub const GKG_COLUMN_PREFIX: &str = "_gkg_";

/// Column name for the typed path array in path finding queries.
/// Contains Array(Tuple(Int64, String)) with (node_id, entity_type) for each step.
pub const PATH_COLUMN: &str = concatcp!(GKG_COLUMN_PREFIX, "path");

/// Column name for the relationship kinds array in path finding queries.
/// Contains Array(String) with the relationship_kind for each hop.
/// Combined with PATH_COLUMN, this allows full edge reconstruction:
/// path[i] --edge_kinds[i]--> path[i+1].
pub const EDGE_KINDS_COLUMN: &str = concatcp!(GKG_COLUMN_PREFIX, "edge_kinds");

/// Column names for neighbor queries. The neighbor's ID and type are dynamic
/// (could be any entity type), similar to path finding nodes.
pub const NEIGHBOR_ID_COLUMN: &str = concatcp!(GKG_COLUMN_PREFIX, "neighbor_id");
pub const NEIGHBOR_TYPE_COLUMN: &str = concatcp!(GKG_COLUMN_PREFIX, "neighbor_type");
pub const RELATIONSHIP_TYPE_COLUMN: &str = concatcp!(GKG_COLUMN_PREFIX, "relationship_type");
pub const NEIGHBOR_IS_OUTGOING_COLUMN: &str = concatcp!(GKG_COLUMN_PREFIX, "neighbor_is_outgoing");

/// Tables that should NOT have traversal path security filters applied.
/// These are entities whose visibility is determined through relationships
/// (e.g., MEMBER_OF) rather than direct path hierarchy.
pub const SKIP_SECURITY_FILTER_TABLES: &[&str] = &[concatcp!(GL_TABLE_PREFIX, "user")];

// _gkg_{alias}_pk  — always the entity's primary key (for hydration lookups)
pub fn primary_key_column(alias: &str) -> String {
    format!("{GKG_COLUMN_PREFIX}{alias}_pk")
}

// _gkg_{alias}_id  — the authorization ID (may differ from pk for indirect-auth entities)
pub fn redaction_id_column(alias: &str) -> String {
    format!("{GKG_COLUMN_PREFIX}{alias}_id")
}

// _gkg_{alias}_type
pub fn redaction_type_column(alias: &str) -> String {
    format!("{GKG_COLUMN_PREFIX}{alias}_type")
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

// ─── CTE and internal column names ──────────────────────────────────────────

/// Internal CTE column for hop depth (used in path-finding and multi-hop traversal).
pub const DEPTH_COLUMN: &str = "depth";

/// Internal CTE column for accumulated path node tuples.
pub const PATH_NODES_COLUMN: &str = "path_nodes";

/// Internal CTE column for accumulated edge relationship kinds per hop.
pub const EDGE_KINDS_CTE_COLUMN: &str = "edge_kinds";

/// Internal CTE column for the frontier anchor node ID.
pub const ANCHOR_ID_COLUMN: &str = "anchor_id";

/// Internal CTE column for the frontier end node ID.
pub const END_ID_COLUMN: &str = "end_id";

/// Internal CTE column for the frontier end node type.
pub const END_KIND_COLUMN: &str = "end_kind";

/// Internal CTE column for the hop start node ID (multi-hop UNION ALL).
pub const START_ID_COLUMN: &str = "start_id";

/// CTE name for forward frontier in path-finding.
pub const FORWARD_CTE: &str = "forward";

/// CTE name for backward frontier in path-finding.
pub const BACKWARD_CTE: &str = "backward";

/// Table alias for the forward frontier CTE.
pub const FORWARD_ALIAS: &str = "f";

/// Table alias for the backward frontier CTE.
pub const BACKWARD_ALIAS: &str = "b";

/// Table alias for the combined paths UNION ALL subquery.
pub const PATHS_ALIAS: &str = "paths";

/// CTE name prefix for node-filter CTEs in edge-centric traversal.
pub const NODE_FILTER_CTE_PREFIX: &str = "_nf_";

/// CTE name prefix for cascading SIP CTEs.
pub const CASCADE_CTE_PREFIX: &str = "_cascade_";

/// Edge alias used in cascade/hop-frontier CTE building.
pub const CASCADE_EDGE_ALIAS: &str = "_ce";

/// Edge alias used in hop-frontier CTE building.
pub const HOP_EDGE_ALIAS: &str = "_he";

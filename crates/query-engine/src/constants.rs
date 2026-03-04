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

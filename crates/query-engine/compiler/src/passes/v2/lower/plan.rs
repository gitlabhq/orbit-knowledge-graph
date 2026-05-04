//! QueryPlan: the output of the PlannerPass, input to the LowerPass.
//!
//! Separates the "what" (decisions about query structure) from the "how"
//! (AST generation). Each variant carries all decisions needed to emit SQL
//! without re-consulting the Input.

use std::collections::HashMap;

use crate::input::*;

use super::types::*;

/// Top-level plan — one variant per query type.
pub enum QueryPlan {
    /// Traversal / Aggregation: skeleton-first edge chain.
    Skeleton(Skeleton),
    /// Neighbors: single-hop edge scan for adjacent entities.
    Neighbors(NeighborsPlan),
    /// PathFinding: bidirectional frontier expansion.
    PathFinding(PathFindingPlan),
    /// Hydration: fetch node properties for a set of IDs.
    Hydration(HydrationPlan),
}

impl QueryPlan {
    /// Return pre-computed node-to-edge-column mappings for the enforce pass.
    pub fn node_edge_mappings(&self) -> HashMap<String, (String, String)> {
        match self {
            Self::Skeleton(s) => s.node_edge_mappings.clone(),
            // Neighbors populates node_edge_col directly in emit (per-arm center col).
            Self::Neighbors(_) | Self::PathFinding(_) | Self::Hydration(_) => HashMap::new(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors plan
// ─────────────────────────────────────────────────────────────────────────────

pub struct NeighborsPlan {
    pub center_id: String,
    pub center_entity: String,
    pub center_table: String,
    pub center_uses_default_pk: bool,
    pub center_redaction_col: String,
    pub center_node_ids: Vec<i64>,
    pub center_filters: Vec<(String, InputFilter)>,
    pub center_id_range: Option<InputIdRange>,
    pub has_non_denorm: bool,
    pub direction: Direction,
    pub edge_tables: Vec<String>,
    pub rel_type_filter: Option<Vec<String>>,
    /// Denorm column map snapshot from CompilerMetadata.
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    pub order_by: Option<InputOrderBy>,
    pub cursor: Option<InputCursor>,
    pub limit: u32,
}

// ─────────────────────────────────────────────────────────────────────────────
// PathFinding plan
// ─────────────────────────────────────────────────────────────────────────────

pub struct PathFindingPlan {
    pub start: PathEndpoint,
    pub end: PathEndpoint,
    pub max_depth: u32,
    pub forward_depth: u32,
    pub backward_depth: u32,
    pub rel_type_filter: Option<Vec<String>>,
    pub forward_first_hop_filter: Option<Vec<String>>,
    pub backward_first_hop_filter: Option<Vec<String>>,
    pub edge_tables: Vec<String>,
    pub scoped_by_tp: bool,
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    pub cursor: Option<InputCursor>,
    pub limit: u32,
}

/// One endpoint of a path-finding query (start or end).
pub struct PathEndpoint {
    pub id: String,
    pub entity: String,
    pub table: String,
    pub node_ids: Vec<i64>,
    pub filters: HashMap<String, InputFilter>,
    pub id_range: Option<InputIdRange>,
    pub has_tp: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// Hydration plan
// ─────────────────────────────────────────────────────────────────────────────

pub struct HydrationNodePlan {
    pub alias: String,
    pub table: String,
    pub entity: String,
    pub id_property: String,
    pub node_ids: Vec<i64>,
    pub columns: Vec<String>,
}

pub struct HydrationPlan {
    pub nodes: Vec<HydrationNodePlan>,
    pub limit: u32,
}

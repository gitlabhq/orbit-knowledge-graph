//! Skeleton IR — describes the query plan, not SQL fragments.
//!
//! The Skeleton is a state machine that builders populate and emitters read.
//! Query-type modules (traversal, aggregation, etc.) call `Skeleton::plan()`
//! to build the IR, then `Skeleton::emit()` to produce the Query AST.

use std::collections::HashMap;

use crate::input::*;

// ─────────────────────────────────────────────────────────────────────────────
// Skeleton: the query plan
// ─────────────────────────────────────────────────────────────────────────────

pub struct Skeleton {
    /// Ordered chain of edge hops.
    pub hops: Vec<Hop>,
    /// Per-node planning metadata. Keyed by node alias.
    pub nodes: HashMap<String, NodePlan>,
    /// Execution strategy for the edge chain.
    pub strategy: Strategy,
}

/// A single edge hop in the skeleton chain.
pub struct Hop {
    /// Relationship types to match (e.g. ["AUTHORED"]).
    pub rel_types: Vec<String>,
    /// Physical edge table (e.g. "gl_edge", "gl_code_edge").
    pub edge_table: String,
    /// Node alias on the "from" side of this hop.
    pub from_node: String,
    /// Node alias on the "to" side of this hop.
    pub to_node: String,
    /// Edge direction (determines source_id vs target_id mapping).
    pub direction: Direction,
    /// Min hops (1 = include depth-1, 2 = skip depth-1, etc.).
    pub min_hops: u32,
    /// Max hops (1 for single-hop, >1 for variable-length).
    pub max_hops: u32,
    /// FK on a node table that encodes this relationship.
    /// When set, the skeleton can join node tables directly without the edge table.
    pub fk: Option<HopFk>,
}

/// FK info for a hop — which node has the FK column.
#[derive(Clone, Debug)]
pub struct HopFk {
    /// Node alias that holds the FK (must be one of from_node or to_node).
    pub fk_node: String,
    /// The FK column on that node (e.g. "project_id").
    pub fk_column: String,
    /// The other node's alias (the one the FK points to).
    pub target_node: String,
}

/// Per-node plan: where its ID comes from and what to do with it.
pub struct NodePlan {
    pub alias: String,
    pub entity: Option<String>,
    pub table: Option<String>,
    pub selectivity: Selectivity,
    pub hydration: HydrationStrategy,
    /// Which edge alias + column carries this node's ID in the skeleton.
    pub id_source: Option<IdSource>,
    /// Node filters from the query input.
    pub filters: Vec<(String, InputFilter)>,
    /// Pinned IDs (node_ids from query input).
    pub node_ids: Vec<i64>,
    /// ID range filter.
    pub id_range: Option<InputIdRange>,
    /// Whether the node table has a traversal_path column (most do; User/Runner don't).
    pub has_traversal_path: bool,
    /// Auth column (usually "id", but e.g. "project_id" for Definition).
    pub redaction_id_column: String,
    /// Columns requested by the user.
    pub columns: Option<ColumnSelection>,
}

/// Where a node's ID lives in the emitted SQL.
#[derive(Clone)]
pub struct IdSource {
    pub edge_alias: String,
    pub column: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Selectivity: how narrow this node's result set is
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Selectivity {
    /// Pinned IDs — most selective.
    Pinned,
    /// Has an ID range.
    IdRange,
    /// Has property filters.
    Filtered,
    /// Auth-scoped only (traversal_path).
    AuthScoped,
    /// Unconstrained.
    Open,
}

impl Selectivity {
    pub fn from_node(node: &InputNode) -> Self {
        if !node.node_ids.is_empty() {
            Self::Pinned
        } else if node.id_range.is_some() {
            Self::IdRange
        } else if !node.filters.is_empty() {
            Self::Filtered
        } else {
            Self::AuthScoped
        }
    }

    pub fn is_selective(self) -> bool {
        matches!(self, Self::Pinned | Self::IdRange | Self::Filtered)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Hydration strategy
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum HydrationStrategy {
    /// Full JOIN — needed for GROUP BY, ORDER BY, agg property targets.
    Join,
    /// WHERE IN subquery — non-denormalized filters, no columns in SELECT.
    FilterOnly,
    /// No hydration — edge carries everything needed.
    Skip,
}

// ─────────────────────────────────────────────────────────────────────────────
// Execution strategy
// ─────────────────────────────────────────────────────────────────────────────

pub enum Strategy {
    /// Flat edge chain: e0 JOIN e1 JOIN e2 ... (no CTEs).
    Flat,
    /// Bidirectional: forward arm + backward arm meeting at a hop index.
    Bidirectional { meeting_hop: usize },
    /// Single node, no edges.
    SingleNode,
    /// Star-schema optimization: all hops have FKs on the same center node.
    /// The center node drives a single scan; other nodes JOIN via FK columns.
    /// Zero edge table scans.
    FkStar { center: String },
}

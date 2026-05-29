//! Query planning: reads Input, produces a Plan.
//!
//! One Plan struct with a PlanBody enum. Common fields (nodes, hops,
//! limit, etc.) live on Plan; query-type-specific data lives in the
//! body variant. The Rust enum enforces that emit functions only access
//! their own variant's data.

pub mod edge_chain;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;

use std::collections::{HashMap, HashSet};

use crate::error::{QueryError, Result};
use crate::input::*;

pub use edge_chain::{Hop, HopFk, HydrationStrategy, JoinColumns, NodePlan, Selectivity, Strategy};
pub use hydration::HydrationNodePlan;

/// Pipeline state compatibility alias (HasQueryPlan, take_query_plan, etc.).
pub type QueryPlan = Plan;

/// The single query plan. Common fields shared by all query types live
/// here; query-type-specific data lives in `body`.
pub struct Plan {
    pub nodes: HashMap<String, NodePlan>,
    pub hops: Vec<Hop>,
    pub strategy: Strategy,
    pub limit: u32,
    pub order_by: Option<InputOrderBy>,
    pub cursor: Option<InputCursor>,
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub denorm_columns: HashMap<(String, String, String), (String, String)>,
    /// Per-table column sets from the ontology. Used by the lowerer to
    /// push node-level filters (e.g. project_id, branch) down to edge
    /// scans when the edge table has those columns.
    pub table_columns: HashMap<String, HashSet<String>>,
    pub body: PlanBody,
}

impl Plan {
    pub fn node_edge_mappings(&self) -> HashMap<String, (String, String)> {
        self.node_edge_mappings.clone()
    }
}

pub enum PlanBody {
    Traversal,
    Aggregation {
        aggregations: Vec<InputAggregationMetric>,
        agg_sort: Option<InputAggSort>,
    },
    Neighbors {
        center: String,
        direction: Direction,
        edge: EdgeTableConfig,
        has_non_denorm: bool,
    },
    PathFinding(PathFindingBody),
    Hydration(Vec<HydrationNodePlan>),
}

pub struct PathFindingBody {
    pub start: String,
    pub end: String,
    pub max_depth: u32,
    pub forward_depth: u32,
    pub backward_depth: u32,
    pub edge: EdgeTableConfig,
    pub forward_first_hop_filter: Option<Vec<String>>,
    pub backward_first_hop_filter: Option<Vec<String>>,
    pub scoped_by_tp: bool,
}

pub struct EdgeTableConfig {
    pub tables: Vec<String>,
    pub rel_type_filter: Option<Vec<String>>,
    /// Valid source entity kinds for the rel_types (union across all types).
    /// Empty when rel_types is unset. Used by pathfinding to add kind
    /// predicates on intermediate hops for granule pruning.
    pub source_kinds: Vec<String>,
    /// Valid target entity kinds for the rel_types.
    pub target_kinds: Vec<String>,
}

impl EdgeTableConfig {
    pub fn from_input(metadata: &CompilerMetadata, rel_types: &[String]) -> Self {
        use std::collections::BTreeSet;
        let mut source_kinds = BTreeSet::new();
        let mut target_kinds = BTreeSet::new();
        for rt in rel_types {
            if let Some(kinds) = metadata.edge_source_kinds.get(rt) {
                source_kinds.extend(kinds.iter().cloned());
            }
            if let Some(kinds) = metadata.edge_target_kinds.get(rt) {
                target_kinds.extend(kinds.iter().cloned());
            }
        }
        Self {
            tables: metadata.resolve_edge_tables(rel_types),
            rel_type_filter: if rel_types.is_empty() {
                None
            } else {
                Some(rel_types.to_vec())
            },
            source_kinds: source_kinds.into_iter().collect(),
            target_kinds: target_kinds.into_iter().collect(),
        }
    }
}

pub fn find_node<'a>(input: &'a Input, alias: &str) -> Result<&'a InputNode> {
    input
        .nodes
        .iter()
        .find(|n| n.id == alias)
        .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
}

pub fn plan(input: &mut Input) -> Result<Plan> {
    match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => Ok(edge_chain::plan(input)),
        QueryType::Neighbors => neighbors::plan_neighbors(input),
        QueryType::PathFinding => pathfinding::plan_pathfinding(input),
        QueryType::Hydration => hydration::plan_hydration(input),
    }
}

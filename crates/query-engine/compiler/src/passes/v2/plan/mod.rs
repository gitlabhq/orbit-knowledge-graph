//! Query planning: reads Input, produces a QueryPlan.
//!
//! Separates the "what" (decisions about query structure) from the "how"
//! (AST generation in `lower/`). Each variant carries all decisions needed
//! to emit SQL without re-consulting the Input.

pub mod edge_chain;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;

use std::collections::HashMap;

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::error::{QueryError, Result};
use crate::input::*;

pub use edge_chain::*;
pub use hydration::{HydrationNodePlan, HydrationPlan};
pub use neighbors::NeighborsPlan;
pub use pathfinding::PathFindingPlan;

/// Top-level plan — one variant per query type.
pub enum QueryPlan {
    /// Traversal: edge-chain-first plan for graph traversal queries.
    Traversal(EdgeChainPlan),
    /// Aggregation: edge-chain-first plan for aggregation queries.
    Aggregation(EdgeChainPlan),
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
            Self::Traversal(p) | Self::Aggregation(p) => p.node_edge_mappings.clone(),
            Self::Neighbors(p) => p.node_edge_mappings.clone(),
            Self::PathFinding(_) | Self::Hydration(_) => HashMap::new(),
        }
    }
}

/// Look up a node by alias from Input, returning an error if not found.
pub fn find_node<'a>(input: &'a Input, alias: &str) -> Result<&'a InputNode> {
    input
        .nodes
        .iter()
        .find(|n| n.id == alias)
        .ok_or_else(|| QueryError::Lowering(format!("node '{alias}' not found")))
}

/// Resolved node identity and constraints, shared across plan types.
pub struct PlanNode {
    pub id: String,
    pub entity: String,
    pub table: String,
    pub node_ids: Vec<i64>,
    pub filters: Vec<(String, InputFilter)>,
    pub id_range: Option<InputIdRange>,
    pub has_traversal_path: bool,
    pub redaction_id_column: String,
}

impl PlanNode {
    pub fn from_input(node: &InputNode) -> Result<Self> {
        Ok(Self {
            id: node.id.clone(),
            entity: node
                .entity
                .as_ref()
                .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no entity", node.id)))?
                .clone(),
            table: node
                .table
                .as_ref()
                .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.id)))?
                .clone(),
            node_ids: node.node_ids.clone(),
            filters: node.filters.clone().into_iter().collect(),
            id_range: node.id_range.clone(),
            has_traversal_path: node.has_traversal_path,
            redaction_id_column: node.redaction_id_column.clone(),
        })
    }

    pub fn uses_default_pk(&self) -> bool {
        self.redaction_id_column == DEFAULT_PRIMARY_KEY
    }
}

/// Resolved edge table(s) and relationship type filter.
pub struct EdgeTableConfig {
    pub tables: Vec<String>,
    pub rel_type_filter: Option<Vec<String>>,
}

impl EdgeTableConfig {
    pub fn from_input(metadata: &CompilerMetadata, rel_types: &[String]) -> Self {
        Self {
            tables: metadata.resolve_edge_tables(rel_types),
            rel_type_filter: if rel_types.is_empty() {
                None
            } else {
                Some(rel_types.to_vec())
            },
        }
    }
}

/// Build a query plan from the input (phase 1).
pub fn plan(input: &mut Input) -> Result<QueryPlan> {
    match input.query_type {
        QueryType::Traversal => {
            let plan = EdgeChainPlan::plan(input);
            Ok(QueryPlan::Traversal(plan))
        }
        QueryType::Aggregation => {
            let plan = EdgeChainPlan::plan(input);
            Ok(QueryPlan::Aggregation(plan))
        }
        QueryType::Neighbors => {
            let p = neighbors::plan_neighbors(input)?;
            Ok(QueryPlan::Neighbors(p))
        }
        QueryType::PathFinding => {
            let p = pathfinding::plan_pathfinding(input)?;
            Ok(QueryPlan::PathFinding(p))
        }
        QueryType::Hydration => {
            let p = hydration::plan_hydration(input)?;
            Ok(QueryPlan::Hydration(p))
        }
    }
}

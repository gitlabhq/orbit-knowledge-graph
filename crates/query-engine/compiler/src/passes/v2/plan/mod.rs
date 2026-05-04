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

use crate::error::Result;
use crate::input::*;

pub use edge_chain::*;
pub use hydration::{HydrationNodePlan, HydrationPlan};
pub use neighbors::NeighborsPlan;
pub use pathfinding::{PathEndpoint, PathFindingPlan};

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
            // Neighbors populates node_edge_col directly in emit (per-arm center col).
            Self::Neighbors(_) | Self::PathFinding(_) | Self::Hydration(_) => HashMap::new(),
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

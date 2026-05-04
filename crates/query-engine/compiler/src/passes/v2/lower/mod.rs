//! Query lowerer: edge-chain-first, nodes are lazy.
//!
//! Two-phase architecture:
//!   1. **Plan** (`plan()` / `plan_*`): reads Input, produces a QueryPlan
//!   2. **Emit** (`emit()` / `lower_*`): reads QueryPlan, produces SQL AST
//!
//! When used via the pipeline, PlannerPass runs phase 1 and LowerPass
//! runs phase 2. When called directly (e.g. `lower()`), both phases
//! run inline for convenience.

pub mod aggregation;
pub mod emit;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
pub mod plan;
pub mod shared;
pub mod traversal;

use crate::ast::Node;
use crate::error::Result;
use crate::input::*;

pub use plan::QueryPlan;

use plan::EdgeChainPlan;

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

/// Emit SQL AST from a query plan (phase 2).
pub fn emit(query_plan: &QueryPlan, input: &mut Input) -> Result<Node> {
    match query_plan {
        QueryPlan::Traversal(plan) => traversal::emit_traversal(plan, input),
        QueryPlan::Aggregation(plan) => aggregation::emit_aggregation(plan, input),
        QueryPlan::Neighbors(p) => neighbors::emit_neighbors(p, input),
        QueryPlan::PathFinding(p) => pathfinding::emit_pathfinding(p, input),
        QueryPlan::Hydration(p) => hydration::emit_hydration(p),
    }
}

/// Convenience: plan + emit in one call (used by LowerPass when no
/// separate PlannerPass is in the pipeline).
pub fn lower(input: &mut Input) -> Result<Node> {
    let query_plan = plan(input)?;
    emit(&query_plan, input)
}

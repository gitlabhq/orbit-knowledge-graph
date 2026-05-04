//! Query lowerer: edge-chain-first, nodes are lazy.
//!
//! Emit phase: reads a QueryPlan, produces SQL AST.
//!
//! When used via the pipeline, PlannerPass runs phase 1 (`plan::plan()`)
//! and LowerPass runs phase 2 (`emit()`). When called directly (e.g.
//! `lower()`), both phases run inline for convenience.

pub mod aggregation;
pub mod emit;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
pub mod traversal;

use crate::ast::Node;
use crate::error::Result;
use crate::input::*;

use super::plan::{self, QueryPlan};

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
    let query_plan = plan::plan(input)?;
    emit(&query_plan, input)
}

//! Query lowerer: edge-chain-first, nodes are lazy.
//!
//! Emit phase: reads a QueryPlan, produces SQL AST.
//!
//! When used via the pipeline, PlannerPass runs phase 1 (`plan::plan()`)
//! and LowerPass runs phase 2 (`emit()`). When called directly (e.g.
//! `lower()`), both phases run inline for convenience.

pub mod aggregation;
mod fk_star;
mod flat_chain;
mod helpers;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
mod single_node;
pub mod traversal;

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::plan::{self, EdgeChainPlan, QueryPlan, Strategy};

// ─────────────────────────────────────────────────────────────────────────────
// EdgeChainPlan::emit()
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeChainPlan {
    /// Emit SQL AST from the plan. Pure AST generation — reads only
    /// from plan fields, does not consult Input.
    pub fn emit(&self, _input: &mut Input) -> Result<EmitOutput> {
        match self.strategy {
            Strategy::SingleNode => single_node::emit_single_node(self),
            Strategy::FkStar { ref center } => fk_star::emit_fk_star(self, center),
            Strategy::Flat | Strategy::Bidirectional { .. } => flat_chain::emit_flat_chain(self),
        }
    }
}

/// The output of emitting a plan — ready for query-type modules to wrap.
pub struct EmitOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
}

impl EmitOutput {
    /// Assemble into a final Query.
    pub fn into_query(
        self,
        mut select: Vec<SelectExpr>,
        group_by: Vec<Expr>,
        order_by: Vec<OrderExpr>,
        limit: u32,
    ) -> Query {
        select.extend(self.select);
        Query {
            ctes: self.ctes,
            select,
            from: self.from,
            where_clause: Expr::conjoin(self.where_parts),
            group_by,
            order_by,
            limit: Some(limit),
            ..Default::default()
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
    let query_plan = plan::plan(input)?;
    emit(&query_plan, input)
}

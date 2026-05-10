//! Query lowerer: edge-chain-first, nodes are lazy.

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

use super::plan::{self, Plan, PlanBody, Strategy};

impl Plan {
    pub fn emit_edge_chain(&self) -> Result<EmitOutput> {
        match self.strategy {
            Strategy::SingleNode => single_node::emit_single_node(self),
            Strategy::FkStar { ref center } => fk_star::emit_fk_star(self, center),
            Strategy::Flat | Strategy::Bidirectional { .. } => flat_chain::emit_flat_chain(self),
        }
    }
}

pub struct EmitOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
}

impl EmitOutput {
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

pub fn emit(plan: &Plan, input: &Input) -> Result<Node> {
    match &plan.body {
        PlanBody::Traversal => traversal::emit_traversal(plan),
        PlanBody::Aggregation {
            aggregations,
            agg_sort,
        } => aggregation::emit_aggregation(
            plan,
            aggregations,
            &input.aggregation.group_by,
            agg_sort.as_ref(),
        ),
        PlanBody::Neighbors {
            center,
            direction,
            edge,
            has_non_denorm,
        } => neighbors::emit_neighbors(plan, center, *direction, edge, *has_non_denorm),
        PlanBody::PathFinding(pf) => pathfinding::emit_pathfinding(plan, pf),
        PlanBody::Hydration(nodes) => {
            hydration::emit_hydration(nodes, plan.limit, input.hydration_dynamic)
        }
    }
}

pub fn lower(input: &mut Input) -> Result<Node> {
    let plan = plan::plan(input)?;
    emit(&plan, input)
}

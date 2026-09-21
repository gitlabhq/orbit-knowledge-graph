//! Query lowerer: edge-chain-first, nodes are lazy.

pub mod aggregation;
mod fk;
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
use super::shared;

impl Plan {
    pub fn emit_edge_chain(&self) -> Result<EmitOutput> {
        let mut out = match self.strategy {
            Strategy::SingleNode => single_node::emit_single_node(self),
            Strategy::Fk(ref shape) => fk::emit_fk(self, shape),
            Strategy::Flat | Strategy::Bidirectional { .. } => flat_chain::emit_flat_chain(self),
        }?;
        out.where_parts.extend(self.scope_guards.iter().cloned());
        Ok(out)
    }
}

pub struct EmitOutput {
    pub from: TableRef,
    pub edge_aliases: Vec<String>,
    pub where_parts: Vec<Expr>,
    pub select: Vec<SelectExpr>,
    pub ctes: Vec<Cte>,
    /// Edge predicates for `-If` aggregate combinators. When set, the
    /// aggregation pass emits `countIf(cond)` / `sumIf(col, cond)` / etc.
    /// and the predicates are already in the LIMIT BY subquery's WHERE.
    pub edge_if_predicates: Option<Expr>,
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
    let mut node = match &plan.body {
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
            center_tp_lookup,
        } => neighbors::emit_neighbors(
            plan,
            center,
            *direction,
            edge,
            *has_non_denorm,
            center_tp_lookup.as_ref(),
        ),
        PlanBody::PathFinding(pf) => pathfinding::emit_pathfinding(plan, pf),
        PlanBody::Hydration(nodes) => hydration::emit_hydration(
            nodes,
            plan.limit,
            input.hydration_dynamic,
            input.path_segment_budget,
        ),
    }?;

    if !input.join_predicates.is_empty()
        && let Node::Query(q) = &mut node
    {
        for jp in &input.join_predicates {
            let filter = InputFilter {
                op: Some(jp.op),
                rhs_column: Some((jp.rhs_node.clone(), jp.rhs_prop.clone())),
                ..Default::default()
            };
            let pred = shared::filter_to_expr(&jp.lhs_node, &jp.lhs_prop, &filter);
            q.where_clause = Some(match q.where_clause.take() {
                Some(existing) => Expr::and(existing, pred),
                None => pred,
            });
        }
    }

    Ok(node)
}

pub fn lower(input: &mut Input) -> Result<Node> {
    let plan = plan::plan(input)?;
    emit(&plan, input)
}

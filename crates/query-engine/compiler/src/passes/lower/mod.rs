//! Query lowerer: edge-chain-first, nodes are lazy.

pub mod aggregation;
mod fk;
mod flat_chain;
pub mod helpers;
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

#[derive(Default)]
pub struct LoweredMetadata {
    pub node_sources: std::collections::HashMap<String, (String, String)>,
    pub edges: Vec<LoweredEdge>,
    pub stable_order: Vec<OrderExpr>,
}

pub struct LoweredEdge {
    pub column_prefix: String,
    pub path_column: Option<String>,
    pub rel_types: Vec<String>,
}

pub struct LoweredQuery {
    pub ast: Node,
    pub metadata: LoweredMetadata,
}

impl Plan {
    pub fn emit_edge_chain(&self) -> Result<EmitOutput> {
        match self.strategy {
            Strategy::SingleNode => single_node::emit_single_node(self),
            Strategy::Fk(ref shape) => fk::emit_fk(self, shape),
            Strategy::Flat => flat_chain::emit_flat_chain(self),
        }
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

pub fn emit(plan: &Plan, input: &Input) -> Result<LoweredQuery> {
    let mut node = match &plan.body {
        PlanBody::Traversal => traversal::emit_traversal(plan, input),
        PlanBody::Aggregation {
            aggregations,
            agg_sort,
        } => aggregation::emit_aggregation(
            plan,
            input,
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
            input,
            center,
            *direction,
            edge,
            *has_non_denorm,
            center_tp_lookup.as_ref(),
        ),
        PlanBody::PathFinding(pf) => pathfinding::emit_pathfinding(plan, input, pf),
        PlanBody::Hydration(nodes) => hydration::emit_hydration(
            nodes,
            input.limit,
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

    let edges = plan
        .hops
        .iter()
        .enumerate()
        .map(|(index, hop)| {
            let prefix = if hop.max_hops > 1 {
                format!("hop_e{index}_")
            } else {
                format!("e{index}_")
            };
            LoweredEdge {
                path_column: (hop.max_hops > 1).then(|| format!("{prefix}path_nodes")),
                column_prefix: prefix,
                rel_types: hop.rel_types.clone(),
            }
        })
        .collect();
    let stable_order = match input.query_type {
        QueryType::Aggregation => match &node {
            Node::Query(query) => query.group_by.iter().cloned().map(OrderExpr::asc).collect(),
            Node::Insert(_) => Vec::new(),
        },
        QueryType::PathFinding => vec![
            OrderExpr::asc(Expr::func(
                "toString",
                vec![Expr::col("paths", crate::constants::path_column())],
            )),
            OrderExpr::asc(Expr::func(
                "toString",
                vec![Expr::col("paths", crate::constants::edge_kinds_column())],
            )),
        ],
        QueryType::Neighbors => match &plan.body {
            PlanBody::Neighbors {
                center, direction, ..
            } if *direction == Direction::Both => vec![
                OrderExpr::asc(Expr::ident(crate::constants::redaction_id_column(center))),
                OrderExpr::asc(Expr::ident(crate::constants::neighbor_id_column())),
                OrderExpr::asc(Expr::ident(crate::constants::relationship_type_column())),
                OrderExpr::asc(Expr::ident(crate::constants::neighbor_is_outgoing_column())),
            ],
            _ => vec![
                OrderExpr::asc(Expr::col("e", ontology::constants::SOURCE_ID_COLUMN)),
                OrderExpr::asc(Expr::col("e", ontology::constants::TARGET_ID_COLUMN)),
                OrderExpr::asc(Expr::col(
                    "e",
                    ontology::constants::RELATIONSHIP_KIND_COLUMN,
                )),
            ],
        },
        _ => {
            if input.relationships.is_empty() {
                return Ok(LoweredQuery {
                    ast: node,
                    metadata: LoweredMetadata {
                        node_sources: plan.node_edge_mappings(),
                        edges,
                        stable_order: input
                            .nodes
                            .iter()
                            .map(|node| OrderExpr::asc(Expr::col(&node.id, &node.id_property)))
                            .collect(),
                    },
                });
            }
            let mut sources: Vec<_> = plan.node_edge_mappings.values().cloned().collect();
            sources.sort();
            sources.dedup();
            sources
                .into_iter()
                .map(|(alias, column)| OrderExpr::asc(Expr::col(alias, column)))
                .collect()
        }
    };
    Ok(LoweredQuery {
        ast: node,
        metadata: LoweredMetadata {
            node_sources: plan.node_edge_mappings(),
            edges,
            stable_order,
        },
    })
}

pub fn lower(input: &mut Input) -> Result<Node> {
    let plan = plan::plan(input)?;
    let mut lowered = emit(&plan, input)?;
    if let Node::Query(query) = &mut lowered.ast {
        query.limit = Some(input.fetch_limit());
    }
    Ok(lowered.ast)
}

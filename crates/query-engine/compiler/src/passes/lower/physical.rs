use std::collections::HashSet;

use super::requirements::{column, predicate, projections};
use super::sql::latest_row_dedup;
use super::{EmitOutput, NodeBinding};
use crate::ast::{Cte, Expr, OrderExpr, Query, SelectExpr, TableRef};
use crate::passes::plan::physical::{
    ExecutionPlan, RelationGraph, RelationId, RelationNode, RelationRef,
};

pub(super) fn execute(plan: &ExecutionPlan) -> EmitOutput {
    let usage = plan.graph.usage(plan.root);
    let source = emit_ref(
        &RelationRef::from(plan.root),
        &plan.graph,
        &usage.materialized().collect(),
        None,
    );
    EmitOutput {
        from: source.from,
        where_parts: source.predicates,
        select: projections(&plan.outputs),
        ctes: plan
            .graph
            .iter()
            .filter(|(id, _)| usage.is_materialized(*id))
            .map(|(id, _)| {
                let mut cte = Cte::new(
                    relation_name(id),
                    relation_query(id, &plan.graph, &usage.materialized().collect(), Some(id)),
                );
                cte.materialized = usage.references(id) > 1;
                cte
            })
            .collect(),
        nodes: plan
            .bindings
            .iter()
            .map(|binding| {
                (
                    binding.node.clone(),
                    NodeBinding::source(
                        &binding.alias,
                        &binding.column,
                        binding.property_source.clone(),
                        binding.traversal_path.as_ref().map(column),
                    ),
                )
            })
            .collect(),
    }
}

pub(super) fn relation_name(id: RelationId) -> String {
    format!("_orbit_relation_{}", id.0)
}

fn relation_query(
    id: RelationId,
    graph: &RelationGraph,
    materialized: &HashSet<RelationId>,
    current: Option<RelationId>,
) -> Query {
    let output = emit_node(id, graph, materialized, current);
    Query {
        select: vec![SelectExpr::star()],
        from: output.from,
        where_clause: Expr::conjoin(output.predicates),
        order_by: output.order_by,
        limit: output.limit,
        ..Default::default()
    }
}

struct RelationOutput {
    from: TableRef,
    predicates: Vec<Expr>,
    order_by: Vec<OrderExpr>,
    limit: Option<u32>,
}

fn emit_ref(
    relation: &RelationRef,
    graph: &RelationGraph,
    materialized: &HashSet<RelationId>,
    current: Option<RelationId>,
) -> RelationOutput {
    let mut output = if materialized.contains(&relation.id) && current != Some(relation.id) {
        RelationOutput {
            from: TableRef::cte(
                relation_name(relation.id),
                relation
                    .alias
                    .clone()
                    .or_else(|| exposed_alias(relation.id, graph))
                    .unwrap_or_else(|| relation_name(relation.id)),
            ),
            predicates: vec![],
            order_by: vec![],
            limit: None,
        }
    } else {
        emit_node(relation.id, graph, materialized, current)
    };
    if let Some(alias) = &relation.alias
        && !matches!(&output.from, TableRef::Cte { alias: current, .. } if current == alias)
    {
        output.from = TableRef::subquery(
            Query {
                select: vec![SelectExpr::star()],
                from: output.from,
                where_clause: Expr::conjoin(std::mem::take(&mut output.predicates)),
                order_by: std::mem::take(&mut output.order_by),
                limit: output.limit.take(),
                ..Default::default()
            },
            alias,
        );
    }
    output
}

/// A materialized reference must expose the alias its inline form would, or predicates and join conditions dangle.
fn exposed_alias(id: RelationId, graph: &RelationGraph) -> Option<String> {
    match graph.get(id) {
        RelationNode::Read { alias, .. }
        | RelationNode::Owned { alias, .. }
        | RelationNode::UnionAll { alias, .. }
        | RelationNode::Bind { alias, .. }
        | RelationNode::Latest { alias, .. } => Some(alias.clone()),
        RelationNode::Filter { input, .. }
        | RelationNode::Sort { input, .. }
        | RelationNode::Limit { input, .. } => input
            .alias
            .clone()
            .or_else(|| exposed_alias(input.id, graph)),
        RelationNode::Project { input, .. } | RelationNode::Distinct { input } => {
            Some(input.alias.clone().unwrap_or_else(|| relation_name(id)))
        }
        RelationNode::Join { .. } => None,
    }
}

fn emit_node(
    id: RelationId,
    graph: &RelationGraph,
    materialized: &HashSet<RelationId>,
    current: Option<RelationId>,
) -> RelationOutput {
    match graph.get(id) {
        RelationNode::Read {
            table,
            alias,
            final_,
        } => RelationOutput {
            from: TableRef::Scan {
                table: table.clone(),
                alias: alias.clone(),
                final_: *final_,
                relationship: None,
            },
            predicates: vec![],
            order_by: vec![],
            limit: None,
        },
        RelationNode::Owned { input, alias, .. } => {
            let mut reference = input.clone();
            reference.alias = Some(alias.clone());
            emit_ref(&reference, graph, materialized, current)
        }
        RelationNode::Filter { input, predicates } => {
            let mut output = emit_ref(input, graph, materialized, current);
            output.predicates.extend(predicates.iter().map(predicate));
            output
        }
        RelationNode::Project { input, outputs } => {
            let output = emit_ref(input, graph, materialized, current);
            RelationOutput {
                from: TableRef::subquery(
                    Query {
                        select: projections(outputs),
                        from: output.from,
                        where_clause: Expr::conjoin(output.predicates),
                        order_by: output.order_by,
                        limit: output.limit,
                        ..Default::default()
                    },
                    input.alias.clone().unwrap_or_else(|| relation_name(id)),
                ),
                predicates: vec![],
                order_by: vec![],
                limit: None,
            }
        }
        RelationNode::Distinct { input } => {
            let output = emit_ref(input, graph, materialized, current);
            RelationOutput {
                from: TableRef::subquery(
                    Query {
                        distinct: true,
                        select: vec![SelectExpr::star()],
                        from: output.from,
                        where_clause: Expr::conjoin(output.predicates),
                        order_by: output.order_by,
                        limit: output.limit,
                        ..Default::default()
                    },
                    input.alias.clone().unwrap_or_else(|| relation_name(id)),
                ),
                predicates: vec![],
                order_by: vec![],
                limit: None,
            }
        }
        RelationNode::UnionAll { inputs, alias } => RelationOutput {
            from: TableRef::union_all(
                inputs
                    .iter()
                    .map(|input| {
                        let output = emit_ref(input, graph, materialized, current);
                        Query {
                            select: vec![SelectExpr::star()],
                            from: output.from,
                            where_clause: Expr::conjoin(output.predicates),
                            order_by: output.order_by,
                            limit: output.limit,
                            ..Default::default()
                        }
                    })
                    .collect(),
                alias,
            ),
            predicates: vec![],
            order_by: vec![],
            limit: None,
        },
        RelationNode::Join {
            left,
            right,
            endpoints,
            predicates,
        } => {
            let mut left = emit_ref(left, graph, materialized, current);
            let right = emit_ref(right, graph, materialized, current);
            let condition = predicates.iter().map(predicate).fold(
                Expr::eq(column(&endpoints.0), column(&endpoints.1)),
                Expr::and,
            );
            left.from = TableRef::join(
                crate::ast::JoinType::Inner,
                left.from,
                right.from,
                condition,
            );
            left.predicates.extend(right.predicates);
            left
        }
        RelationNode::Bind { input, alias } => {
            let output = emit_ref(input, graph, materialized, current);
            RelationOutput {
                from: TableRef::subquery(
                    Query {
                        select: vec![SelectExpr::star()],
                        from: output.from,
                        where_clause: Expr::conjoin(output.predicates),
                        order_by: output.order_by,
                        limit: output.limit,
                        ..Default::default()
                    },
                    alias,
                ),
                predicates: vec![],
                order_by: vec![],
                limit: None,
            }
        }
        RelationNode::Latest {
            input,
            alias,
            sort_key,
            condition,
        } => {
            let output = emit_ref(input, graph, materialized, current);
            let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
            RelationOutput {
                from: TableRef::subquery(
                    Query {
                        select: vec![SelectExpr::star()],
                        from: output.from,
                        where_clause: Expr::conjoin(output.predicates),
                        order_by,
                        limit_by,
                        ..Default::default()
                    },
                    alias,
                ),
                predicates: condition.iter().map(predicate).collect(),
                order_by: vec![],
                limit: None,
            }
        }
        RelationNode::Sort { input, keys } => {
            let mut output = emit_ref(input, graph, materialized, current);
            output.order_by = keys
                .iter()
                .map(|(value, desc)| {
                    if *desc {
                        OrderExpr::desc(column(value))
                    } else {
                        OrderExpr::asc(column(value))
                    }
                })
                .collect();
            output
        }
        RelationNode::Limit { input, rows } => {
            let mut output = emit_ref(input, graph, materialized, current);
            output.limit = Some(*rows);
            output
        }
    }
}

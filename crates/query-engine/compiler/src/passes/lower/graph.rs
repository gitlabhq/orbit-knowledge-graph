use std::collections::HashSet;

use super::predicates::{column, predicate, projections};
use super::sql::latest_row_dedup;
use super::{EmitOutput, NodeBinding};
use crate::ast::{Cte, Expr, Query, SelectExpr, TableRef};
use crate::passes::plan::graph::{
    ExecutionPlan, ReadOwner, RelationGraph, RelationId, RelationNode, RelationRef,
};
use crate::passes::plan::predicates::Column;
use crate::scope::{QueryScope, scope_predicate};

pub(super) struct Reads<'a> {
    graph: &'a RelationGraph,
    materialized: HashSet<RelationId>,
    scope: Option<&'a QueryScope>,
}

pub(super) fn execute(
    plan: &ExecutionPlan,
    root_columns: &[Column],
    scope: Option<&QueryScope>,
) -> EmitOutput {
    let usage = plan.graph.usage(plan.root);
    let reads = Reads {
        graph: &plan.graph,
        materialized: usage.materialized().collect(),
        scope,
    };
    let required = super::columns::required_columns(plan, &usage, root_columns);
    let source = emit_ref(&RelationRef::from(plan.root), &reads, None);
    EmitOutput {
        from: source.from,
        where_parts: source.predicates,
        select: projections(&plan.outputs),
        ctes: usage
            .materialized()
            .map(|id| {
                let select = match (required.get(&id), exposed_alias(id, &plan.graph)) {
                    (Some(columns), Some(alias)) if !columns.is_empty() => columns
                        .iter()
                        .map(|column| SelectExpr::new(Expr::col(&alias, column), column))
                        .collect(),
                    _ => star(),
                };
                let mut cte = Cte::new(
                    relation_name(id),
                    emit_node(id, &reads, Some(id)).query(select),
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

fn star() -> Vec<SelectExpr> {
    vec![SelectExpr::star()]
}

struct RelationOutput {
    from: TableRef,
    predicates: Vec<Expr>,
}

impl RelationOutput {
    fn query(self, select: Vec<SelectExpr>) -> Query {
        Query {
            select,
            from: self.from,
            where_clause: Expr::conjoin(self.predicates),
            ..Default::default()
        }
    }

    fn wrapped(self, select: Vec<SelectExpr>, alias: impl Into<String>) -> Self {
        Self {
            from: TableRef::subquery(self.query(select), alias),
            predicates: vec![],
        }
    }
}

fn emit_ref(
    relation: &RelationRef,
    reads: &Reads<'_>,
    current: Option<RelationId>,
) -> RelationOutput {
    let graph = reads.graph;
    let output = if reads.materialized.contains(&relation.id) && current != Some(relation.id) {
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
        }
    } else {
        emit_node(relation.id, reads, current)
    };
    match &relation.alias {
        Some(alias)
            if !matches!(&output.from, TableRef::Cte { alias: current, .. } if current == alias)
                && !matches!(&output.from, TableRef::Scan { alias: current, final_: false, .. } if current == alias) =>
        {
            output.wrapped(star(), alias)
        }
        _ => output,
    }
}

/// A materialized reference must expose the alias its inline form would, or predicates and join conditions dangle.
pub(super) fn exposed_alias(id: RelationId, graph: &RelationGraph) -> Option<String> {
    match graph.get(id) {
        RelationNode::Read { alias, .. }
        | RelationNode::Owned { alias, .. }
        | RelationNode::UnionAll { alias, .. }
        | RelationNode::Bind { alias, .. }
        | RelationNode::Latest { alias, .. } => Some(alias.clone()),
        RelationNode::Filter { input, .. } => input
            .alias
            .clone()
            .or_else(|| exposed_alias(input.id, graph)),
        RelationNode::Project { input, .. } => {
            Some(input.alias.clone().unwrap_or_else(|| relation_name(id)))
        }
        RelationNode::Join { .. } => None,
    }
}

fn emit_node(id: RelationId, reads: &Reads<'_>, current: Option<RelationId>) -> RelationOutput {
    let graph = reads.graph;
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
        },
        RelationNode::Owned {
            input,
            owner,
            alias,
        } => {
            let mut reference = input.clone();
            reference.alias = Some(alias.clone());
            let mut output = emit_ref(&reference, reads, current);
            let bound = reads.scope.and_then(|scope| match owner {
                ReadOwner::Node(node) => scope.node(node),
                ReadOwner::Relationship(index) => scope.relationship(*index),
            });
            if let Some(proof) = bound {
                output.predicates.insert(0, scope_predicate(proof, alias));
            }
            output
        }
        RelationNode::Filter { input, predicates } => {
            let mut output = emit_ref(input, reads, current);
            output.predicates.extend(predicates.iter().map(predicate));
            output
        }
        RelationNode::Project { input, outputs } => emit_ref(input, reads, current).wrapped(
            projections(outputs),
            input.alias.clone().unwrap_or_else(|| relation_name(id)),
        ),
        RelationNode::UnionAll { inputs, alias } => RelationOutput {
            from: TableRef::union_all(
                inputs
                    .iter()
                    .map(|input| emit_ref(input, reads, current).query(star()))
                    .collect(),
                alias,
            ),
            predicates: vec![],
        },
        RelationNode::Join {
            left,
            right,
            endpoints,
            predicates,
        } => {
            let mut left = emit_ref(left, reads, current);
            let right = emit_ref(right, reads, current);
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
            emit_ref(input, reads, current).wrapped(star(), alias)
        }
        RelationNode::Latest {
            input,
            alias,
            sort_key,
            condition,
            limit,
        } => {
            let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
            let dedup = Query {
                order_by,
                limit_by,
                ..emit_ref(input, reads, current).query(star())
            };
            let picked = RelationOutput {
                from: TableRef::subquery(dedup, alias),
                predicates: condition.iter().map(predicate).collect(),
            };
            match limit {
                Some(limit) => RelationOutput {
                    from: TableRef::subquery(
                        Query {
                            limit: Some(*limit),
                            ..picked.query(star())
                        },
                        alias,
                    ),
                    predicates: vec![],
                },
                None => picked,
            }
        }
    }
}

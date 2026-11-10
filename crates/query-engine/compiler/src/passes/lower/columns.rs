//! Columns a materialized relation must select: ClickHouse stores every column an `AS MATERIALIZED`
//! body selects, so the body keeps only what the relation's consumers reference.

use std::collections::{BTreeSet, HashMap, HashSet};

use super::graph::exposed_alias;
use super::predicates::{column, predicate, projections};
use super::sql::latest_row_dedup;
use crate::ast::Expr;
use crate::ast::visit::visit_expressions;
use crate::passes::plan::graph::{ExecutionPlan, RelationId, RelationNode};
use crate::passes::plan::predicates::{Column, Predicate};
use crate::passes::plan::reuse::RelationUsage;

type Consumer = (Option<RelationId>, Option<String>);

/// An empty set means the relation must keep every column.
pub(super) fn required_columns(
    plan: &ExecutionPlan,
    usage: &RelationUsage,
    root: &[Column],
) -> HashMap<RelationId, BTreeSet<String>> {
    let graph = &plan.graph;
    let mut consumers: HashMap<RelationId, Vec<Consumer>> = HashMap::new();
    let mut keys: HashMap<RelationId, BTreeSet<String>> = HashMap::new();
    consumers.entry(plan.root).or_default().push((None, None));
    for (id, node) in graph.iter().filter(|(id, _)| usage.references(*id) > 0) {
        for input in node.inputs() {
            consumers
                .entry(input.id)
                .or_default()
                .push((Some(id), input.alias.clone()));
        }
        for leaf in node.predicates().iter().flat_map(Predicate::leaves) {
            if let Predicate::Membership {
                relation,
                keys: wanted,
                ..
            } = leaf
            {
                keys.entry(*relation)
                    .or_default()
                    .extend(wanted.iter().cloned());
            }
        }
    }
    let mut root_columns = root.to_vec();
    for select in projections(&plan.outputs) {
        referenced(&select.expr, &mut root_columns);
    }
    for binding in &plan.bindings {
        root_columns.push(Column::new(&binding.alias, &binding.column));
        root_columns.extend(binding.traversal_path.clone());
    }
    usage
        .materialized()
        .map(|id| {
            let mut aliases: HashSet<String> = exposed_alias(id, graph).into_iter().collect();
            let mut columns = Vec::new();
            let mut visited = HashSet::new();
            let mut queue = vec![id];
            while let Some(current) = queue.pop() {
                if !visited.insert(current) {
                    continue;
                }
                for (parent, alias) in consumers.get(&current).into_iter().flatten() {
                    aliases.extend(alias.clone());
                    let Some(parent) = parent else {
                        columns.extend(root_columns.iter().cloned());
                        continue;
                    };
                    let node = graph.get(*parent);
                    if matches!(node, RelationNode::UnionAll { .. }) {
                        return (id, BTreeSet::new());
                    }
                    aliases.extend(exposed_alias(*parent, graph));
                    for expr in node_expressions(node) {
                        referenced(&expr, &mut columns);
                    }
                    if !matches!(node, RelationNode::Project { .. }) {
                        queue.push(*parent);
                    }
                }
            }
            let mut needed: BTreeSet<String> = columns
                .into_iter()
                .filter(|column| aliases.contains(&column.source))
                .map(|column| column.name)
                .collect();
            needed.extend(keys.get(&id).into_iter().flatten().cloned());
            (id, needed)
        })
        .collect()
}

pub(super) fn referenced(expr: &Expr, out: &mut Vec<Column>) {
    let _ = visit_expressions(expr, &mut |expr| {
        if let Expr::Column { table, column } = expr {
            out.push(Column::new(table, column));
        }
        Ok(())
    });
}

fn node_expressions(node: &RelationNode) -> Vec<Expr> {
    let mut exprs: Vec<Expr> = node.predicates().iter().map(predicate).collect();
    match node {
        RelationNode::Join { endpoints, .. } => {
            exprs.extend([column(&endpoints.0), column(&endpoints.1)]);
        }
        RelationNode::Latest {
            alias, sort_key, ..
        } => {
            let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
            exprs.extend(order_by.into_iter().map(|order| order.expr));
            exprs.extend(limit_by.into_iter().flat_map(|(_, keys)| keys));
        }
        RelationNode::Project { outputs, .. } => {
            exprs.extend(projections(outputs).into_iter().map(|select| select.expr));
        }
        _ => {}
    }
    exprs
}

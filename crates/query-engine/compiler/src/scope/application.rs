use query_data_model::QueryDataModel;
use std::collections::HashMap;

use super::{QueryScope, scope_predicate};
use crate::ast::visit::{visit_queries_mut, visit_relations};
use crate::ast::{Expr, Identifier, Node, TableRef};
use crate::error::Result;

pub fn apply(
    node: &mut Node,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
) -> Result<()> {
    apply_with_bindings(node, scope, model, &HashMap::new())
}

pub fn apply_with_bindings(
    node: &mut Node,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
    bindings: &HashMap<Identifier, String>,
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    visit_queries_mut(query, &mut |query| {
        apply_scan_predicates(&query.from, &mut query.where_clause, scope, model, bindings);
        Ok(())
    })?;
    Ok(())
}

fn apply_scan_predicates(
    table: &TableRef,
    target: &mut Option<Expr>,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
    bindings: &HashMap<Identifier, String>,
) {
    visit_relations(table, &mut |relation| {
        if let TableRef::Scan {
            table,
            alias,
            relationship,
            ..
        } = relation
        {
            let relationship_proof = relationship
                .and_then(|index| scope.relationships.get(index))
                .and_then(Option::as_ref);
            let node_proof = model
                .table_path_scopable(table)
                .then(|| {
                    bindings
                        .get(alias)
                        .map(String::as_str)
                        .or_else(|| alias.name())
                        .and_then(|binding| scope.nodes.get(binding))
                })
                .flatten();

            if let Some(proof) = relationship_proof {
                append_predicate(target, scope_predicate(proof, alias));
            }
            if let Some(proof) = node_proof.filter(|proof| Some(*proof) != relationship_proof) {
                append_predicate(target, scope_predicate(proof, alias));
            }
        }
    });
}

fn append_predicate(target: &mut Option<Expr>, predicate: Expr) {
    *target = Some(match target.take() {
        Some(existing) => Expr::and(existing, predicate),
        None => predicate,
    });
}

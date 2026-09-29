use query_data_model::QueryDataModel;

use super::{QueryScope, resolved_scope_guard, scope_predicate};
use crate::ast::visit::{visit_queries_mut, visit_relations};
use crate::ast::{Expr, Node, TableRef};
use crate::error::Result;

pub fn apply(
    node: &mut Node,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    visit_queries_mut(query, &mut |query| {
        apply_scan_predicates(&query.from, &mut query.where_clause, scope, model);
        Ok(())
    })?;
    for requirement in &scope.requirements {
        append_predicate(&mut query.where_clause, resolved_scope_guard(requirement));
    }
    Ok(())
}

fn apply_scan_predicates(
    table: &TableRef,
    target: &mut Option<Expr>,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
) {
    visit_relations(table, &mut |relation| {
        if let TableRef::Scan {
            table,
            alias,
            relationship,
            ..
        } = relation
        {
            let proof = match relationship {
                Some(index) => scope.relationships.get(*index).and_then(Option::as_ref),
                None if model.table_path_scopable(table) => scope.nodes.get(alias),
                None => None,
            };
            if let Some(proof) = proof {
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

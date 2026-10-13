use query_data_model::OrbitQueryModel;

use super::{QueryScope, resolved_scope_guard, scope_predicate};
use crate::ast::visit::{visit_relations, visit_selects_mut};
use crate::ast::{Ast, Expr, TableRef};
use crate::error::Result;

pub fn apply<S>(
    ast: &mut Ast<S>,
    scope: &QueryScope,
    model: &(impl OrbitQueryModel + ?Sized),
) -> Result<()> {
    let Some(query) = ast.select_mut() else {
        return Ok(());
    };
    visit_selects_mut(query, &mut |select| {
        apply_scan_predicates(&select.from, &mut select.where_clause, scope, model);
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
    model: &(impl OrbitQueryModel + ?Sized),
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

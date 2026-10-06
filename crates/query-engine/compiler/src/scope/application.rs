use query_data_model::QueryDataModel;

use super::{QueryScope, resolved_scope_guard, scope_predicate};
use crate::ast::visit::{visit_queries_mut, visit_relations};
use crate::ast::{Expr, Node, TableRef};
use crate::config::BindingNames;
use crate::error::Result;
use query_data_model::bindings::QueryBindings;

pub fn apply(
    node: &mut Node,
    scope: &QueryScope,
    model: &(impl QueryDataModel + ?Sized),
    bindings: &mut QueryBindings,
    names: &mut BindingNames,
) -> Result<()> {
    let Node::Query(query) = node else {
        return Ok(());
    };
    visit_queries_mut(query, &mut |query| {
        let mut scans = Vec::new();
        visit_relations(&query.from, &mut |table| {
            if let TableRef::Scan {
                relation,
                relationship,
                ..
            } = table
            {
                scans.push((*relation, *relationship));
            }
        });
        for (relation, relationship) in scans {
            let table = names.source(bindings, relation)?;
            let proof = match relationship {
                Some(index) => scope.relationships.get(index).and_then(Option::as_ref),
                None if model.table_path_scopable(table) => {
                    scope.nodes.get(&names.relations[&relation])
                }
                None => None,
            };
            if let Some(proof) = proof {
                let column = crate::passes::lower::context::stored_column(
                    model,
                    bindings,
                    query.scope,
                    relation,
                    ontology::TRAVERSAL_PATH_COLUMN,
                )?;
                append_predicate(
                    &mut query.where_clause,
                    scope_predicate(proof, column, model, bindings, names)?,
                );
            }
        }
        Ok(())
    })?;
    for requirement in &scope.requirements {
        append_predicate(
            &mut query.where_clause,
            resolved_scope_guard(requirement, query.scope, model, bindings, names)?,
        );
    }
    Ok(())
}

fn append_predicate(target: &mut Option<Expr>, predicate: Expr) {
    *target = Some(match target.take() {
        Some(existing) => Expr::and(existing, predicate),
        None => predicate,
    });
}

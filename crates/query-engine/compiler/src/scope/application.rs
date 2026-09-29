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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::visit::{visit_expressions, visit_queries};
    use crate::ast::{JoinType, Query, SelectExpr};
    use crate::passes::{check, security};
    use crate::scope::{PathScopeId, ScopeProof, ScopeSource};
    use crate::types::SecurityContext;

    #[test]
    fn scope_and_security_keep_predicates_on_their_own_scans() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology)).unwrap();
        let proof = ScopeProof(vec![ScopeSource::Lookup {
            source_table: "gl_project".into(),
            key_column: "id".into(),
            value: PathScopeId::Numeric(1),
        }]);
        let scope = QueryScope {
            nodes: [("p".into(), proof.clone()), ("u".into(), proof.clone())].into(),
            relationships: vec![Some(proof.clone())],
            requirements: vec![],
        };
        let inner = Query {
            select: vec![SelectExpr::col("p", "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(Query {
            from: TableRef::join(
                JoinType::Inner,
                TableRef::subquery(inner, "p"),
                TableRef::scan("gl_user", "u"),
                Expr::lit(true),
            ),
            union_all: vec![Query {
                from: TableRef::scan("gl_edge", "edge").with_relationship(0),
                ..Default::default()
            }],
            ..Default::default()
        }));
        apply(&mut node, &scope, model.as_ref()).unwrap();
        let context = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        security::apply_security_context(&mut node, &context, model.as_ref()).unwrap();
        check::check_ast(&node, &context, model.as_ref()).unwrap();
        let Node::Query(query) = &node else {
            unreachable!()
        };
        assert!(query.where_clause.is_none());
        let mut lookups = 0;
        visit_queries(query, &mut |query| {
            if let TableRef::Scan { alias, .. } = &query.from {
                let authorization = Expr::func(
                    "startsWith",
                    vec![Expr::col(alias, "traversal_path"), Expr::string("1/")],
                );
                if alias == "_scope" {
                    lookups += 1;
                    assert_eq!(
                        query.where_clause,
                        Some(Expr::and(
                            authorization,
                            Expr::eq(Expr::col(alias, "id"), Expr::int(1))
                        ))
                    );
                } else {
                    let mut filters = Vec::new();
                    visit_expressions(query.where_clause.as_ref().unwrap(), &mut |expression| {
                        if let Expr::FuncCall { name, args } = expression
                            && name == "startsWith"
                        {
                            assert_eq!(args[0], Expr::col(alias, "traversal_path"));
                            filters.push(expression);
                        }
                        Ok(())
                    })?;
                    assert_eq!(filters.len(), 2, "{alias}");
                    assert!(filters.contains(&&authorization), "{alias}");
                }
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(lookups, 4);
    }
}

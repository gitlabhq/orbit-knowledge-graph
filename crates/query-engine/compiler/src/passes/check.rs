//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the AST is handed to codegen. Checks that every node table alias
//! has a `startsWith(alias.traversal_path, path)` predicate whose path literal
//! is derivable from the [`SecurityContext`] — catching both injection bugs
//! and path value mismatches.

use serde_json::Value;

use crate::ast::visit::visit_queries;
use crate::ast::{Expr, Node, Op, Query};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::{QueryError, Result};
use crate::passes::security::{SecurityContext, collect_node_aliases};
#[cfg(test)]
use ontology::Ontology;

const STARTS_WITH_FNAME: &str = "startsWith";

pub fn check_ast(
    node: &Node,
    ctx: &SecurityContext,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Result<()> {
    match node {
        Node::Query(q) => visit_queries(q, &mut |query| check_query(query, ctx, model)),
        Node::Insert(_) => Ok(()),
    }
}

fn check_query(
    q: &Query,
    ctx: &SecurityContext,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Result<()> {
    let aliases = collect_node_aliases(&q.from, model);
    for alias in &aliases {
        if !has_valid_path_filter(q.where_clause.as_ref(), alias, ctx) {
            return Err(QueryError::Security(format!(
                "post-check failed: alias '{alias}' missing valid traversal_path filter"
            )));
        }
    }

    Ok(())
}

fn has_valid_path_filter(expr: Option<&Expr>, alias: &str, ctx: &SecurityContext) -> bool {
    let Some(expr) = expr else { return false };
    match expr {
        Expr::Literal(Value::Bool(false))
        | Expr::Param {
            value: Value::Bool(false),
            ..
        } => true,
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => {
            has_valid_path_filter(Some(left), alias, ctx)
                || has_valid_path_filter(Some(right), alias, ctx)
        }
        Expr::BinaryOp {
            op: Op::Or,
            left,
            right,
        } => {
            has_valid_path_filter(Some(left), alias, ctx)
                && has_valid_path_filter(Some(right), alias, ctx)
        }
        Expr::FuncCall { name, args } if name == STARTS_WITH_FNAME => {
            let [Expr::Column { table, column }, path] = args.as_slice() else {
                return false;
            };
            if table != alias || column != TRAVERSAL_PATH_COLUMN {
                return false;
            }
            match path {
                Expr::Literal(Value::String(path))
                | Expr::Param {
                    value: Value::String(path),
                    ..
                } => ctx
                    .traversal_paths
                    .iter()
                    .any(|tp| path.starts_with(tp.path.as_str())),
                _ => false,
            }
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{SelectExpr, TableRef};

    fn apply_security(node: &mut Node, context: &SecurityContext, ontology: &Ontology) {
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology.clone())).unwrap();
        crate::passes::security::apply_security_context(node, context, model.as_ref()).unwrap();
    }

    fn check(node: &Node, context: &SecurityContext, ontology: &Ontology) -> Result<()> {
        let model = crate::data_model::clickhouse(std::sync::Arc::new(ontology.clone())).unwrap();
        check_ast(node, context, model.as_ref())
    }
    fn project_query(where_clause: Option<Expr>) -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause,
            limit: Some(10),
            ..Default::default()
        }))
    }

    #[test]
    fn passes_after_security_injection() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = Ontology::new().with_nodes(["Project"]);
        let mut node = project_query(None);
        apply_security(&mut node, &ctx, &ontology);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn security_injection_and_check_cover_nested_query_positions() {
        use crate::ast::{Cte, JoinType, OrderExpr};

        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = Ontology::new().with_nodes(["Project"]);
        for position in [
            "select",
            "where",
            "having",
            "group",
            "order",
            "limit_by",
            "join",
            "in_operand",
            "lambda",
            "nested_cte",
            "union",
            "derived",
            "table_union",
        ] {
            let inner = Query {
                select: vec![SelectExpr::col("protected", "id")],
                from: TableRef::scan("gl_project", "protected"),
                ..Default::default()
            };
            let scalar = Expr::Scalar(Box::new(inner.clone()));
            let mut query = Query {
                select: vec![SelectExpr::new(Expr::int(1), "result")],
                from: TableRef::scan("constant_source", "outer"),
                ..Default::default()
            };
            match position {
                "select" => query.select = vec![SelectExpr::new(scalar, "result")],
                "where" => query.where_clause = Some(Expr::eq(scalar, Expr::int(1))),
                "having" => query.having = Some(Expr::eq(scalar, Expr::int(1))),
                "group" => query.group_by.push(scalar),
                "order" => query.order_by.push(OrderExpr::asc(scalar)),
                "limit_by" => query.limit_by = Some((1, vec![scalar])),
                "join" => {
                    query.from = TableRef::join(
                        JoinType::Inner,
                        query.from,
                        TableRef::scan("constant_source", "other"),
                        Expr::eq(scalar, Expr::int(1)),
                    )
                }
                "in_operand" => {
                    query.where_clause = Some(Expr::InSelect {
                        expr: Box::new(scalar),
                        query: Box::new(Query {
                            from: TableRef::scan("constant_source", "lookup"),
                            ..Default::default()
                        }),
                    })
                }
                "lambda" => {
                    query.select = vec![SelectExpr::new(
                        Expr::func("arrayMap", vec![Expr::lambda("x", scalar)]),
                        "result",
                    )]
                }
                "nested_cte" => query.ctes.push(Cte::new(
                    "outer_cte",
                    Query {
                        ctes: vec![Cte::new("inner_cte", inner)],
                        from: TableRef::scan("inner_cte", "nested"),
                        ..Default::default()
                    },
                )),
                "union" => query.union_all.push(inner),
                "derived" => query.from = TableRef::subquery(inner, "derived"),
                "table_union" => query.from = TableRef::union_all(vec![inner], "arms"),
                _ => unreachable!(),
            }
            let outer_predicate = query.where_clause.clone();
            let mut node = Node::Query(Box::new(query));
            assert!(check(&node, &context, &ontology).is_err(), "{position}");
            apply_security(&mut node, &context, &ontology);
            check(&node, &context, &ontology).unwrap_or_else(|error| panic!("{position}: {error}"));
            let Node::Query(query) = &node else {
                unreachable!()
            };
            if outer_predicate.is_none() {
                assert!(query.where_clause.is_none(), "{position}");
            }
            let mut protected_scans = 0;
            visit_queries(query, &mut |query| {
                if matches!(&query.from, TableRef::Scan { alias, .. } if alias == "protected") {
                    protected_scans += 1;
                    assert_eq!(
                        query.where_clause,
                        Some(Expr::func(
                            "startsWith",
                            vec![
                                Expr::col("protected", "traversal_path"),
                                Expr::string("42/43/")
                            ],
                        )),
                        "{position}"
                    );
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(protected_scans, 1, "{position}");
        }
    }

    #[test]
    fn security_filters_physical_scans_without_filtering_their_wrappers() {
        use crate::ast::{Cte, JoinType};

        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = Ontology::load_embedded().unwrap();
        let inner = Query {
            select: vec![SelectExpr::col("p", "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        };
        let mut query = Query {
            ctes: vec![Cte::new("project_ids", inner.clone())],
            select: vec![SelectExpr::col("p", "id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "p"),
                TableRef::join(
                    JoinType::Inner,
                    TableRef::scan("project_ids", "ids"),
                    TableRef::subquery(inner, "derived"),
                    Expr::lit(true),
                ),
                Expr::lit(true),
            ),
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(query.clone()));
        let predicate = Some(Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        ));
        query.ctes[0].query.where_clause = predicate.clone();
        let TableRef::Join { right, .. } = &mut query.from else {
            unreachable!()
        };
        let TableRef::Join { right, .. } = right.as_mut() else {
            unreachable!()
        };
        let TableRef::Subquery { query: derived, .. } = right.as_mut() else {
            unreachable!()
        };
        derived.where_clause = predicate;

        apply_security(&mut node, &context, &ontology);
        assert_eq!(node, Node::Query(Box::new(query)));
        check(&node, &context, &ontology).unwrap();
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = project_query(Some(Expr::lit(true)));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn rejects_path_filters_that_do_not_restrict_every_result() {
        use crate::ast::Op;

        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = Ontology::new().with_nodes(["Project"]);
        let authorized = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        for predicate in [
            Expr::binary(Op::Or, authorized.clone(), Expr::lit(true)),
            Expr::unary(Op::Not, authorized.clone()),
            Expr::eq(authorized.clone(), Expr::lit(false)),
            Expr::binary(
                Op::Or,
                authorized.clone(),
                Expr::eq(Expr::col("p", "id"), Expr::int(1)),
            ),
            Expr::func(
                STARTS_WITH_FNAME,
                vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("42/")],
            ),
            Expr::func(
                STARTS_WITH_FNAME,
                vec![
                    Expr::string("42/43/"),
                    Expr::col("p", TRAVERSAL_PATH_COLUMN),
                ],
            ),
        ] {
            let node = project_query(Some(predicate));
            assert!(check(&node, &context, &ontology).is_err(), "{node:?}");
        }
        let guarded = Expr::binary(
            Op::Or,
            authorized.clone(),
            Expr::and(authorized, Expr::eq(Expr::col("p", "id"), Expr::int(1))),
        );
        assert!(check(&project_query(Some(guarded)), &context, &ontology).is_ok());
    }

    #[test]
    fn fails_with_wrong_path_literal() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let wrong_filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("99/")],
        );
        let node = project_query(Some(wrong_filter));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let ontology = Ontology::new().with_nodes(["Project"]);
        let mut node = project_query(None);
        apply_security(&mut node, &ctx, &ontology);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    /// An AND-chain containing `Bool(false)` short-circuits to zero rows, so
    /// the post-check must accept the query even when no per-alias
    /// `startsWith` filter is present. The security pass emits this shape
    /// when an alias has no eligible traversal paths (e.g. a Reporter-only
    /// user hitting an entity that requires Security Manager).
    #[test]
    fn accepts_bool_false_as_dead_alias_filter() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let dead = Expr::param(crate::ast::ChType::Bool, false);
        let node = project_query(Some(Expr::binary(Op::And, dead, Expr::lit(true))));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    /// A `col = false` comparison (or any other non-AND operator whose
    /// operand happens to be a boolean false literal) must NOT be treated
    /// as a proof that the alias is scoped. Bool(false) short-circuits the
    /// clause only when AND-chained into the top level. OR-ing or
    /// equality-ing against it leaves other rows reachable.
    ///
    /// Without this guard a user filter like `Project.archived = false`
    /// would bypass CheckPass defense-in-depth for any alias whose
    /// `startsWith` is missing — defeating the purpose of the post-check.
    #[test]
    fn rejects_bool_false_nested_inside_comparison() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let eq_false = Expr::binary(
            Op::Eq,
            Expr::col("p", "archived"),
            Expr::param(crate::ast::ChType::Bool, false),
        );
        let node = project_query(Some(eq_false));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "CheckPass must still require a startsWith for alias 'p'; got: {err}"
        );
    }

    /// `Bool(false)` OR-ed with anything is not a dead clause: the OR
    /// arms can still produce rows. Treating it as proof of scoping would
    /// leak data if the security pass forgot to emit a `startsWith`.
    #[test]
    fn rejects_bool_false_ored_with_true() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let or_expr = Expr::binary(
            Op::Or,
            Expr::param(crate::ast::ChType::Bool, false),
            Expr::lit(true),
        );
        let node = project_query(Some(or_expr));
        let ontology = ontology::Ontology::new();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "OR-ed Bool(false) must not satisfy the post-check; got: {err}"
        );
    }

    /// A `Bool(false)` buried in a deeply nested AND chain (the shape
    /// `Expr::and_all` typically emits for multi-alias queries) still
    /// short-circuits the clause and counts as a valid path filter.
    #[test]
    fn accepts_bool_false_in_nested_and_chain() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let dead_conjunct = Expr::binary(
            Op::And,
            Expr::binary(Op::Eq, Expr::col("p", "id"), Expr::lit(5)),
            Expr::param(crate::ast::ChType::Bool, false),
        );
        let where_expr = Expr::binary(
            Op::And,
            Expr::func(
                STARTS_WITH_FNAME,
                vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
            ),
            dead_conjunct,
        );
        let node = project_query(Some(where_expr));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    /// Inverse of the previous test: if the AND chain has no Bool(false)
    /// and no matching startsWith for the alias, the check must fail even
    /// when the clause contains `col = false` (which is NOT a dead
    /// conjunct).
    #[test]
    fn rejects_and_chain_with_col_eq_false_and_no_starts_with() {
        use crate::ast::Op;
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let where_expr = Expr::binary(
            Op::And,
            Expr::binary(
                Op::Eq,
                Expr::col("p", "archived"),
                Expr::param(crate::ast::ChType::Bool, false),
            ),
            Expr::binary(Op::Gt, Expr::col("p", "id"), Expr::lit(0)),
        );
        let node = project_query(Some(where_expr));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "col = false is NOT a dead conjunct — check must still require startsWith, got: {err}"
        );
    }

    #[test]
    fn skips_non_gl_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("c", "id"),
                alias: None,
            }],
            from: TableRef::scan("path_cte", "c"),
            where_clause: None,
            ..Default::default()
        }));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    fn wrap_in_subquery(inner: Query) -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("sq", "id"),
                alias: None,
            }],
            from: TableRef::subquery(inner, "sq"),
            where_clause: None,
            ..Default::default()
        }))
    }

    fn inner_project_query(where_clause: Option<Expr>) -> Query {
        Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause,
            ..Default::default()
        }
    }

    #[test]
    fn rejects_subquery_without_inner_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = wrap_in_subquery(inner_project_query(None));
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut inner = inner_project_query(None);
        let mut wrapped = Node::Query(Box::new(inner.clone()));
        apply_security(&mut wrapped, &ctx, &ontology::Ontology::new());
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        inner.where_clause = Some(filter);
        let node = wrap_in_subquery(inner);
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_aggregate_subquery_without_inner_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::func("count", vec![Expr::col("p", "id")]),
                alias: Some("cnt".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            group_by: vec![Expr::col("p", "namespace_id")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::func("count", vec![Expr::col("p", "id")]),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_aggregate_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::func("count", vec![Expr::col("p", "id")]),
                alias: Some("cnt".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(filter),
            group_by: vec![Expr::col("p", "namespace_id")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::func("count", vec![Expr::col("p", "id")]),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let ontology = ontology::Ontology::new().with_nodes(["Project"]);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn accepts_subquery_wrapping_non_sensitive_table() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::col("d", "value"),
                alias: None,
            }],
            from: TableRef::scan("dedup_cte", "d"),
            where_clause: None,
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let ontology = ontology::Ontology::new();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_union_all_arm_without_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![Expr::col("u", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
        );
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "u"),
            where_clause: Some(filter),
            union_all: vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: None,
                }],
                from: TableRef::scan("gl_project", "p"),
                where_clause: None,
                ..Default::default()
            }],
            ..Default::default()
        }));
        let ontology = ontology::Ontology::new();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_union_all_arms_with_security_filters() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "u"),
            where_clause: None,
            union_all: vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("p", "id"),
                    alias: None,
                }],
                from: TableRef::scan("gl_project", "p"),
                where_clause: None,
                ..Default::default()
            }],
            ..Default::default()
        }));
        let ontology = ontology::Ontology::new();
        apply_security(&mut node, &ctx, &ontology);
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_cte_with_sensitive_table_missing_filter() {
        use crate::ast::Cte;

        let node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "base",
                Query {
                    select: vec![SelectExpr {
                        expr: Expr::col("p", "id"),
                        alias: Some("node_id".into()),
                    }],
                    from: TableRef::scan("gl_project", "p"),
                    where_clause: None,
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr {
                expr: Expr::col("base", "node_id"),
                alias: None,
            }],
            from: TableRef::scan("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let ontology = ontology::Ontology::new();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "CTE scanning gl_project without filter should be rejected: {}",
            err
        );
    }

    #[test]
    fn accepts_cte_with_security_filter() {
        use crate::ast::Cte;

        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "base",
                Query {
                    select: vec![SelectExpr {
                        expr: Expr::col("p", "id"),
                        alias: Some("node_id".into()),
                    }],
                    from: TableRef::scan("gl_project", "p"),
                    where_clause: Some(filter),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr {
                expr: Expr::col("base", "node_id"),
                alias: None,
            }],
            from: TableRef::scan("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let ontology = ontology::Ontology::new();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }

    #[test]
    fn rejects_union_arm_missing_security_filter() {
        use ontology::constants::EDGE_TABLE;

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![Expr::col("e", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
        );
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("hop", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "e"),
                TableRef::Union {
                    queries: vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("p", "id"),
                            alias: None,
                        }],
                        from: TableRef::scan("gl_project", "p"),
                        where_clause: None,
                        ..Default::default()
                    }],
                    alias: "bad_union".into(),
                },
                Expr::lit(true),
            ),
            where_clause: Some(filter),
            ..Default::default()
        }));
        let ontology = ontology::Ontology::new();
        let err = check(&node, &ctx, &ontology).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "union arm scanning gl_project without filter should be rejected, got: {err}"
        );
    }

    #[test]
    fn accepts_union_arm_with_security_filter() {
        use ontology::constants::EDGE_TABLE;

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let outer_filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("e", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let arm_filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("e1", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("hop", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "e"),
                TableRef::Union {
                    queries: vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("e1", "source_id"),
                            alias: None,
                        }],
                        from: TableRef::scan(EDGE_TABLE, "e1"),
                        where_clause: Some(arm_filter),
                        ..Default::default()
                    }],
                    alias: "hop_e0".into(),
                },
                Expr::lit(true),
            ),
            where_clause: Some(outer_filter),
            ..Default::default()
        }));
        let ontology = ontology::Ontology::new();
        assert!(check(&node, &ctx, &ontology).is_ok());
    }
}

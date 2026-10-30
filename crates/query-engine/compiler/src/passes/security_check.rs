//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the AST is handed to codegen. Checks that every namespaced graph
//! table alias has a role-eligible traversal path predicate or a fail-closed
//! false predicate.

use query_data_model::{GitLabPolicy, OwnerScope};
use serde_json::Value;

use crate::ast::visit::{visit_queries, visit_relations};
use crate::ast::{Expr, Function, Node, Op, Query, TableRef};
use crate::error::{QueryError, Result};
use crate::passes::result_contract::ResultContext;
use crate::types::SecurityContext;

pub fn verify(
    node: &Node,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
    result_ctx: &ResultContext,
) -> Result<()> {
    match node {
        Node::Query(q) => {
            visit_queries(q, &mut |query| check_query(query, ctx, model))?;
            check_owner_scoped_nodes_read_their_table(q, ctx, model, result_ctx)
        }
        Node::Insert(_) => Ok(()),
    }
}

fn check_owner_scoped_nodes_read_their_table(
    q: &Query,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
    result_ctx: &ResultContext,
) -> Result<()> {
    let mut scanned = std::collections::HashSet::new();
    visit_queries(q, &mut |query| {
        visit_relations(&query.from, &mut |relation| {
            if let TableRef::Scan { table, .. } = relation {
                scanned.insert(table.clone());
            }
        });
        Ok(())
    })?;
    for node in result_ctx.nodes() {
        if model
            .entity_owner_scope(&node.entity_type)
            .is_none_or(|scope| !scope.gates(ctx.admin))
        {
            continue;
        }
        let table = model.entity_table(&node.entity_type).unwrap_or_default();
        if !scanned.contains(table) {
            return Err(QueryError::Security(format!(
                "post-check failed: owner-scoped node '{}' never reads its table '{table}'",
                node.alias
            )));
        }
    }
    Ok(())
}

fn check_query(
    q: &Query,
    ctx: &SecurityContext,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Result<()> {
    let mut error = None;
    visit_relations(&q.from, &mut |relation| {
        if error.is_some() {
            return;
        }
        if let TableRef::Scan { table, alias, .. } = relation {
            if table.is_empty() {
                return;
            }
            let Some(columns) = model.table_path_columns(table) else {
                error = Some(QueryError::Security(format!(
                    "post-check failed: unresolved table '{table}'"
                )));
                return;
            };
            for column in columns {
                let access_level = column
                    .entity
                    .and_then(|entity| model.query_authorization().required_access_level(entity))
                    .unwrap_or(ontology::RequiredRole::Reporter.as_access_level());
                let eligible_paths = ctx.paths_at_least(access_level);
                if !gates_every_row(q.where_clause.as_ref(), &|expr| {
                    is_path_filter(expr, alias, &column.name, &eligible_paths)
                }) {
                    error = Some(QueryError::Security(format!(
                        "post-check failed: column '{alias}.{}' missing valid traversal_path filter at access level {access_level}",
                        column.name
                    )));
                    return;
                }
            }
            if let Some(scope) = model
                .table_owner_scope(table)
                .filter(|scope| scope.gates(ctx.admin))
                && !gates_every_row(q.where_clause.as_ref(), &|expr| {
                    is_owner_filter(expr, alias, scope, ctx.user_id)
                })
            {
                error = Some(QueryError::Security(format!(
                    "post-check failed: column '{alias}.{}' missing owner filter",
                    scope.column
                )));
            }
        }
    });
    error.map_or(Ok(()), Err)
}

fn gates_every_row(expr: Option<&Expr>, accepts: &impl Fn(&Expr) -> bool) -> bool {
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
        } => gates_every_row(Some(left), accepts) || gates_every_row(Some(right), accepts),
        Expr::BinaryOp {
            op: Op::Or,
            left,
            right,
        } => {
            accepts(expr)
                || gates_every_row(Some(left), accepts) && gates_every_row(Some(right), accepts)
        }
        leaf => accepts(leaf),
    }
}

fn is_path_filter(
    expr: &Expr,
    alias: &str,
    column: &str,
    eligible_paths: &[&orbit_utils::traversal_path::TraversalPath],
) -> bool {
    let Expr::FuncCall {
        name: Function::StartsWith,
        args,
    } = expr
    else {
        return false;
    };
    let [target, path] = args.as_slice() else {
        return false;
    };
    if !is_column(target, alias, column) {
        return false;
    }
    match path {
        Expr::Literal(Value::String(path))
        | Expr::Param {
            value: Value::String(path),
            ..
        } => eligible_paths
            .iter()
            .any(|authorized| path.starts_with(authorized.as_str())),
        _ => false,
    }
}

fn is_owner_filter(expr: &Expr, alias: &str, scope: OwnerScope<'_>, user_id: Option<i64>) -> bool {
    let Expr::BinaryOp {
        op: Op::Or,
        left,
        right: tombstone,
    } = expr
    else {
        return false;
    };
    let Expr::BinaryOp {
        op: Op::Eq,
        left: owner,
        right: caller,
    } = left.as_ref()
    else {
        return false;
    };
    let caller_matches = matches!(
        caller.as_ref(),
        Expr::Literal(Value::Number(value)) | Expr::Param { value: Value::Number(value), .. }
            if user_id.is_some_and(|user_id| value.as_i64() == Some(user_id))
    );
    caller_matches
        && is_column(owner, alias, scope.column)
        && is_column(tombstone, alias, ontology::constants::DELETED_COLUMN)
}

fn is_column(expr: &Expr, alias: &str, column: &str) -> bool {
    matches!(expr, Expr::Column { table, column: actual } if table == alias && actual == column)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, LazyLock};

    use crate::ast::{SelectExpr, TableRef};
    use crate::constants::TRAVERSAL_PATH_COLUMN;
    use query_data_model::ClickHouseDataModel;

    fn model() -> &'static ClickHouseDataModel {
        static MODEL: LazyLock<ClickHouseDataModel> = LazyLock::new(|| {
            ClickHouseDataModel::derive(Arc::new(ontology::Ontology::load_embedded().unwrap()))
                .unwrap()
        });
        &MODEL
    }

    fn derive_model(ontology: ontology::Ontology) -> ClickHouseDataModel {
        ClickHouseDataModel::derive(Arc::new(ontology)).unwrap()
    }

    fn apply_security(node: &mut Node, context: &SecurityContext, model: &ClickHouseDataModel) {
        crate::passes::security::apply_security_context(node, context, model).unwrap();
    }

    fn check(node: &Node, context: &SecurityContext, model: &ClickHouseDataModel) -> Result<()> {
        verify(node, context, model, &ResultContext::new())
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
        let mut node = project_query(None);
        apply_security(&mut node, &ctx, model());
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn security_injection_and_check_cover_nested_query_positions() {
        use crate::ast::{Cte, JoinType, OrderExpr};

        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
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
                        TableRef::subquery(Query::default(), "outer"),
                        TableRef::subquery(Query::default(), "other"),
                        Expr::eq(scalar, Expr::int(1)),
                    )
                }
                "in_operand" => {
                    query.where_clause = Some(Expr::InSelect {
                        expr: Box::new(scalar),
                        query: Box::new(Query {
                            from: TableRef::subquery(Query::default(), "lookup"),
                            ..Default::default()
                        }),
                    })
                }
                "lambda" => {
                    query.select = vec![SelectExpr::new(
                        Expr::func(
                            Function::ArrayMap,
                            vec![
                                Expr::lambda("x", scalar),
                                Expr::func(Function::Array, vec![Expr::int(1)]),
                            ],
                        ),
                        "result",
                    )]
                }
                "nested_cte" => query.ctes.push(Cte::new(
                    "outer_cte",
                    Query {
                        ctes: vec![Cte::new("inner_cte", inner)],
                        from: TableRef::cte("inner_cte", "nested"),
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
            assert!(check(&node, &context, model()).is_err(), "{position}");
            apply_security(&mut node, &context, model());
            check(&node, &context, model()).unwrap_or_else(|error| panic!("{position}: {error}"));
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
                            Function::StartsWith,
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
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = project_query(Some(Expr::lit(true)));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn rejects_path_filters_that_do_not_restrict_every_result() {
        let context = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let column = Expr::col("p", TRAVERSAL_PATH_COLUMN);
        let starts_with = |left, right| Expr::func(Function::StartsWith, vec![left, right]);
        let authorized = starts_with(column.clone(), Expr::string("42/43/"));
        let condition = Expr::eq(Expr::col("p", "id"), Expr::int(1));
        for (predicate, valid) in [
            (
                Expr::binary(Op::Or, authorized.clone(), Expr::lit(true)),
                false,
            ),
            (Expr::unary(Op::Not, authorized.clone()), false),
            (Expr::eq(authorized.clone(), Expr::lit(false)), false),
            (
                Expr::binary(Op::Or, authorized.clone(), condition.clone()),
                false,
            ),
            (starts_with(column.clone(), Expr::string("42/")), false),
            (starts_with(Expr::string("42/43/"), column), false),
            (
                Expr::binary(Op::Or, authorized.clone(), Expr::and(authorized, condition)),
                true,
            ),
        ] {
            let node = project_query(Some(predicate));
            assert_eq!(check(&node, &context, model()).is_ok(), valid, "{node:?}");
        }
    }

    #[test]
    fn fails_with_wrong_path_literal() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let wrong_filter = Expr::func(
            Function::StartsWith,
            vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("99/")],
        );
        let node = project_query(Some(wrong_filter));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let mut node = project_query(None);
        apply_security(&mut node, &ctx, model());
        assert!(check(&node, &ctx, model()).is_ok());
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
        let dead = Expr::param(crate::ast::SqlType::Bool, false);
        let node = project_query(Some(Expr::binary(Op::And, dead, Expr::lit(true))));
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_reporter_path_for_security_manager_table() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let model = derive_model(
            ontology::Ontology::new()
                .with_nodes(["Vulnerability"])
                .with_redaction("Vulnerability", "vulnerability", "id")
                .with_redaction_role("Vulnerability", ontology::RequiredRole::SecurityManager),
        );
        let node = Node::Query(Box::new(Query {
            from: TableRef::scan("gl_vulnerability", "v"),
            where_clause: Some(Expr::func(
                Function::StartsWith,
                vec![
                    Expr::col("v", TRAVERSAL_PATH_COLUMN),
                    Expr::string("1/100/"),
                ],
            )),
            ..Default::default()
        }));

        let error = check(&node, &context, &model).unwrap_err();
        assert!(error.to_string().contains("access level 25"));
    }

    #[test]
    fn validates_role_floor_for_denormalized_join() {
        let context = SecurityContext::new(1, vec!["1/100/".into()]).unwrap();
        let model = derive_model(
            ontology::Ontology::load_embedded()
                .unwrap()
                .with_redaction_role("MergeRequest", ontology::RequiredRole::SecurityManager)
                .with_denormalized_join(
                    "reviewer_project",
                    &[
                        ("REVIEWER", "User", "MergeRequest", false),
                        ("IN_PROJECT", "MergeRequest", "Project", true),
                    ],
                ),
        );
        let node = Node::Query(Box::new(Query {
            from: TableRef::scan("gl_denorm_reviewer_project", "d"),
            where_clause: Some(Expr::func(
                Function::StartsWith,
                vec![
                    Expr::col("d", TRAVERSAL_PATH_COLUMN),
                    Expr::string("1/100/"),
                ],
            )),
            ..Default::default()
        }));

        let error = check(&node, &context, &model).unwrap_err();
        assert!(error.to_string().contains("d.t2_traversal_path"));
        assert!(error.to_string().contains("access level 25"));
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
            Expr::param(crate::ast::SqlType::Bool, false),
        );
        let node = project_query(Some(eq_false));
        let err = check(&node, &ctx, model()).unwrap_err();
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
            Expr::param(crate::ast::SqlType::Bool, false),
            Expr::lit(true),
        );
        let node = project_query(Some(or_expr));
        let err = check(&node, &ctx, model()).unwrap_err();
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
            Expr::param(crate::ast::SqlType::Bool, false),
        );
        let where_expr = Expr::binary(
            Op::And,
            Expr::func(
                Function::StartsWith,
                vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
            ),
            dead_conjunct,
        );
        let node = project_query(Some(where_expr));
        assert!(check(&node, &ctx, model()).is_ok());
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
                Expr::param(crate::ast::SqlType::Bool, false),
            ),
            Expr::binary(Op::Gt, Expr::col("p", "id"), Expr::lit(0)),
        );
        let node = project_query(Some(where_expr));
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter"),
            "col = false is NOT a dead conjunct — check must still require startsWith, got: {err}"
        );
    }

    #[test]
    fn skips_declared_ctes() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = Node::Query(Box::new(Query {
            ctes: vec![crate::ast::Cte::new("path_cte", Query::default())],
            select: vec![SelectExpr {
                expr: Expr::col("c", "id"),
                alias: None,
            }],
            from: TableRef::cte("path_cte", "c"),
            where_clause: None,
            ..Default::default()
        }));
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_unknown_graph_tables() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        for table in ["gl_unknown", "v42_gl_unknown"] {
            let node = Node::Query(Box::new(Query {
                from: TableRef::scan(table, "unknown"),
                ..Default::default()
            }));
            let error = check(&node, &ctx, model()).unwrap_err();
            assert!(error.to_string().contains(table));
        }
    }

    #[test]
    fn reports_first_invalid_scan() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = Node::Query(Box::new(Query {
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "first"),
                TableRef::scan("gl_merge_request", "second"),
                Expr::lit(true),
            ),
            ..Default::default()
        }));
        let error = check(&node, &ctx, model()).unwrap_err();
        assert!(error.to_string().contains("first.traversal_path"));
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
        let err = check(&node, &ctx, model()).unwrap_err();
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
        apply_security(&mut wrapped, &ctx, model());
        let filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        inner.where_clause = Some(filter);
        let node = wrap_in_subquery(inner);
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_aggregate_subquery_without_inner_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                alias: Some("cnt".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            group_by: vec![Expr::col("p", "namespace_id")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        let err = check(&node, &ctx, model()).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_aggregate_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let filter = Expr::func(
            Function::StartsWith,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let inner = Query {
            select: vec![SelectExpr {
                expr: Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                alias: Some("cnt".into()),
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(filter),
            group_by: vec![Expr::col("p", "namespace_id")],
            having: Some(Expr::binary(
                crate::ast::Op::Gt,
                Expr::aggregate(crate::input::AggFunction::Count, Some(Expr::col("p", "id"))),
                Expr::lit(1),
            )),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn accepts_subquery_wrapping_cte() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let inner = Query {
            ctes: vec![crate::ast::Cte::new("dedup_cte", Query::default())],
            from: TableRef::cte("dedup_cte", "d"),
            ..Default::default()
        };
        let node = wrap_in_subquery(inner);
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_union_all_arm_without_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            Function::StartsWith,
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
        let err = check(&node, &ctx, model()).unwrap_err();
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
        apply_security(&mut node, &ctx, model());
        assert!(check(&node, &ctx, model()).is_ok());
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
            from: TableRef::cte("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let err = check(&node, &ctx, model()).unwrap_err();
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
            Function::StartsWith,
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
            from: TableRef::cte("base", "b"),
            ..Default::default()
        }));

        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        assert!(check(&node, &ctx, model()).is_ok());
    }

    #[test]
    fn rejects_union_arm_missing_security_filter() {
        use ontology::constants::EDGE_TABLE;

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            Function::StartsWith,
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
        let err = check(&node, &ctx, model()).unwrap_err();
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
            Function::StartsWith,
            vec![
                Expr::col("e", TRAVERSAL_PATH_COLUMN),
                Expr::string("42/43/"),
            ],
        );
        let arm_filter = Expr::func(
            Function::StartsWith,
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
        assert!(check(&node, &ctx, model()).is_ok());
    }

    fn todo_model(bypass: ontology::OwnerBypass) -> ClickHouseDataModel {
        derive_model(
            ontology::Ontology::new()
                .with_nodes(["Todo", "Project"])
                .with_fields("Todo", [("user_id", ontology::DataType::Int)])
                .with_redaction("Todo", "todo", "id")
                .with_redaction_owner_column("Todo", "user_id")
                .with_redaction_owner_bypass("Todo", bypass),
        )
    }

    #[test]
    fn a_bound_owner_scoped_node_must_read_its_table() {
        let model = todo_model(ontology::OwnerBypass::Admin);
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_user_id(Some(7));
        let mut result_ctx = ResultContext::new();
        result_ctx.add_node("t", "Todo");
        let path_filter = |alias| {
            Expr::func(
                Function::StartsWith,
                vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
            )
        };
        let query = |table, alias, where_clause| {
            Node::Query(Box::new(Query {
                select: vec![SelectExpr::col(alias, "id")],
                from: TableRef::scan(table, alias),
                where_clause: Some(where_clause),
                ..Default::default()
            }))
        };

        let without_todo = query("gl_project", "p", path_filter("p"));
        let error = verify(&without_todo, &ctx, &model, &result_ctx).unwrap_err();
        assert!(
            error.to_string().contains("never reads its table"),
            "{error}"
        );
        assert!(
            verify(
                &without_todo,
                &ctx.clone().with_role(true, None),
                &model,
                &result_ctx
            )
            .is_ok()
        );
        assert!(
            verify(
                &without_todo,
                &ctx.clone().with_role(true, None),
                &todo_model(ontology::OwnerBypass::None),
                &result_ctx
            )
            .is_err()
        );
        let with_todo = query(
            "gl_todo",
            "t",
            Expr::and(
                path_filter("t"),
                Expr::binary(
                    Op::Or,
                    Expr::eq(Expr::col("t", "user_id"), Expr::int(7)),
                    Expr::col("t", "_deleted"),
                ),
            ),
        );
        assert!(verify(&with_todo, &ctx, &model, &result_ctx).is_ok());
    }

    #[test]
    fn owner_edge_table_needs_the_caller_filter() {
        let model = derive_model(
            ontology::Ontology::new()
                .with_nodes(["User", "Todo"])
                .with_edges(["HAS_TODO"])
                .with_edge_table("gl_todo_edge")
                .with_edge_for_table("HAS_TODO", "gl_todo_edge")
                .with_edge_table_owner("gl_todo_edge", "_owner_id", ontology::OwnerBypass::None),
        );
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_user_id(Some(7));
        let path_filter = Expr::func(
            Function::StartsWith,
            vec![Expr::col("e", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
        );
        let owned = |user_id| {
            Expr::binary(
                Op::Or,
                Expr::eq(Expr::col("e", "_owner_id"), Expr::int(user_id)),
                Expr::col("e", "_deleted"),
            )
        };
        let unowned_or_caller = Expr::binary(
            Op::Or,
            Expr::binary(
                Op::In,
                Expr::col("e", "_owner_id"),
                Expr::Param {
                    data_type: crate::ast::SqlType::Int64.to_array(),
                    value: Value::Array(vec![0.into(), 7.into()]),
                },
            ),
            Expr::col("e", "_deleted"),
        );
        let edge_query = |where_clause| {
            Node::Query(Box::new(Query {
                select: vec![SelectExpr::col("e", "target_id")],
                from: TableRef::scan("gl_todo_edge", "e"),
                where_clause: Some(where_clause),
                ..Default::default()
            }))
        };

        let error = check(&edge_query(path_filter.clone()), &ctx, &model).unwrap_err();
        assert!(
            error.to_string().contains("missing owner filter"),
            "{error}"
        );
        assert!(
            check(
                &edge_query(Expr::and(path_filter.clone(), owned(7))),
                &ctx,
                &model
            )
            .is_ok()
        );
        for wrong in [owned(8), unowned_or_caller] {
            assert!(
                check(
                    &edge_query(Expr::and(path_filter.clone(), wrong)),
                    &ctx,
                    &model
                )
                .is_err()
            );
        }
        assert!(
            check(
                &edge_query(path_filter.clone()),
                &ctx.clone().with_role(true, None),
                &model
            )
            .is_err()
        );
    }

    #[test]
    fn owner_scoped_alias_needs_the_caller_filter() {
        let model = todo_model(ontology::OwnerBypass::Admin);
        let ctx = SecurityContext::new(1, vec!["1/".into()])
            .unwrap()
            .with_user_id(Some(7));
        let path_filter = Expr::func(
            Function::StartsWith,
            vec![Expr::col("t", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
        );
        let owner_filter = |user_id| {
            Expr::binary(
                Op::Or,
                Expr::eq(Expr::col("t", "user_id"), Expr::int(user_id)),
                Expr::col("t", "_deleted"),
            )
        };
        let todo_query = |where_clause| {
            Node::Query(Box::new(Query {
                select: vec![SelectExpr::col("t", "id")],
                from: TableRef::scan("gl_todo", "t"),
                where_clause: Some(where_clause),
                ..Default::default()
            }))
        };

        let error = check(&todo_query(path_filter.clone()), &ctx, &model).unwrap_err();
        assert!(
            error.to_string().contains("missing owner filter"),
            "{error}"
        );
        assert!(
            check(
                &todo_query(Expr::and(path_filter.clone(), owner_filter(7))),
                &ctx,
                &model
            )
            .is_ok()
        );
        assert!(
            check(
                &todo_query(Expr::and(path_filter.clone(), owner_filter(8))),
                &ctx,
                &model
            )
            .is_err()
        );
        assert!(
            check(
                &todo_query(Expr::and(
                    path_filter.clone(),
                    Expr::eq(Expr::col("t", "user_id"), Expr::int(7))
                )),
                &ctx,
                &model
            )
            .is_err()
        );
        assert!(
            check(
                &todo_query(Expr::binary(Op::Or, owner_filter(7), Expr::lit(true))),
                &ctx,
                &model
            )
            .is_err()
        );
        assert!(
            check(
                &todo_query(path_filter.clone()),
                &ctx.clone().with_role(true, None),
                &todo_model(ontology::OwnerBypass::None)
            )
            .is_err()
        );
        assert!(
            check(
                &todo_query(path_filter.clone()),
                &ctx.clone().with_role(true, None),
                &model
            )
            .is_ok()
        );

        let mut node = todo_query(Expr::lit(true));
        apply_security(&mut node, &ctx, &model);
        assert!(check(&node, &ctx, &model).is_ok());
    }
}

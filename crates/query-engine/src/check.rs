//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the AST is handed to codegen. Checks that every node table alias
//! has a `startsWith(alias.traversal_path, path)` predicate whose path literal
//! is derivable from the [`SecurityContext`] — catching both injection bugs
//! and path value mismatches.

use serde_json::Value;

#[cfg(test)]
use crate::ast::ChType;
use crate::ast::{Expr, Node, Query, TableRef};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::error::{QueryError, Result};
use crate::security::{SecurityContext, collect_node_aliases};

const STARTS_WITH_FNAME: &str = "startsWith";

/// Verify post-compilation invariants on the final AST.
pub fn check_ast(node: &Node, ctx: &SecurityContext) -> Result<()> {
    match node {
        Node::Query(q) => {
            for cte in &q.ctes {
                check_query(&cte.query, ctx)?;
            }
            check_query(q, ctx)
        }
    }
}

fn check_query(q: &Query, ctx: &SecurityContext) -> Result<()> {
    let aliases = collect_node_aliases(&q.from);
    for alias in &aliases {
        if !has_valid_path_filter(q.where_clause.as_ref(), alias, ctx) {
            return Err(QueryError::Security(format!(
                "post-check failed: alias '{alias}' missing valid traversal_path filter"
            )));
        }
    }

    // Recurse into UNION ALL arms (defense-in-depth: currently only
    // recursive CTE arms which scan CTE names, not gl_* tables).
    for arm in &q.union_all {
        check_query(arm, ctx)?;
    }

    check_derived_tables_in_from(&q.from, ctx)
}

/// Recurse into derived tables (subqueries, UNION ALL arms) in a FROM clause
/// and verify each arm's query has valid security filters.
fn check_derived_tables_in_from(table_ref: &TableRef, ctx: &SecurityContext) -> Result<()> {
    match table_ref {
        TableRef::Subquery { query, .. } => check_query(query, ctx),
        TableRef::Union { queries, .. } => {
            for arm in queries {
                check_query(arm, ctx)?;
            }
            Ok(())
        }
        TableRef::Join { left, right, .. } => {
            check_derived_tables_in_from(left, ctx)?;
            check_derived_tables_in_from(right, ctx)
        }
        TableRef::Scan { .. } => Ok(()),
    }
}

/// Recursively checks whether `expr` contains a `startsWith(alias.traversal_path, path)`
/// call where `path` is a prefix of (or equal to) at least one path in the security context.
fn has_valid_path_filter(expr: Option<&Expr>, alias: &str, ctx: &SecurityContext) -> bool {
    let Some(expr) = expr else { return false };
    match expr {
        Expr::FuncCall { name, args } if name == STARTS_WITH_FNAME => {
            let has_column = args.iter().any(|a| {
                matches!(a, Expr::Column { table, column }
                    if table == alias && column == TRAVERSAL_PATH_COLUMN)
            });
            if !has_column {
                return false;
            }
            // The path literal must be a prefix of at least one SecurityContext path.
            // This covers both exact paths ("42/43/") and the lowest-common-prefix ("42/").
            args.iter().any(|a| match a {
                Expr::Literal(Value::String(path))
                | Expr::Param {
                    value: Value::String(path),
                    ..
                } => ctx
                    .traversal_paths
                    .iter()
                    .any(|tp| tp.starts_with(path.as_str())),
                _ => false,
            })
        }
        Expr::BinaryOp { left, right, .. } => {
            has_valid_path_filter(Some(left), alias, ctx)
                || has_valid_path_filter(Some(right), alias, ctx)
        }
        Expr::UnaryOp { expr: inner, .. } => has_valid_path_filter(Some(inner), alias, ctx),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{SelectExpr, TableRef};
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
        crate::security::apply_security_context(&mut node, &ctx).unwrap();
        assert!(check_ast(&node, &ctx).is_ok());
    }

    #[test]
    fn fails_without_any_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let node = project_query(Some(Expr::lit(true)));
        let err = check_ast(&node, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn fails_with_wrong_path_literal() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        // Manually construct a startsWith with a path not in the security context
        let wrong_filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::param(ChType::String, "99/"),
            ],
        );
        let node = project_query(Some(wrong_filter));
        let err = check_ast(&node, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_lowest_common_prefix() {
        let ctx = SecurityContext::new(42, vec!["42/10/".into(), "42/20/".into()]).unwrap();
        let mut node = project_query(None);
        crate::security::apply_security_context(&mut node, &ctx).unwrap();
        assert!(check_ast(&node, &ctx).is_ok());
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
        assert!(check_ast(&node, &ctx).is_ok());
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
        let err = check_ast(&node, &ctx).unwrap_err();
        assert!(
            err.to_string()
                .contains("missing valid traversal_path filter")
        );
    }

    #[test]
    fn accepts_subquery_with_inner_security_filter() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut inner = inner_project_query(None);
        crate::security::apply_security_context(&mut Node::Query(Box::new(inner.clone())), &ctx)
            .unwrap();
        // Re-extract the filtered query from the node
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("p", TRAVERSAL_PATH_COLUMN),
                Expr::param(ChType::String, "42/43/"),
            ],
        );
        inner.where_clause = Some(filter);
        let node = wrap_in_subquery(inner);
        assert!(check_ast(&node, &ctx).is_ok());
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
        let err = check_ast(&node, &ctx).unwrap_err();
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
                Expr::param(ChType::String, "42/43/"),
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
        assert!(check_ast(&node, &ctx).is_ok());
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
        assert!(check_ast(&node, &ctx).is_ok());
    }

    // ── UNION ALL arm checks ────────────────────────────────────────

    #[test]
    fn rejects_union_all_arm_without_security_filter() {
        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("u", TRAVERSAL_PATH_COLUMN),
                Expr::param(ChType::String, "1/"),
            ],
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
        let err = check_ast(&node, &ctx).unwrap_err();
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
        crate::security::apply_security_context(&mut node, &ctx).unwrap();
        assert!(check_ast(&node, &ctx).is_ok());
    }

    // ── CTE security check tests ────────────────────────────────────

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
        let err = check_ast(&node, &ctx).unwrap_err();
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
                Expr::param(ChType::String, "42/43/"),
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
        assert!(check_ast(&node, &ctx).is_ok());
    }

    // ── TableRef::Union structural enforcement ──────────────────────

    #[test]
    fn rejects_union_arm_missing_security_filter() {
        use ontology::constants::EDGE_TABLE;

        let ctx = SecurityContext::new(1, vec!["1/".into()]).unwrap();
        let filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("e", TRAVERSAL_PATH_COLUMN),
                Expr::param(ChType::String, "1/"),
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
        let err = check_ast(&node, &ctx).unwrap_err();
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
                Expr::param(ChType::String, "42/43/"),
            ],
        );
        let arm_filter = Expr::func(
            STARTS_WITH_FNAME,
            vec![
                Expr::col("e1", TRAVERSAL_PATH_COLUMN),
                Expr::param(ChType::String, "42/43/"),
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
        assert!(check_ast(&node, &ctx).is_ok());
    }
}

//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the AST is handed to codegen. Checks that every node table alias
//! has a `startsWith(alias.traversal_path, path)` predicate whose path literal
//! is derivable from the [`SecurityContext`] — catching both injection bugs
//! and path value mismatches.

use serde_json::Value;

use crate::ast::{Expr, Node, Query};
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
    Ok(())
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
                Expr::Literal(Value::String(path)) => ctx
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
            vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::lit("99/")],
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
}

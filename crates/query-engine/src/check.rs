//! Post-compilation safety checks.
//!
//! Runs after security filter injection to verify invariants that must hold
//! before the AST is handed to codegen:
//!
//! 1. **Security**: every node table alias has a valid `startsWith(traversal_path)`
//!    predicate derivable from the [`SecurityContext`].
//! 2. **Deduplication**: every `gl_*` table alias has argMax-based dedup applied
//!    (either inline via GROUP BY + HAVING, or via a dedup Subquery).

use serde_json::Value;

use crate::ast::{Expr, Node, Op, Query, TableRef};
use crate::constants::{GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN};
use crate::error::{QueryError, Result};
use crate::security::{SecurityContext, collect_node_aliases};
use ontology::constants::DELETED_COLUMN;

const STARTS_WITH_FNAME: &str = "startsWith";

/// Verify post-compilation invariants on the final AST.
pub fn check_ast(node: &Node, ctx: &SecurityContext) -> Result<()> {
    match node {
        Node::Query(q) => {
            for cte in &q.ctes {
                check_query(&cte.query, ctx)?;
                check_dedup_query(&cte.query)?;
            }
            check_query(q, ctx)?;
            check_dedup_query(q)?;
            Ok(())
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

// ─────────────────────────────────────────────────────────────────────────────
// Dedup validation
// ─────────────────────────────────────────────────────────────────────────────

fn check_dedup_query(q: &Query) -> Result<()> {
    let scans = collect_gl_scan_aliases(&q.from);
    if scans.is_empty() {
        return Ok(());
    }

    // Path 1: inline dedup — verify HAVING has argMax(_deleted) per alias
    if !q.group_by.is_empty() && q.having.is_some() {
        for alias in scans.keys() {
            if !has_deleted_filter(q.having.as_ref(), alias) {
                return Err(QueryError::Security(format!(
                    "dedup check failed: alias '{alias}' missing argMax(_deleted) in HAVING"
                )));
            }
        }
        return Ok(());
    }

    // Path 2: subquery dedup — verify each gl_* alias comes from a Subquery
    // with proper inner dedup
    let subquery_aliases = collect_subquery_aliases(&q.from);
    for alias in scans.keys() {
        if subquery_aliases.contains(alias) {
            continue;
        }
        return Err(QueryError::Security(format!(
            "dedup check failed: alias '{alias}' has no deduplication"
        )));
    }

    Ok(())
}

fn collect_gl_scan_aliases(table_ref: &TableRef) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    collect_gl_scan_aliases_inner(table_ref, &mut map);
    map
}

fn collect_gl_scan_aliases_inner(
    table_ref: &TableRef,
    map: &mut std::collections::HashMap<String, String>,
) {
    match table_ref {
        TableRef::Scan { table, alias, .. } if table.starts_with(GL_TABLE_PREFIX) => {
            map.insert(alias.clone(), table.clone());
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            collect_gl_scan_aliases_inner(left, map);
            collect_gl_scan_aliases_inner(right, map);
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

fn collect_subquery_aliases(table_ref: &TableRef) -> Vec<String> {
    match table_ref {
        TableRef::Subquery { alias, query } => {
            if query.having.is_some() && !query.group_by.is_empty() {
                vec![alias.clone()]
            } else {
                vec![]
            }
        }
        TableRef::Join { left, right, .. } => {
            let mut v = collect_subquery_aliases(left);
            v.extend(collect_subquery_aliases(right));
            v
        }
        _ => vec![],
    }
}

fn has_deleted_filter(expr: Option<&Expr>, alias: &str) -> bool {
    let Some(expr) = expr else { return false };
    match expr {
        Expr::BinaryOp {
            op: Op::Eq,
            left,
            right,
        } => {
            is_argmax_deleted(left, alias)
                && matches!(right.as_ref(), Expr::Literal(v) if v == &serde_json::Value::Bool(false))
        }
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => has_deleted_filter(Some(left), alias) || has_deleted_filter(Some(right), alias),
        _ => false,
    }
}

fn is_argmax_deleted(expr: &Expr, alias: &str) -> bool {
    matches!(
        expr,
        Expr::FuncCall { name, args }
            if name == "argMax"
            && args.len() == 2
            && matches!(&args[0], Expr::Column { table, column } if table == alias && column == DELETED_COLUMN)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::SelectExpr;

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
        let ontology = ontology::Ontology::load_embedded().unwrap();
        crate::deduplicate::deduplicate(&mut node, &ontology).unwrap();
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
        let ontology = ontology::Ontology::load_embedded().unwrap();
        crate::deduplicate::deduplicate(&mut node, &ontology).unwrap();
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

    // ── Dedup validation ────────────────────────────────────────────────

    #[test]
    fn dedup_passes_after_inline() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }));

        let ontology = ontology::Ontology::load_embedded().unwrap();
        crate::deduplicate::deduplicate(&mut node, &ontology).unwrap();
        let Node::Query(q) = &node;
        check_dedup_query(q).unwrap();
    }

    #[test]
    fn dedup_passes_after_subquery() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(
                Expr::func("COUNT", vec![Expr::col("p", "id")]),
                "cnt",
            )],
            from: TableRef::scan("gl_project", "p"),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        }));

        let ontology = ontology::Ontology::load_embedded().unwrap();
        crate::deduplicate::deduplicate(&mut node, &ontology).unwrap();
        let Node::Query(q) = &node;
        check_dedup_query(q).unwrap();
    }

    #[test]
    fn dedup_fails_without_dedup() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        };

        let err = check_dedup_query(&q).unwrap_err();
        assert!(err.to_string().contains("dedup check failed"));
    }

    #[test]
    fn dedup_skips_non_gl_tables() {
        let q = Query {
            select: vec![SelectExpr::new(Expr::col("c", "id"), "id")],
            from: TableRef::scan("custom_table", "c"),
            ..Default::default()
        };

        check_dedup_query(&q).unwrap();
    }
}

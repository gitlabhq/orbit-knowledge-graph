//! Security filter injection for multi-tenant isolation.
//!
//! Injects traversal_path filters on all node table scans.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(LCP) AND (startsWith(p1) OR startsWith(p2) OR ...)`

use crate::ast::{ChType, Expr, Node, Query, TableRef};
use crate::constants::{GL_TABLE_PREFIX, SKIP_SECURITY_FILTER_TABLES, TRAVERSAL_PATH_COLUMN};
use crate::envs::HasSecurityCtx;
use crate::error::{QueryError, Result};
use crate::pipeline::{CompilerContext, CompilerPass, PipelineEnv};
pub use crate::types::SecurityContext;

/// Pipeline pass: injects `startsWith(traversal_path, ...)` security filters.
/// Reads security context from the environment.
pub struct SecurityPass;

impl<E: PipelineEnv + HasSecurityCtx> CompilerPass<E> for SecurityPass {
    const NAME: &'static str = "security";

    fn run(&self, ctx: &mut CompilerContext<E>) -> Result<()> {
        let security_ctx = ctx.env().security_ctx().clone();
        let node = ctx.node.as_mut().expect("node must exist after enforce");
        apply_security_context(node, &security_ctx)?;
        Ok(())
    }
}

/// Inject security filters into an AST node (mutates in place).
pub fn apply_security_context(node: &mut Node, ctx: &SecurityContext) -> Result<()> {
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                apply_to_query(&mut cte.query, ctx)?;
            }
            apply_to_query(q, ctx)
        }
    }
}

fn apply_to_query(q: &mut Query, ctx: &SecurityContext) -> Result<()> {
    let aliases = collect_node_aliases(&q.from);
    if !aliases.is_empty() {
        let security_conds = aliases
            .iter()
            .map(|a| build_path_filter(a, &ctx.traversal_paths));
        q.where_clause = Expr::and_all(
            security_conds
                .map(Some)
                .chain(std::iter::once(q.where_clause.take())),
        );
    }

    // Recurse into derived tables (UNION ALL arms, subqueries) in FROM
    apply_security_to_from(&mut q.from, ctx)?;

    // Recurse into top-level UNION ALL arms
    for arm in &mut q.union_all {
        apply_to_query(arm, ctx)?;
    }

    Ok(())
}

fn build_path_filter(alias: &str, paths: &[String]) -> Expr {
    match paths.len() {
        0 => Expr::param(ChType::Bool, false),
        1 => starts_with_expr(alias, &paths[0]),
        _ => {
            let prefix = lowest_common_prefix(paths);
            let prefix_filter = starts_with_expr(alias, &prefix);
            match Expr::or_all(paths.iter().map(|p| Some(starts_with_expr(alias, p)))) {
                Some(or_filters) => Expr::and(prefix_filter, or_filters),
                None => prefix_filter,
            }
        }
    }
}

/// Find the lowest common path prefix.
fn lowest_common_prefix(paths: &[String]) -> String {
    if paths.is_empty() {
        return String::new();
    }

    let segments: Vec<Vec<&str>> = paths
        .iter()
        .map(|p| p.trim_end_matches('/').split('/').collect())
        .collect();

    let first = &segments[0];
    let common_len = (0..first.len())
        .take_while(|&i| segments.iter().all(|s| s.get(i) == first.get(i)))
        .count();

    if common_len == 0 {
        String::new()
    } else {
        format!("{}/", first[..common_len].join("/"))
    }
}

fn starts_with_expr(alias: &str, path: &str) -> Expr {
    Expr::func(
        "startsWith",
        vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string(path)],
    )
}

pub(crate) fn collect_node_aliases(table_ref: &TableRef) -> Vec<String> {
    match table_ref {
        TableRef::Scan { table, alias, .. } if should_apply_security_filter(table) => {
            vec![alias.clone()]
        }
        TableRef::Scan { .. } => vec![],
        TableRef::Join { left, right, .. } => {
            let mut aliases = collect_node_aliases(left);
            aliases.extend(collect_node_aliases(right));
            aliases
        }
        // Derived tables don't have traversal_path columns themselves.
        // Their arms get security filters via apply_security_to_from.
        TableRef::Union { .. } | TableRef::Subquery { .. } => vec![],
    }
}

/// Recurse into derived tables (UNION ALL arms, subqueries) inside a FROM
/// clause and apply security filters to each arm's query.
fn apply_security_to_from(table_ref: &mut TableRef, ctx: &SecurityContext) -> Result<()> {
    match table_ref {
        TableRef::Union { queries, .. } => {
            for arm in queries {
                apply_to_query(arm, ctx)?;
            }
        }
        TableRef::Subquery { query, .. } => {
            apply_to_query(query, ctx)?;
        }
        TableRef::Join { left, right, .. } => {
            apply_security_to_from(left, ctx)?;
            apply_security_to_from(right, ctx)?;
        }
        TableRef::Scan { .. } => {}
    }
    Ok(())
}

/// Determines if a table should have traversal path security filters applied.
fn should_apply_security_filter(table: &str) -> bool {
    // Only apply to actual node tables and edge table (GL_TABLE_PREFIX)
    // This excludes CTEs like "path_cte" which don't have traversal_path
    if !table.starts_with(GL_TABLE_PREFIX) {
        return false;
    }
    // Skip tables for entities whose visibility is relationship-based
    if SKIP_SECURITY_FILTER_TABLES.contains(&table) {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{JoinType, Op, SelectExpr};
    use ontology::constants::EDGE_TABLE;

    fn simple_query() -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("p", "id"),
                alias: None,
            }],
            from: TableRef::scan("gl_project", "p"),
            where_clause: None,
            limit: Some(10),
            ..Default::default()
        }))
    }

    #[test]
    fn traversal_path_validation() {
        // Valid paths (org_id must match first segment)
        assert!(SecurityContext::new(1, vec!["1/".into()]).is_ok());
        assert!(SecurityContext::new(1, vec!["1/2/3/".into()]).is_ok());
        assert!(SecurityContext::new(42, vec!["42/100/".into()]).is_ok());

        // Invalid: org_id mismatch
        assert!(SecurityContext::new(1, vec!["42/".into()]).is_err());
        assert!(SecurityContext::new(99, vec!["1/2/3/".into()]).is_err());

        // Invalid: format errors
        assert!(SecurityContext::new(1, vec!["1/2/3".into()]).is_err()); // missing trailing slash
        assert!(SecurityContext::new(1, vec!["".into()]).is_err()); // empty
        assert!(SecurityContext::new(1, vec!["abc/".into()]).is_err()); // non-numeric
        assert!(SecurityContext::new(1, vec!["1/abc/2/".into()]).is_err()); // mixed
        assert!(SecurityContext::new(1, vec!["99999999999999999999999999999/".into()]).is_err()); // exceeds i64
        assert!(SecurityContext::new(1, vec!["-1/".into()]).is_err()); // negative
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Path filter generation tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn single_path_uses_starts_with() {
        let expr = build_path_filter("u", &["42/43/".into()]);
        assert!(matches!(expr, Expr::FuncCall { name, .. } if name == "startsWith"));
    }

    #[test]
    fn multiple_paths_uses_prefix_and_or_starts_with() {
        let expr = build_path_filter("u", &["1/2/4/".into(), "1/2/5/".into()]);
        // Should be: startsWith(..., '1/2/') AND (startsWith(..., '1/2/4/') OR startsWith(..., '1/2/5/'))
        assert!(matches!(expr, Expr::BinaryOp { op: Op::And, .. }));
    }

    #[test]
    fn lowest_common_prefix_finds_shared_path() {
        assert_eq!(
            lowest_common_prefix(&["1/2/4/".into(), "1/2/5/".into()]),
            "1/2/"
        );
        assert_eq!(lowest_common_prefix(&["1/2/".into(), "1/3/".into()]), "1/");
        assert_eq!(lowest_common_prefix(&["1/".into(), "2/".into()]), "");
        assert_eq!(lowest_common_prefix(&["42/".into()]), "42/");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Security injection tests
    // ─────────────────────────────────────────────────────────────────────────

    #[test]
    fn inject_adds_security_to_simple_query() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = simple_query();
        apply_security_context(&mut node, &ctx).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));
    }

    #[test]
    fn inject_filters_edge_table() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan(EDGE_TABLE, "e"),
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_some()));
    }

    #[test]
    fn inject_includes_edge_table() {
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_project", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::eq(Expr::col("p", "id"), Expr::col("e", "source")),
        );

        let aliases = collect_node_aliases(&from);
        assert_eq!(aliases, vec!["p", "e"]);
    }

    #[test]
    fn inject_skips_user_table() {
        // User visibility is determined through MEMBER_OF, not traversal path
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("gl_user", "u"),
            TableRef::scan("gl_merge_request", "mr"),
            Expr::lit(true),
        );

        let aliases = collect_node_aliases(&from);
        // Should only include mr, not u (gl_user is skipped)
        assert_eq!(aliases, vec!["mr"]);
    }

    #[test]
    fn should_apply_security_filter_skips_user() {
        assert!(!should_apply_security_filter("gl_user"));
        assert!(should_apply_security_filter(EDGE_TABLE));
        assert!(should_apply_security_filter("gl_project"));
        assert!(should_apply_security_filter("gl_merge_request"));
    }

    #[test]
    fn should_apply_security_filter_skips_ctes() {
        // CTEs like path_cte don't have traversal_path column
        assert!(!should_apply_security_filter("path_cte"));
        assert!(!should_apply_security_filter("some_cte"));
        // Only gl_ prefixed tables should have security filters
        assert!(!should_apply_security_filter("nodes"));
    }

    #[test]
    fn union_aliases_are_not_collected() {
        let from = TableRef::union_all(
            vec![Query {
                select: vec![SelectExpr {
                    expr: Expr::col("e", "source_id"),
                    alias: None,
                }],
                from: TableRef::scan(EDGE_TABLE, "e"),
                ..Default::default()
            }],
            "hop_e0",
        );
        let aliases = collect_node_aliases(&from);
        assert!(aliases.is_empty());
    }

    #[test]
    fn inject_recurses_into_union_from_arms() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("outer_e", "source_id"),
                alias: None,
            }],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan(EDGE_TABLE, "outer_e"),
                TableRef::union_all(
                    vec![Query {
                        select: vec![SelectExpr {
                            expr: Expr::col("e1", "source_id"),
                            alias: None,
                        }],
                        from: TableRef::scan(EDGE_TABLE, "e1"),
                        where_clause: None,
                        ..Default::default()
                    }],
                    "hop_e0",
                ),
                Expr::lit(true),
            ),
            where_clause: None,
            ..Default::default()
        }));

        apply_security_context(&mut node, &ctx).unwrap();

        let Node::Query(q) = &node;
        assert!(
            q.where_clause.is_some(),
            "outer query should have security filter on outer_e"
        );

        // The union arm scanning gl_edge should also get a filter
        if let TableRef::Join { right, .. } = &q.from {
            if let TableRef::Union { queries, .. } = right.as_ref() {
                assert!(
                    queries[0].where_clause.is_some(),
                    "UNION ALL arm should have security filter applied"
                );
            } else {
                panic!("expected Union");
            }
        } else {
            panic!("expected Join");
        }
    }

    #[test]
    fn inject_recurses_into_union_all_arms() {
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

        apply_security_context(&mut node, &ctx).unwrap();

        let Node::Query(q) = &node;
        assert!(
            q.where_clause.is_some(),
            "base query should have security filter"
        );
        assert_eq!(q.union_all.len(), 1);
        assert!(
            q.union_all[0].where_clause.is_some(),
            "UNION ALL arm should have security filter"
        );
    }
}

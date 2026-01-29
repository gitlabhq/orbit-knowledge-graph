//! Security filter injection for multi-tenant isolation.
//!
//! Injects traversal_path filters on all node table scans.
//! The org_id is encoded as the first segment of each path, validated at construction.
//!
//! Path filtering strategy:
//! - 1 path: `startsWith(path)`
//! - 2+ paths: `startsWith(LCP) AND (startsWith(p1) OR startsWith(p2) OR ...)`

use crate::ast::{Expr, Node, Op, Query, TableRef};
use crate::error::{QueryError, Result};
use once_cell::sync::Lazy;
use ontology::EDGE_TABLE;
use regex::Regex;

static TRAVERSAL_PATH_COLUMN: &str = "traversal_path";

/// Matches paths like "1/", "1/2/", "123/456/789/"
static TRAVERSAL_PATH_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^(\d+/)+$").expect("valid regex"));

/// Tables that should NOT have traversal path security filters applied.
/// These are entities whose visibility is determined through relationships
/// (e.g., MEMBER_OF) rather than direct path hierarchy.
/// TODO!!! : This table name needs to be derived directly from the ontology.
const SKIP_SECURITY_FILTER_TABLES: &[&str] = &["kg_user"];

/// Security context for request-level isolation.
#[derive(Debug, Clone)]
pub struct SecurityContext {
    pub org_id: i64,
    pub traversal_paths: Vec<String>,
}

impl SecurityContext {
    /// Create a new security context with validation.
    ///
    /// Validates that:
    /// - Each path matches the format `int/int/.../`
    /// - Each segment fits in i64
    /// - The first segment of each path equals org_id
    pub fn new(org_id: i64, traversal_paths: Vec<String>) -> Result<Self> {
        for path in &traversal_paths {
            validate_traversal_path(path, org_id)?;
        }
        Ok(Self {
            org_id,
            traversal_paths,
        })
    }
}

fn validate_traversal_path(path: &str, org_id: i64) -> Result<()> {
    if !TRAVERSAL_PATH_REGEX.is_match(path) {
        return Err(QueryError::Security(format!(
            "invalid traversal_path format: '{path}' (expected pattern like '1/2/3/')"
        )));
    }

    let segments: Vec<&str> = path.trim_end_matches('/').split('/').collect();

    for segment in &segments {
        segment.parse::<i64>().map_err(|_| {
            QueryError::Security(format!(
                "traversal_path segment '{segment}' exceeds i64 range"
            ))
        })?;
    }

    let first_segment: i64 = segments[0].parse().expect("validated above");
    if first_segment != org_id {
        return Err(QueryError::Security(format!(
            "traversal_path '{path}' does not start with org_id {org_id}"
        )));
    }

    Ok(())
}

/// Inject security filters into an AST node (mutates in place).
pub fn apply_security_context(node: &mut Node, ctx: &SecurityContext) -> Result<()> {
    match node {
        Node::Query(q) => apply_to_query(q, ctx),
        Node::RecursiveCte(cte) => {
            apply_to_query(&mut cte.base, ctx)?;
            apply_to_query(&mut cte.recursive, ctx)?;
            apply_to_query(&mut cte.final_query, ctx)
        }
    }
}

fn apply_to_query(q: &mut Query, ctx: &SecurityContext) -> Result<()> {
    let aliases = collect_node_aliases(&q.from);
    if aliases.is_empty() {
        return Ok(());
    }
    let security_conds = aliases
        .iter()
        .map(|a| build_path_filter(a, &ctx.traversal_paths));
    // Note: Security predicates always applied first for short-circuit filtering
    q.where_clause = Expr::and_all(
        security_conds
            .map(Some)
            .chain(std::iter::once(q.where_clause.take())),
    );
    Ok(())
}

fn build_path_filter(alias: &str, paths: &[String]) -> Expr {
    match paths.len() {
        0 => Expr::lit(false),
        1 => starts_with_expr(alias, &paths[0]),
        _ => {
            let prefix = lowest_common_prefix(paths);
            let prefix_filter = starts_with_expr(alias, &prefix);
            match Expr::or_all(paths.iter().map(|p| Some(starts_with_expr(alias, p)))) {
                Some(or_filters) => Expr::binary(Op::And, prefix_filter, or_filters),
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
        vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::lit(path)],
    )
}

fn collect_node_aliases(table_ref: &TableRef) -> Vec<String> {
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
    }
}

/// Determines if a table should have traversal path security filters applied.
fn should_apply_security_filter(table: &str) -> bool {
    // Skip edge table
    if table == EDGE_TABLE {
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
    use crate::ast::{JoinType, SelectExpr};

    fn simple_query() -> Node {
        Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("u", "id"),
                alias: None,
            }],
            from: TableRef::scan("nodes_user", "u"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(10),
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
    fn inject_noop_when_no_node_tables() {
        let ctx = SecurityContext::new(42, vec!["42/43/".into()]).unwrap();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr {
                expr: Expr::col("e", "id"),
                alias: None,
            }],
            from: TableRef::scan(EDGE_TABLE, "e"),
            where_clause: None,
            group_by: vec![],
            order_by: vec![],
            limit: Some(10),
        }));

        apply_security_context(&mut node, &ctx).unwrap();
        assert!(matches!(node, Node::Query(q) if q.where_clause.is_none()));
    }

    #[test]
    fn inject_skips_edge_table() {
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("kg_project", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::eq(Expr::col("p", "id"), Expr::col("e", "source")),
        );

        let aliases = collect_node_aliases(&from);
        assert_eq!(aliases, vec!["p"]);
    }

    #[test]
    fn inject_skips_user_table() {
        // User visibility is determined through MEMBER_OF, not traversal path
        let from = TableRef::join(
            JoinType::Inner,
            TableRef::scan("kg_user", "u"),
            TableRef::scan("kg_mergerequest", "mr"),
            Expr::lit(true),
        );

        let aliases = collect_node_aliases(&from);
        // Should only include mr, not u
        assert_eq!(aliases, vec!["mr"]);
    }

    #[test]
    fn should_apply_security_filter_skips_user() {
        assert!(!should_apply_security_filter("kg_user"));
        assert!(!should_apply_security_filter(EDGE_TABLE));
        assert!(should_apply_security_filter("kg_project"));
        assert!(should_apply_security_filter("kg_mergerequest"));
    }
}

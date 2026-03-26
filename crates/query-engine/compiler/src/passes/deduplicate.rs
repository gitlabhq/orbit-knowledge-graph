//! Row deduplication pass for ReplacingMergeTree node tables.
//!
//! ClickHouse's `ReplacingMergeTree` deduplicates during background merges,
//! but between merges queries can see stale duplicates. This pass wraps
//! every node table scan in a subquery with `GROUP BY id` + `argMax`.
//!
//! Edge tables are excluded — their full-tuple ORDER BY makes RMT dedup
//! effective, and wrapping them would kill LIMIT pushdown.
//!
//! ```sql
//! -- before
//! FROM gl_merge_request AS mr WHERE mr.state = 'merged'
//!
//! -- after
//! FROM (SELECT id, argMax(state, _version) AS state
//!       FROM gl_merge_request AS mr
//!       GROUP BY id
//!       HAVING argMax(_deleted, _version) = false) AS mr
//! WHERE mr.state = 'merged'
//! ```
//!
//! The pass doesn't touch WHERE — property filters stay outer naturally,
//! security injects `startsWith(traversal_path)` into inner queries via
//! its own subquery recursion, and ClickHouse pushes `id` predicates
//! through `GROUP BY id` automatically.
//!
//! Refs: <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/308>

use std::collections::{HashMap, HashSet};

use ontology::constants::{DELETED_COLUMN, EDGE_TABLE, GL_TABLE_PREFIX, VERSION_COLUMN};

use crate::ast::{Expr, Node, Query, SelectExpr, TableRef};

fn is_node_table(table: &str) -> bool {
    table.starts_with(GL_TABLE_PREFIX) && table != EDGE_TABLE
}

/// Apply row deduplication to all node table scans in the AST.
pub fn deduplicate(node: &mut Node) {
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                dedup_query(&mut cte.query);
            }
            dedup_query(q);
        }
    }
}

fn dedup_query(q: &mut Query) {
    // Bottom-up: derived tables first, then wrap scans at this level.
    visit_derived_tables(&mut q.from);
    for arm in &mut q.union_all {
        dedup_query(arm);
    }
    let used = used_columns(q);
    wrap_node_scans(&mut q.from, &used);
}

fn visit_derived_tables(from: &mut TableRef) {
    match from {
        TableRef::Join { left, right, .. } => {
            visit_derived_tables(left);
            visit_derived_tables(right);
        }
        TableRef::Union { queries, .. } => queries.iter_mut().for_each(dedup_query),
        TableRef::Subquery { query, .. } => dedup_query(query),
        TableRef::Scan { .. } => {}
    }
}

// ─── Column collection ───────────────────────────────────────────────────

/// Collect `alias → {columns}` referenced at this query level.
fn used_columns(q: &Query) -> HashMap<String, HashSet<String>> {
    let mut out = HashMap::new();
    for sel in &q.select {
        walk_expr(&sel.expr, &mut out);
    }
    if let Some(w) = &q.where_clause {
        walk_expr(w, &mut out);
    }
    walk_joins(&q.from, &mut out);
    for g in &q.group_by {
        walk_expr(g, &mut out);
    }
    if let Some(h) = &q.having {
        walk_expr(h, &mut out);
    }
    for o in &q.order_by {
        walk_expr(&o.expr, &mut out);
    }
    out
}

fn walk_expr(expr: &Expr, out: &mut HashMap<String, HashSet<String>>) {
    match expr {
        Expr::Column { table, column } => {
            out.entry(table.clone()).or_default().insert(column.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_expr(left, out);
            walk_expr(right, out);
        }
        Expr::UnaryOp { expr, .. } => walk_expr(expr, out),
        Expr::FuncCall { args, .. } => args.iter().for_each(|a| walk_expr(a, out)),
        Expr::InSubquery { expr, .. } => walk_expr(expr, out),
        Expr::Literal(_) | Expr::Param { .. } => {}
    }
}

fn walk_joins(tr: &TableRef, out: &mut HashMap<String, HashSet<String>>) {
    if let TableRef::Join {
        left, right, on, ..
    } = tr
    {
        walk_joins(left, out);
        walk_joins(right, out);
        walk_expr(on, out);
    }
}

// ─── Wrapping ────────────────────────────────────────────────────────────

fn wrap_node_scans(from: &mut TableRef, used: &HashMap<String, HashSet<String>>) {
    match from {
        TableRef::Scan { table, alias } if is_node_table(table) => {
            let Some(columns) = used.get(alias.as_str()) else {
                return;
            };
            let table_name = table.clone();
            let alias = alias.clone();

            let argmax = |col: &str| {
                Expr::func(
                    "argMax",
                    vec![Expr::col(&alias, col), Expr::col(&alias, VERSION_COLUMN)],
                )
            };

            let mut select = vec![SelectExpr::new(Expr::col(&alias, "id"), "id")];
            for col in columns {
                if col == "id" {
                    continue;
                }
                select.push(SelectExpr::new(argmax(col), col.clone()));
            }

            *from = TableRef::subquery(
                Query {
                    select,
                    from: TableRef::scan(table_name, &alias),
                    group_by: vec![Expr::col(&alias, "id")],
                    having: Some(Expr::eq(argmax(DELETED_COLUMN), Expr::lit(false))),
                    ..Default::default()
                },
                alias,
            );
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            wrap_node_scans(left, used);
            wrap_node_scans(right, used);
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Cte;
    use ontology::constants::TRAVERSAL_PATH_COLUMN;

    fn starts_with(alias: &str, path: &str) -> Expr {
        Expr::func(
            "startsWith",
            vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string(path)],
        )
    }

    fn find_subquery<'a>(from: &'a TableRef, target: &str) -> Option<&'a Query> {
        match from {
            TableRef::Subquery { query, alias, .. } if alias == target => Some(query),
            TableRef::Join { left, right, .. } => {
                find_subquery(left, target).or_else(|| find_subquery(right, target))
            }
            _ => None,
        }
    }

    fn has_argmax_having(q: &Query) -> bool {
        q.having.as_ref().is_some_and(|h| {
            matches!(h, Expr::BinaryOp { left, .. }
                if matches!(left.as_ref(), Expr::FuncCall { name, .. } if name == "argMax"))
        })
    }

    #[test]
    fn wraps_node_scan() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::eq(Expr::col("mr", "state"), Expr::string("merged"))),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert_eq!(inner.group_by.len(), 1);
        assert!(has_argmax_having(inner));
        // Property filter stays in outer WHERE, untouched.
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn skips_edge_table() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "src")],
            from: TableRef::scan("gl_edge", "e"),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "gl_edge"));
    }

    #[test]
    fn skips_non_graph_tables() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("c", "id"), "id")],
            from: TableRef::scan("some_cte", "c"),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "some_cte"));
    }

    #[test]
    fn wraps_node_in_join_not_edge() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "name"),
                SelectExpr::new(Expr::col("e", "source_id"), "src"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_edge", "e"),
                Expr::eq(Expr::col("p", "id"), Expr::col("e", "target_id")),
            ),
            where_clause: Some(starts_with("p", "1/")),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(find_subquery(&q.from, "p").is_some());
        assert!(find_subquery(&q.from, "e").is_none());
    }

    #[test]
    fn wraps_cte_node_scans() {
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "_nf_mr",
                Query {
                    select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                    from: TableRef::scan("gl_merge_request", "mr"),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("_nf_mr", "b"),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.ctes[0].query.from, "mr").expect("CTE scan wrapped");
        assert!(has_argmax_having(inner));
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "_nf_mr"));
    }

    #[test]
    fn argmax_covers_used_columns() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("mr", "id"), "id"),
                SelectExpr::new(Expr::col("mr", "title"), "title"),
                SelectExpr::new(Expr::col("mr", "state"), "state"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").unwrap();
        let cols: HashSet<_> = inner
            .select
            .iter()
            .filter_map(|s| s.alias.as_deref())
            .collect();
        assert!(cols.contains("id"));
        assert!(cols.contains("title"));
        assert!(cols.contains("state"));
    }

    #[test]
    fn user_table_dedup_by_id() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("u", "username"), "name")],
            from: TableRef::scan("gl_user", "u"),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "u").expect("user should be wrapped");
        assert_eq!(inner.group_by.len(), 1);
        assert!(has_argmax_having(inner));
    }
}

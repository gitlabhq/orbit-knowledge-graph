//! Row deduplication pass for ReplacingMergeTree node tables.
//!
//! ClickHouse's `ReplacingMergeTree` deduplicates during background merges,
//! but between merges queries can see stale duplicates. This pass wraps
//! every node table scan in a subquery that picks the latest row per `id`
//! using `ORDER BY _version DESC LIMIT 1 BY id`, then filters deleted rows.
//!
//! Edge tables are excluded — their full-tuple ORDER BY makes RMT dedup
//! effective, and wrapping them would kill LIMIT pushdown.
//!
//! ```sql
//! -- before
//! FROM gl_merge_request AS mr WHERE mr.state = 'merged'
//!
//! -- after
//! FROM (SELECT * FROM gl_merge_request AS mr
//!       ORDER BY _version DESC LIMIT 1 BY id) AS mr
//! WHERE mr._deleted = false AND mr.state = 'merged'
//! ```
//!
//! No column tracking needed — `SELECT *` passes all columns through.
//! Security injects `startsWith(traversal_path)` into inner queries via
//! its own subquery recursion.
//!
//! Refs: <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/308>

use ontology::constants::{DELETED_COLUMN, EDGE_TABLE, GL_TABLE_PREFIX, VERSION_COLUMN};

use crate::ast::{Expr, Node, OrderExpr, Query, SelectExpr, TableRef};

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
    visit_derived_tables(&mut q.from);
    for arm in &mut q.union_all {
        dedup_query(arm);
    }
    wrap_node_scans(&mut q.from, &mut q.where_clause);
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

fn wrap_node_scans(from: &mut TableRef, where_clause: &mut Option<Expr>) {
    match from {
        TableRef::Scan { table, alias } if is_node_table(table) => {
            let table_name = table.clone();
            let alias = alias.clone();

            // _deleted = false filter goes on the outer query.
            let deleted_filter = Expr::eq(Expr::col(&alias, DELETED_COLUMN), Expr::lit(false));
            *where_clause = Some(match where_clause.take() {
                Some(existing) => Expr::and(deleted_filter, existing),
                None => deleted_filter,
            });

            *from = TableRef::subquery(
                Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(table_name, &alias),
                    order_by: vec![OrderExpr {
                        expr: Expr::col(&alias, VERSION_COLUMN),
                        desc: true,
                    }],
                    limit_by: Some((1, vec![Expr::col(&alias, "id")])),
                    ..Default::default()
                },
                alias,
            );
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            wrap_node_scans(left, where_clause);
            wrap_node_scans(right, where_clause);
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Cte;
    use ontology::constants::TRAVERSAL_PATH_COLUMN;

    fn find_subquery<'a>(from: &'a TableRef, target: &str) -> Option<&'a Query> {
        match from {
            TableRef::Subquery { query, alias, .. } if alias == target => Some(query),
            TableRef::Join { left, right, .. } => {
                find_subquery(left, target).or_else(|| find_subquery(right, target))
            }
            _ => None,
        }
    }

    fn has_limit_by(q: &Query) -> bool {
        q.limit_by
            .as_ref()
            .is_some_and(|(n, cols)| *n == 1 && !cols.is_empty())
    }

    fn outer_where_contains(q: &Query, needle: &str) -> bool {
        q.where_clause
            .as_ref()
            .is_some_and(|w| format!("{w:?}").contains(needle))
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
        assert!(has_limit_by(inner));
        assert!(inner.order_by[0].desc);
        assert!(outer_where_contains(q, "_deleted"));
        assert!(outer_where_contains(q, "state"));
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
            where_clause: Some(Expr::func(
                "startsWith",
                vec![Expr::col("p", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
            )),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(find_subquery(&q.from, "p").is_some());
        assert!(find_subquery(&q.from, "e").is_none());
        assert!(outer_where_contains(q, "_deleted"));
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
        assert!(has_limit_by(inner));
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "_nf_mr"));
    }

    #[test]
    fn user_table_dedup() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("u", "username"), "name")],
            from: TableRef::scan("gl_user", "u"),
            ..Default::default()
        }));
        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "u").expect("user should be wrapped");
        assert!(has_limit_by(inner));
    }
}

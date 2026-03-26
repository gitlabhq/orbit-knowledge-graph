//! Row deduplication pass for ReplacingMergeTree node tables.
//!
//! ClickHouse's `ReplacingMergeTree` engine deduplicates rows during
//! background merges, but between merges queries can return stale or
//! duplicate rows. This pass wraps every **node** table scan in a
//! subquery that uses `GROUP BY id` + `argMax(_deleted, _version)` to
//! deduplicate at query time.
//!
//! Edge tables (`gl_edge`) are intentionally excluded: their ORDER BY key
//! is the full column tuple, making RMT dedup highly effective. Wrapping
//! edges would add 6-column GROUP BY overhead on the hottest table,
//! kill LIMIT pushdown, and block streaming joins.
//!
//! ## Pattern
//!
//! ```sql
//! -- before
//! SELECT mr.id, mr.state FROM gl_merge_request AS mr
//!   WHERE startsWith(mr.traversal_path, '1/') AND mr.state = 'merged'
//!
//! -- after
//! SELECT mr.id, mr.state
//! FROM (
//!   SELECT id, argMax(state, _version) AS state
//!   FROM gl_merge_request AS mr
//!   WHERE startsWith(mr.traversal_path, '1/')
//!   GROUP BY id
//!   HAVING argMax(_deleted, _version) = false
//! ) AS mr
//! WHERE mr.state = 'merged'
//! ```
//!
//! Rules (per <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/308>):
//! - Primary key filters go into the inner subquery (granule pruning).
//! - Property filters stay in the outer query (argMax must see all versions).
//! - `FINAL` is avoided; `GROUP BY` + `argMax` is lighter.
//!
//! Runs before the security pass so that security's subquery recursion
//! injects `startsWith(traversal_path, ...)` directly into inner queries.

use std::collections::{HashMap, HashSet};

use ontology::constants::{
    DELETED_COLUMN, EDGE_TABLE, GL_TABLE_PREFIX, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

use crate::ast::{Expr, Node, Op, Query, SelectExpr, TableRef};
use crate::constants::SKIP_SECURITY_FILTER_TABLES;

/// Apply row deduplication to all graph table scans in the AST.
pub fn deduplicate(node: &mut Node) {
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                deduplicate_query(&mut cte.query);
            }
            deduplicate_query(q);
        }
    }
}

fn deduplicate_query(q: &mut Query) {
    // Bottom-up: process derived tables inside the FROM tree first so that
    // wrapping at the current level doesn't recurse into already-wrapped scans.
    recurse_into_derived_tables(&mut q.from);
    for arm in &mut q.union_all {
        deduplicate_query(arm);
    }

    // Collect columns referenced at this query level and wrap remaining scans.
    let used = collect_used_columns(q);
    wrap_scans(&mut q.from, &mut q.where_clause, &used);
}

/// Walk the FROM tree and recurse into subqueries / UNION arms.
fn recurse_into_derived_tables(from: &mut TableRef) {
    match from {
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            recurse_into_derived_tables(left);
            recurse_into_derived_tables(right);
        }
        TableRef::Union { queries, .. } => {
            for q in queries {
                deduplicate_query(q);
            }
        }
        TableRef::Subquery { query, .. } => {
            deduplicate_query(query);
        }
    }
}

// ─── Table metadata ──────────────────────────────────────────────────────

/// Node tables start with `gl_` but are not the edge table.
fn is_node_table(table: &str) -> bool {
    table.starts_with(GL_TABLE_PREFIX) && table != EDGE_TABLE
}

/// Columns whose filters are safe to push into the inner subquery.
/// Only ORDER BY / PRIMARY KEY columns — property filters must stay outer.
fn pushdown_columns(table: &str) -> &'static [&'static str] {
    if SKIP_SECURITY_FILTER_TABLES.contains(&table) {
        &["id"] // gl_user: ORDER BY (id)
    } else {
        &[TRAVERSAL_PATH_COLUMN, "id"]
    }
}

// ─── Column collection ───────────────────────────────────────────────────

/// Map of `alias → {column names}` referenced at this query level.
fn collect_used_columns(q: &Query) -> HashMap<String, HashSet<String>> {
    let mut out: HashMap<String, HashSet<String>> = HashMap::new();
    for sel in &q.select {
        walk_columns(&sel.expr, &mut out);
    }
    if let Some(w) = &q.where_clause {
        walk_columns(w, &mut out);
    }
    walk_join_conditions(&q.from, &mut out);
    for g in &q.group_by {
        walk_columns(g, &mut out);
    }
    if let Some(h) = &q.having {
        walk_columns(h, &mut out);
    }
    for o in &q.order_by {
        walk_columns(&o.expr, &mut out);
    }
    out
}

fn walk_columns(expr: &Expr, out: &mut HashMap<String, HashSet<String>>) {
    match expr {
        Expr::Column { table, column } => {
            out.entry(table.clone()).or_default().insert(column.clone());
        }
        Expr::BinaryOp { left, right, .. } => {
            walk_columns(left, out);
            walk_columns(right, out);
        }
        Expr::UnaryOp { expr, .. } => walk_columns(expr, out),
        Expr::FuncCall { args, .. } => args.iter().for_each(|a| walk_columns(a, out)),
        Expr::InSubquery { expr, .. } => walk_columns(expr, out),
        Expr::Literal(_) | Expr::Param { .. } => {}
    }
}

fn walk_join_conditions(tr: &TableRef, out: &mut HashMap<String, HashSet<String>>) {
    if let TableRef::Join {
        left, right, on, ..
    } = tr
    {
        walk_join_conditions(left, out);
        walk_join_conditions(right, out);
        walk_columns(on, out);
    }
}

// ─── Core wrapping logic ─────────────────────────────────────────────────

fn wrap_scans(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    used: &HashMap<String, HashSet<String>>,
) {
    match from {
        TableRef::Scan { table, alias } if is_node_table(table) => {
            let Some(columns) = used.get(alias.as_str()) else {
                return;
            };
            let table_name = table.clone();
            let alias = alias.clone();
            let pk: HashSet<&str> = pushdown_columns(&table_name).iter().copied().collect();

            let (pushable, remaining) = extract_pushable(where_clause.take(), &alias, &pk);
            *where_clause = rebuild_and(remaining);

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

            let inner = Query {
                select,
                from: TableRef::scan(table_name, &alias),
                where_clause: rebuild_and(pushable),
                group_by: vec![Expr::col(&alias, "id")],
                having: Some(Expr::eq(argmax(DELETED_COLUMN), Expr::lit(false))),
                ..Default::default()
            };
            *from = TableRef::subquery(inner, alias);
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            wrap_scans(left, where_clause, used);
            wrap_scans(right, where_clause, used);
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

// ─── WHERE clause helpers ────────────────────────────────────────────────

/// Split WHERE conjuncts into pushable (reference only `alias` + PK columns)
/// and remaining (everything else).
fn extract_pushable(
    expr: Option<Expr>,
    alias: &str,
    pk_cols: &HashSet<&str>,
) -> (Vec<Expr>, Vec<Expr>) {
    let Some(e) = expr else {
        return (vec![], vec![]);
    };
    let mut pushable = Vec::new();
    let mut remaining = Vec::new();
    for conjunct in flatten_and(e) {
        if is_pushable(&conjunct, alias, pk_cols) {
            pushable.push(conjunct);
        } else {
            remaining.push(conjunct);
        }
    }
    (pushable, remaining)
}

/// True when every column reference in `expr` targets `alias` with a PK column.
fn is_pushable(expr: &Expr, alias: &str, pk_cols: &HashSet<&str>) -> bool {
    match expr {
        Expr::Column { table, column } => table == alias && pk_cols.contains(column.as_str()),
        Expr::BinaryOp { left, right, .. } => {
            is_pushable(left, alias, pk_cols) && is_pushable(right, alias, pk_cols)
        }
        Expr::UnaryOp { expr, .. } => is_pushable(expr, alias, pk_cols),
        Expr::FuncCall { args, .. } => args.iter().all(|a| is_pushable(a, alias, pk_cols)),
        Expr::InSubquery { expr, .. } => is_pushable(expr, alias, pk_cols),
        Expr::Literal(_) | Expr::Param { .. } => true,
    }
}

fn flatten_and(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => {
            let mut v = flatten_and(*left);
            v.extend(flatten_and(*right));
            v
        }
        other => vec![other],
    }
}

fn rebuild_and(mut v: Vec<Expr>) -> Option<Expr> {
    if v.is_empty() {
        return None;
    }
    let first = v.remove(0);
    Some(v.into_iter().fold(first, Expr::and))
}

// ─── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::OrderExpr;

    fn starts_with(alias: &str, path: &str) -> Expr {
        Expr::func(
            "startsWith",
            vec![Expr::col(alias, TRAVERSAL_PATH_COLUMN), Expr::string(path)],
        )
    }

    fn eq_filter(alias: &str, col: &str, val: &str) -> Expr {
        Expr::eq(Expr::col(alias, col), Expr::string(val))
    }

    fn has_argmax_having(q: &Query) -> bool {
        q.having.as_ref().is_some_and(|h| match h {
            Expr::BinaryOp { left, .. } => matches!(
                left.as_ref(),
                Expr::FuncCall { name, .. } if name == "argMax"
            ),
            _ => false,
        })
    }

    fn find_subquery<'a>(from: &'a TableRef, alias: &str) -> Option<&'a Query> {
        match from {
            TableRef::Subquery {
                query, alias: a, ..
            } if a == alias => Some(query),
            TableRef::Join { left, right, .. } => {
                find_subquery(left, alias).or_else(|| find_subquery(right, alias))
            }
            _ => None,
        }
    }

    // ── Node table dedup ─────────────────────────────────────────────

    #[test]
    fn wraps_node_scan_in_argmax_subquery() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("mr", "id"), "mr_id"),
                SelectExpr::new(Expr::col("mr", "state"), "mr_state"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                starts_with("mr", "1/"),
                eq_filter("mr", "state", "merged"),
            )),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        // Outer FROM is now a subquery.
        let inner = find_subquery(&q.from, "mr").expect("mr should be wrapped");

        // Inner query groups by id and has argMax HAVING.
        assert_eq!(inner.group_by.len(), 1);
        assert!(has_argmax_having(inner));

        // startsWith was pushed into inner WHERE (PK column).
        assert!(
            inner.where_clause.is_some(),
            "inner WHERE should contain pushed-down traversal_path filter"
        );

        // state = 'merged' stays in outer WHERE (non-PK column).
        let outer_where = q.where_clause.as_ref().expect("outer WHERE should exist");
        let outer_str = format!("{outer_where:?}");
        assert!(outer_str.contains("state"), "state filter should be outer");
    }

    #[test]
    fn pushes_id_filter_into_inner() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "p_id")],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(Expr::and(
                starts_with("p", "1/"),
                Expr::eq(Expr::col("p", "id"), Expr::int(42)),
            )),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "p").expect("p should be wrapped");

        // Both traversal_path and id filters are PK → pushed down.
        let inner_where = format!("{:?}", inner.where_clause);
        assert!(
            inner_where.contains("traversal_path"),
            "traversal_path should be inner"
        );
        assert!(inner_where.contains("42"), "id filter should be inner");

        // No outer WHERE remains (both conjuncts were pushable).
        assert!(q.where_clause.is_none(), "outer WHERE should be empty");
    }

    // ── Edge table skipped ──────────────────────────────────────────

    #[test]
    fn skips_edge_table() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("e", "source_id"), "src"),
                SelectExpr::new(Expr::col("e", "target_id"), "dst"),
            ],
            from: TableRef::scan("gl_edge", "e"),
            where_clause: Some(eq_filter("e", "relationship_kind", "AUTHORED")),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(
            matches!(&q.from, TableRef::Scan { table, .. } if table == "gl_edge"),
            "edge table should not be wrapped"
        );
    }

    // ── gl_user: no traversal_path ───────────────────────────────────

    #[test]
    fn user_table_dedup_by_id_only() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("u", "id"), "u_id"),
                SelectExpr::new(Expr::col("u", "username"), "u_name"),
            ],
            from: TableRef::scan("gl_user", "u"),
            where_clause: Some(Expr::eq(Expr::col("u", "id"), Expr::int(1))),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "u").expect("u should be wrapped");

        assert_eq!(inner.group_by.len(), 1); // GROUP BY id
        assert!(has_argmax_having(inner));

        // id filter pushed down (PK column).
        assert!(inner.where_clause.is_some());
        assert!(q.where_clause.is_none());
    }

    // ── Join: wraps node side only ──────────────────────────────────

    #[test]
    fn wraps_node_side_of_join_not_edge() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "name"), "p_name"),
                SelectExpr::new(Expr::col("e", "source_id"), "src"),
            ],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_edge", "e"),
                Expr::eq(Expr::col("p", "id"), Expr::col("e", "target_id")),
            ),
            where_clause: Some(Expr::and(starts_with("p", "1/"), starts_with("e", "1/"))),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(
            find_subquery(&q.from, "p").is_some(),
            "node should be wrapped"
        );
        assert!(
            find_subquery(&q.from, "e").is_none(),
            "edge should NOT be wrapped"
        );
    }

    // ── CTE dedup ────────────────────────────────────────────────────

    #[test]
    fn deduplicates_cte_scans() {
        use crate::ast::Cte;

        let cte_query = Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(starts_with("mr", "1/")),
            ..Default::default()
        };
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new("_nf_mr", cte_query)],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("_nf_mr", "b"),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        // CTE's inner query should be wrapped.
        let cte = &q.ctes[0];
        let inner = find_subquery(&cte.query.from, "mr").expect("CTE scan should be wrapped");
        assert!(has_argmax_having(inner));

        // Main query FROM references a CTE name (not gl_*), not wrapped.
        assert!(
            matches!(&q.from, TableRef::Scan { table, .. } if table == "_nf_mr"),
            "CTE reference should not be wrapped"
        );
    }

    // ── Non-graph tables skipped ─────────────────────────────────────

    #[test]
    fn skips_non_graph_tables() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("c", "id"), "id")],
            from: TableRef::scan("some_cte", "c"),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        assert!(
            matches!(&q.from, TableRef::Scan { table, .. } if table == "some_cte"),
            "non-graph scan should not be wrapped"
        );
    }

    // ── Cross-alias conjuncts stay outer ─────────────────────────────

    #[test]
    fn cross_alias_conjuncts_stay_outer() {
        let cross = Expr::eq(Expr::col("p", "id"), Expr::col("e", "target_id"));
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::join(
                crate::ast::JoinType::Inner,
                TableRef::scan("gl_project", "p"),
                TableRef::scan("gl_edge", "e"),
                Expr::lit(true),
            ),
            where_clause: Some(Expr::and(
                Expr::and(starts_with("p", "1/"), starts_with("e", "1/")),
                cross.clone(),
            )),
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        // The cross-alias conjunct should remain in the outer WHERE.
        let outer = format!("{:?}", q.where_clause);
        assert!(
            outer.contains("target_id"),
            "cross-alias filter should stay outer"
        );
    }

    // ── argMax columns match used columns ────────────────────────────

    #[test]
    fn argmax_covers_all_used_columns() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("mr", "id"), "mr_id"),
                SelectExpr::new(Expr::col("mr", "title"), "mr_title"),
                SelectExpr::new(Expr::col("mr", "state"), "mr_state"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                starts_with("mr", "1/"),
                eq_filter("mr", "state", "merged"),
            )),
            order_by: vec![OrderExpr {
                expr: Expr::col("mr", "id"),
                desc: false,
            }],
            ..Default::default()
        }));

        deduplicate(&mut node);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").unwrap();

        // Inner SELECT should have: id (GROUP BY) + argMax(title, _version) + argMax(state, _version)
        // (traversal_path is not directly referenced in outer query, but was in WHERE before pushdown)
        assert!(
            inner.select.len() >= 3,
            "should have id + at least 2 argMax columns"
        );

        let aliases: Vec<_> = inner
            .select
            .iter()
            .filter_map(|s| s.alias.as_deref())
            .collect();
        assert!(aliases.contains(&"id"));
        assert!(aliases.contains(&"title"));
        assert!(aliases.contains(&"state"));
    }
}

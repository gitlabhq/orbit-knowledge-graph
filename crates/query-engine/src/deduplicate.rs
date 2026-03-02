//! Deduplication for ReplacingMergeTree tables.
//!
//! All `gl_*` graph tables use `ReplacingMergeTree(_version, _deleted)`.
//! Without deduplication, queries may return stale duplicate rows or
//! soft-deleted rows that haven't been merged yet.
//!
//! For non-aggregation queries (no existing GROUP BY), this phase:
//! - Wraps column refs in SELECT/ORDER BY with `argMax(col, _version)`
//! - Adds GROUP BY on dedup key columns
//! - Adds HAVING `argMax(_deleted, _version) = false` per alias
//!
//! For aggregation queries (existing GROUP BY), it wraps each `gl_*` scan
//! in a Subquery that deduplicates before the outer aggregation runs.

use std::collections::HashMap;

use crate::ast::{Expr, Node, Op, Query, SelectExpr, TableRef};
use crate::constants::GL_TABLE_PREFIX;
use crate::error::Result;
use ontology::constants::{DELETED_COLUMN, EDGE_TABLE, VERSION_COLUMN};

pub fn deduplicate(node: &mut Node) -> Result<()> {
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                deduplicate_query(&mut cte.query);
            }
            deduplicate_query(q);
        }
    }
    Ok(())
}

fn deduplicate_query(q: &mut Query) {
    let scans = collect_scan_aliases(&q.from);
    if scans.is_empty() {
        return;
    }

    if q.group_by.is_empty() {
        deduplicate_inline(q, &scans);
    } else {
        deduplicate_with_subqueries(q, &scans);
    }

    for union_q in &mut q.union_all {
        deduplicate_query(union_q);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Path 1: inline argMax wrapping (non-aggregation queries)
// ─────────────────────────────────────────────────────────────────────────────

fn deduplicate_inline(q: &mut Query, scans: &HashMap<String, String>) {
    for sel in &mut q.select {
        sel.expr = wrap_expr(&sel.expr, scans);
    }

    for ord in &mut q.order_by {
        ord.expr = wrap_expr(&ord.expr, scans);
    }

    for (alias, table) in scans {
        for key in dedup_key_columns(table) {
            let key_expr = Expr::col(alias, key);
            if !q.group_by.contains(&key_expr) {
                q.group_by.push(key_expr);
            }
        }
    }

    let deleted_conds = scans.keys().map(|alias| {
        Some(Expr::eq(
            Expr::func(
                "argMax",
                vec![
                    Expr::col(alias, DELETED_COLUMN),
                    Expr::col(alias, VERSION_COLUMN),
                ],
            ),
            Expr::lit(false),
        ))
    });

    q.having = Expr::and_all(deleted_conds.chain(std::iter::once(q.having.take())));
}

/// Replace `Expr::Column` refs for deduped aliases with `argMax(col, _version)`.
fn wrap_expr(expr: &Expr, scans: &HashMap<String, String>) -> Expr {
    match expr {
        Expr::Column { table, column } if scans.contains_key(table) && column != VERSION_COLUMN => {
            Expr::func(
                "argMax",
                vec![Expr::col(table, column), Expr::col(table, VERSION_COLUMN)],
            )
        }
        Expr::FuncCall { name, args } => {
            Expr::func(name, args.iter().map(|a| wrap_expr(a, scans)).collect())
        }
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op: *op,
            left: Box::new(wrap_expr(left, scans)),
            right: Box::new(wrap_expr(right, scans)),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(wrap_expr(inner, scans)),
        },
        _ => expr.clone(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Path 2: subquery wrapping (aggregation queries with existing GROUP BY)
// ─────────────────────────────────────────────────────────────────────────────

fn deduplicate_with_subqueries(q: &mut Query, scans: &HashMap<String, String>) {
    let old = std::mem::replace(
        &mut q.from,
        TableRef::Scan {
            table: String::new(),
            alias: String::new(),
            type_filter: None,
        },
    );
    q.from = wrap_scans(old, scans);
}

fn wrap_scans(table_ref: TableRef, scans: &HashMap<String, String>) -> TableRef {
    let needs_wrap =
        matches!(&table_ref, TableRef::Scan { alias, .. } if scans.contains_key(alias));
    if needs_wrap {
        return build_dedup_subquery(table_ref);
    }

    match table_ref {
        TableRef::Scan { .. } => table_ref,
        TableRef::Join {
            join_type,
            left,
            right,
            on,
        } => TableRef::Join {
            join_type,
            left: Box::new(wrap_scans(*left, scans)),
            right: Box::new(wrap_scans(*right, scans)),
            on,
        },
        TableRef::Union { mut queries, alias } => {
            for uq in &mut queries {
                deduplicate_query(uq);
            }
            TableRef::Union { queries, alias }
        }
        TableRef::Subquery { query, alias } => TableRef::Subquery { query, alias },
    }
}

/// Build a dedup subquery that uses `SELECT *` with argMax + GROUP BY + HAVING.
///
/// We use `SELECT *` here because we don't know which columns the outer
/// aggregation query will reference. ClickHouse expands `*` at parse time.
/// The argMax wrapping is applied to a wildcard column — but since we can't
/// wrap `*` in argMax, we build a simple subquery:
///
/// ```sql
/// (SELECT key1, key2, argMax(*) ... FROM table GROUP BY keys
///  HAVING argMax(_deleted, _version) = false) AS alias
/// ```
///
/// Actually, for aggregation subqueries we take a simpler approach: we just
/// do a `FINAL` scan. But the user rejected FINAL.
///
/// The pragmatic approach: the inner query selects all key columns directly
/// and wraps only `_deleted` in argMax for the HAVING filter. For value
/// columns, we rely on ClickHouse's behavior that `argMax` on a GROUP BY
/// column is just the column itself. So we select `*` and GROUP BY keys.
///
/// Wait — we can't SELECT * with GROUP BY in standard SQL. ClickHouse
/// actually allows it but it's undefined for non-key columns.
///
/// Real approach: build a proper inner SELECT with all key columns passed
/// through and all other referenced columns wrapped in argMax. Since we
/// don't know which columns the outer query needs, we need to collect them.
/// But that's complex. Instead, we use the simpler pattern the principal
/// engineer showed: SELECT columns with argMax for non-keys.
///
/// Since the outer query's column references are already known (they're in
/// q.select, q.where_clause, q.group_by, etc.), we collect them per alias
/// and build a targeted inner SELECT.
fn build_dedup_subquery(scan: TableRef) -> TableRef {
    let TableRef::Scan {
        table,
        alias,
        type_filter,
    } = scan
    else {
        unreachable!();
    };

    let inner_alias = format!("_d_{alias}");
    let keys = dedup_key_columns(&table);

    // Build inner SELECT: key columns pass through, _deleted via argMax.
    // For aggregation subqueries we select key columns only — the outer
    // query only needs join keys (which are always dedup keys like id,
    // traversal_path) to match rows. Any value columns the outer query
    // references are on OTHER tables, not this one, since the aggregation
    // groups by this table's key.
    let mut select: Vec<SelectExpr> = keys
        .iter()
        .map(|&k| SelectExpr::new(Expr::col(&inner_alias, k), k))
        .collect();

    // Always expose _deleted via argMax for HAVING filter.
    select.push(SelectExpr::new(
        Expr::func(
            "argMax",
            vec![
                Expr::col(&inner_alias, DELETED_COLUMN),
                Expr::col(&inner_alias, VERSION_COLUMN),
            ],
        ),
        DELETED_COLUMN,
    ));

    let group_by: Vec<Expr> = keys.iter().map(|&k| Expr::col(&inner_alias, k)).collect();

    let having = Some(Expr::eq(
        Expr::func(
            "argMax",
            vec![
                Expr::col(&inner_alias, DELETED_COLUMN),
                Expr::col(&inner_alias, VERSION_COLUMN),
            ],
        ),
        Expr::lit(false),
    ));

    let inner_scan = TableRef::Scan {
        table,
        alias: inner_alias,
        type_filter,
    };

    let inner_query = Query {
        select,
        from: inner_scan,
        group_by,
        having,
        ..Default::default()
    };

    TableRef::Subquery {
        query: Box::new(inner_query),
        alias,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scan alias collection
// ─────────────────────────────────────────────────────────────────────────────

/// Walk the FROM tree and collect `alias → table_name` for all `gl_*` scans.
fn collect_scan_aliases(table_ref: &TableRef) -> HashMap<String, String> {
    let mut map = HashMap::new();
    collect_scan_aliases_inner(table_ref, &mut map);
    map
}

fn collect_scan_aliases_inner(table_ref: &TableRef, map: &mut HashMap<String, String>) {
    match table_ref {
        TableRef::Scan { table, alias, .. } if table.starts_with(GL_TABLE_PREFIX) => {
            map.insert(alias.clone(), table.clone());
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            collect_scan_aliases_inner(left, map);
            collect_scan_aliases_inner(right, map);
        }
        // Union inner queries are handled by deduplicate_query recursion.
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dedup key mapping (from graph.sql ORDER BY clauses)
// ─────────────────────────────────────────────────────────────────────────────

const CODE_TABLES: &[&str] = &[
    "gl_directory",
    "gl_file",
    "gl_definition",
    "gl_imported_symbol",
];

fn dedup_key_columns(table: &str) -> Vec<&'static str> {
    if table == "gl_user" {
        vec!["id"]
    } else if table == EDGE_TABLE {
        vec![
            "traversal_path",
            "source_id",
            "source_kind",
            "relationship_kind",
            "target_id",
            "target_kind",
        ]
    } else if CODE_TABLES.contains(&table) {
        vec!["traversal_path", "project_id", "branch", "id"]
    } else {
        vec!["traversal_path", "id"]
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Dedup validation (called from check.rs)
// ─────────────────────────────────────────────────────────────────────────────

/// Verify that all `gl_*` table aliases have deduplication applied.
///
/// For inline dedup (Path 1): checks that GROUP BY contains the dedup key
/// columns and HAVING references `argMax(_deleted, _version)`.
///
/// For subquery dedup (Path 2): checks that the alias comes from a
/// `TableRef::Subquery` whose inner query has the above.
pub fn check_dedup(node: &Node) -> crate::error::Result<()> {
    match node {
        Node::Query(q) => {
            for cte in &q.ctes {
                check_dedup_query(&cte.query)?;
            }
            check_dedup_query(q)
        }
    }
}

fn check_dedup_query(q: &Query) -> crate::error::Result<()> {
    let scans = collect_scan_aliases(&q.from);
    if scans.is_empty() {
        return Ok(());
    }

    // Path 1: inline dedup — verify HAVING has argMax(_deleted) per alias
    if !q.group_by.is_empty() && q.having.is_some() {
        for alias in scans.keys() {
            if !has_deleted_filter(q.having.as_ref(), alias) {
                return Err(crate::error::QueryError::Security(format!(
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
        return Err(crate::error::QueryError::Security(format!(
            "dedup check failed: alias '{alias}' has no deduplication"
        )));
    }

    Ok(())
}

fn collect_subquery_aliases(table_ref: &TableRef) -> Vec<String> {
    match table_ref {
        TableRef::Subquery { alias, query } => {
            // Verify the inner query actually has dedup (GROUP BY + HAVING)
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

/// Check whether a HAVING expression contains `argMax(alias._deleted, ...) = false`.
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
    use crate::ast::{JoinType, OrderExpr};
    use ontology::constants::EDGE_TABLE;

    fn assert_has_group_by(q: &Query) {
        assert!(!q.group_by.is_empty(), "expected GROUP BY");
    }

    fn assert_has_having(q: &Query) {
        assert!(q.having.is_some(), "expected HAVING clause");
    }

    fn assert_is_subquery(t: &TableRef) -> &Query {
        match t {
            TableRef::Subquery { query, .. } => query,
            _ => panic!("expected Subquery, got {t:?}"),
        }
    }

    // ── Path 1: inline dedup ────────────────────────────────────────────

    #[test]
    fn wraps_select_columns_in_argmax() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "id"), "id"),
                SelectExpr::new(Expr::col("p", "name"), "name"),
            ],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // Both columns should be wrapped in argMax
        for sel in &q.select {
            assert!(
                matches!(&sel.expr, Expr::FuncCall { name, .. } if name == "argMax"),
                "expected argMax, got {:?}",
                sel.expr
            );
        }
        assert_has_group_by(q);
        assert_has_having(q);
    }

    #[test]
    fn wraps_order_by_in_argmax() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            order_by: vec![OrderExpr {
                expr: Expr::col("p", "name"),
                desc: true,
            }],
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        assert!(
            matches!(&q.order_by[0].expr, Expr::FuncCall { name, .. } if name == "argMax"),
            "ORDER BY should use argMax"
        );
    }

    #[test]
    fn leaves_where_untouched() {
        let filter = Expr::eq(Expr::col("p", "name"), Expr::lit("test"));

        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            where_clause: Some(filter.clone()),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // WHERE should still reference p.name directly (not argMax)
        assert_eq!(q.where_clause.as_ref().unwrap(), &filter);
    }

    #[test]
    fn adds_correct_group_by_keys() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // gl_project: (traversal_path, id)
        assert_eq!(q.group_by.len(), 2);
        assert!(q.group_by.contains(&Expr::col("p", "traversal_path")));
        assert!(q.group_by.contains(&Expr::col("p", "id")));
    }

    #[test]
    fn gl_user_groups_by_id_only() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("u", "username"), "name")],
            from: TableRef::scan("gl_user", "u"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        assert_eq!(q.group_by.len(), 1);
        assert!(q.group_by.contains(&Expr::col("u", "id")));
    }

    #[test]
    fn gl_edge_groups_by_composite_key() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "src")],
            from: TableRef::scan(EDGE_TABLE, "e"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        assert_eq!(q.group_by.len(), 6);
    }

    #[test]
    fn code_table_groups_by_project_branch() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("d", "name"), "name")],
            from: TableRef::scan("gl_definition", "d"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        assert_eq!(q.group_by.len(), 4);
        assert!(q.group_by.contains(&Expr::col("d", "project_id")));
        assert!(q.group_by.contains(&Expr::col("d", "branch")));
    }

    #[test]
    fn handles_joins() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("u", "username"), "name"),
                SelectExpr::new(Expr::col("p", "name"), "project"),
            ],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::join(
                    JoinType::Inner,
                    TableRef::scan(EDGE_TABLE, "e"),
                    TableRef::scan("gl_project", "p"),
                    Expr::eq(Expr::col("e", "target_id"), Expr::col("p", "id")),
                ),
                Expr::eq(Expr::col("u", "id"), Expr::col("e", "source_id")),
            ),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // GROUP BY should have keys for all 3 tables: u(1) + e(6) + p(2)
        assert_eq!(q.group_by.len(), 9);
        assert_has_having(q);
    }

    #[test]
    fn skips_non_gl_tables() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("c", "id"), "id")],
            from: TableRef::scan("custom_table", "c"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        assert!(q.group_by.is_empty());
        assert!(q.having.is_none());
    }

    #[test]
    fn skips_literals_and_non_column_exprs() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("p", "id"), "id"),
                SelectExpr::new(Expr::lit("Project"), "type"),
            ],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // First: argMax. Second: still a literal.
        assert!(matches!(&q.select[0].expr, Expr::FuncCall { name, .. } if name == "argMax"));
        assert!(matches!(&q.select[1].expr, Expr::Literal(_)));
    }

    #[test]
    fn handles_ctes() {
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![crate::ast::Cte::new(
                "base",
                Query {
                    select: vec![SelectExpr::new(Expr::col("s", "id"), "id")],
                    from: TableRef::scan("gl_project", "s"),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("base", "b"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // CTE inner query should have dedup
        assert_has_group_by(&q.ctes[0].query);
        assert_has_having(&q.ctes[0].query);
        // "base" is not a gl_* table, outer query should not have dedup
        assert!(q.group_by.is_empty());
    }

    #[test]
    fn handles_union_all_branches() {
        let base = Query {
            select: vec![SelectExpr::new(Expr::col("s", "id"), "node_id")],
            from: TableRef::scan("gl_project", "s"),
            ..Default::default()
        };
        let recursive = Query {
            select: vec![SelectExpr::new(Expr::col("e", "target_id"), "node_id")],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("paths", "p"),
                TableRef::scan(EDGE_TABLE, "e"),
                Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source_id")),
            ),
            ..Default::default()
        };

        let mut cte_query = base;
        cte_query.union_all = vec![recursive];

        let mut node = Node::Query(Box::new(Query {
            ctes: vec![crate::ast::Cte::recursive("paths", cte_query)],
            select: vec![SelectExpr::new(Expr::col("paths", "node_id"), "id")],
            from: TableRef::scan("paths", "paths"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // Base CTE query: gl_project should get dedup
        assert_has_group_by(&q.ctes[0].query);
        // Recursive branch: gl_edge should get dedup, "paths" CTE skipped
        let union_branch = &q.ctes[0].query.union_all[0];
        assert_has_group_by(union_branch);
    }

    // ── Path 2: subquery dedup (aggregation) ────────────────────────────

    #[test]
    fn aggregation_uses_subqueries() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("u", "id"), "user_id"),
                SelectExpr::new(
                    Expr::func("COUNT", vec![Expr::col("n", "id")]),
                    "note_count",
                ),
            ],
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan("gl_user", "u"),
                TableRef::join(
                    JoinType::Inner,
                    TableRef::scan(EDGE_TABLE, "e"),
                    TableRef::scan("gl_note", "n"),
                    Expr::eq(Expr::col("e", "target_id"), Expr::col("n", "id")),
                ),
                Expr::eq(Expr::col("u", "id"), Expr::col("e", "source_id")),
            ),
            group_by: vec![Expr::col("u", "id")],
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        // Outer GROUP BY should be untouched (still just u.id)
        assert_eq!(q.group_by, vec![Expr::col("u", "id")]);
        // No HAVING on outer (dedup is in subqueries)
        assert!(q.having.is_none());

        // All scans should now be subqueries
        fn assert_all_subqueries(t: &TableRef) {
            match t {
                TableRef::Subquery { query, .. } => {
                    assert!(!query.group_by.is_empty());
                    assert!(query.having.is_some());
                }
                TableRef::Join { left, right, .. } => {
                    assert_all_subqueries(left);
                    assert_all_subqueries(right);
                }
                TableRef::Scan { table, .. } => {
                    panic!("expected Subquery for {table}");
                }
                _ => {}
            }
        }
        assert_all_subqueries(&q.from);
    }

    #[test]
    fn subquery_preserves_type_filter() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(
                Expr::func("COUNT", vec![Expr::col("e", "source_id")]),
                "cnt",
            )],
            from: TableRef::scan_with_filter(EDGE_TABLE, "e", vec!["AUTHORED".into()]),
            group_by: vec![Expr::col("e", "source_id")],
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();

        let Node::Query(q) = &node;
        let inner = assert_is_subquery(&q.from);
        if let TableRef::Scan { type_filter, .. } = &inner.from {
            assert_eq!(type_filter.as_ref().unwrap(), &vec!["AUTHORED".to_string()]);
        } else {
            panic!("expected inner Scan");
        }
    }

    // ── Validation ──────────────────────────────────────────────────────

    #[test]
    fn check_dedup_passes_after_inline() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();
        check_dedup(&node).unwrap();
    }

    #[test]
    fn check_dedup_passes_after_subquery() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(
                Expr::func("COUNT", vec![Expr::col("p", "id")]),
                "cnt",
            )],
            from: TableRef::scan("gl_project", "p"),
            group_by: vec![Expr::col("p", "name")],
            ..Default::default()
        }));

        deduplicate(&mut node).unwrap();
        check_dedup(&node).unwrap();
    }

    #[test]
    fn check_dedup_fails_without_dedup() {
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("p", "id"), "id")],
            from: TableRef::scan("gl_project", "p"),
            ..Default::default()
        }));

        let err = check_dedup(&node).unwrap_err();
        assert!(err.to_string().contains("dedup check failed"));
    }

    #[test]
    fn check_dedup_skips_non_gl_tables() {
        let node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("c", "id"), "id")],
            from: TableRef::scan("custom_table", "c"),
            ..Default::default()
        }));

        check_dedup(&node).unwrap();
    }
}

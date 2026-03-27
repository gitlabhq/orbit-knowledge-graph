//! Row deduplication pass for ReplacingMergeTree node tables.
//!
//! ClickHouse's `ReplacingMergeTree` deduplicates during background merges,
//! but between merges queries can see stale duplicates. This pass ensures
//! query-time correctness by applying one of two strategies per node table:
//!
//! 1. **Subquery with LIMIT 1 BY** (default): wraps the scan in a subquery
//!    that picks the latest row per `id`, then filters `_deleted = false`.
//!    Pushes sargable filters into the inner subquery for index utilization.
//!
//! 2. **argMax aggregation** (search queries): rewrites SELECT columns with
//!    `argMax(col, _version)` and adds GROUP BY id + HAVING. Preserves
//!    ClickHouse's LIMIT pushdown which the subquery approach breaks.
//!
//! Edge tables are always excluded -- their full-tuple ORDER BY makes RMT
//! dedup effective, and wrapping them would kill LIMIT pushdown.
//!
//! Hydration queries skip dedup entirely (separate pipeline without this
//! pass) since they read by pre-authorized IDs and stale property values
//! are acceptable.
//!
//! Refs: <https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/308>

use crate::ast::{Expr, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
use crate::input::QueryType;
use ontology::constants::{DELETED_COLUMN, EDGE_TABLE, GL_TABLE_PREFIX, VERSION_COLUMN};

fn is_node_table(table: &str) -> bool {
    table.starts_with(GL_TABLE_PREFIX) && table != EDGE_TABLE
}

/// Apply row deduplication to all node table scans in the AST.
pub fn deduplicate(node: &mut Node, query_type: QueryType) {
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                dedup_query(&mut cte.query, query_type);
            }
            dedup_query(q, query_type);
        }
    }
}

fn dedup_query(q: &mut Query, query_type: QueryType) {
    visit_derived_tables(&mut q.from, query_type);
    for arm in &mut q.union_all {
        dedup_query(arm, query_type);
    }
    wrap_node_scans(
        &mut q.from,
        &mut q.where_clause,
        &mut q.select,
        &mut q.group_by,
        &mut q.having,
        &mut q.order_by,
        query_type,
    );
}

fn visit_derived_tables(from: &mut TableRef, query_type: QueryType) {
    match from {
        TableRef::Join { left, right, .. } => {
            visit_derived_tables(left, query_type);
            visit_derived_tables(right, query_type);
        }
        TableRef::Union { queries, .. } => {
            for q in queries.iter_mut() {
                dedup_query(q, query_type);
            }
        }
        TableRef::Subquery { query, .. } => dedup_query(query, query_type),
        TableRef::Scan { .. } => {}
    }
}

// ── Predicate analysis helpers ───────────────────────────────────────────────

/// Flatten an AND tree into a list of conjuncts.
fn flatten_and(expr: Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op: Op::And,
            left,
            right,
        } => {
            let mut out = flatten_and(*left);
            out.extend(flatten_and(*right));
            out
        }
        other => vec![other],
    }
}

/// Rebuild an AND chain from conjuncts. Returns None if empty.
fn conjoin(exprs: Vec<Expr>) -> Option<Expr> {
    exprs.into_iter().reduce(Expr::and)
}

/// Check if an expression only references columns from `alias`.
fn references_only(expr: &Expr, alias: &str) -> bool {
    match expr {
        Expr::Column { table, .. } => table == alias,
        Expr::Literal(_) | Expr::Param { .. } | Expr::Star => true,
        Expr::FuncCall { args, .. } => args.iter().all(|a| references_only(a, alias)),
        Expr::BinaryOp { left, right, .. } => {
            references_only(left, alias) && references_only(right, alias)
        }
        Expr::UnaryOp { expr, .. } => references_only(expr, alias),
        Expr::InSubquery { expr, .. } => references_only(expr, alias),
    }
}

fn is_deleted_filter(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp { left, .. }
            if matches!(left.as_ref(), Expr::Column { column, .. } if column == DELETED_COLUMN)
    )
}

/// Split outer WHERE into (pushable into dedup subquery, must stay outside).
/// Filters referencing only `alias` are pushable. `_deleted` is never pushed
/// because it must filter *after* dedup picks the latest version.
fn partition_filters(where_clause: Option<Expr>, alias: &str) -> (Vec<Expr>, Vec<Expr>) {
    let Some(expr) = where_clause else {
        return (vec![], vec![]);
    };

    let conjuncts = flatten_and(expr);
    let mut inner = vec![];
    let mut outer = vec![];

    for c in conjuncts {
        if !is_deleted_filter(&c) && references_only(&c, alias) {
            inner.push(c);
        } else {
            outer.push(c);
        }
    }

    (inner, outer)
}

// ── argMax strategy for search queries ───────────────────────────────────────
//
// Search queries scan a single node table with LIMIT n. The subquery/LIMIT 1 BY
// approach kills LIMIT pushdown (CH must read all rows to sort+dedup before
// the outer LIMIT takes effect). argMax preserves it:
//
//   SELECT id, argMax(status, _version) AS status, ...
//   FROM gl_pipeline WHERE startsWith(traversal_path, '1/') ...
//   GROUP BY id
//   HAVING argMax(_deleted, _version) = false
//       AND argMax(status, _version) = 'failed'
//   LIMIT 50
//
// Namespace filters (traversal_path) stay in WHERE for PK pruning.
// Value filters move to HAVING wrapped in argMax to check the latest version.

/// Wrap column references in `argMaxIfOrNull(col, _version, _deleted = false)`.
/// Uses the -If combinator so only non-deleted rows are considered, and -OrNull
/// so fully-deleted groups return NULL (filtered out by HAVING IS NOT NULL).
fn wrap_in_argmax_if(expr: &Expr, alias: &str) -> Expr {
    let not_deleted = Expr::eq(Expr::col(alias, DELETED_COLUMN), Expr::lit(false));
    match expr {
        Expr::Column { table, column, .. } if table == alias && column != "id" => Expr::func(
            "argMaxIfOrNull",
            vec![expr.clone(), Expr::col(alias, VERSION_COLUMN), not_deleted],
        ),
        Expr::BinaryOp { op, left, right } => Expr::BinaryOp {
            op: *op,
            left: Box::new(wrap_in_argmax_if(left, alias)),
            right: Box::new(wrap_in_argmax_if(right, alias)),
        },
        Expr::FuncCall { name, args } => Expr::FuncCall {
            name: name.clone(),
            args: args.iter().map(|a| wrap_in_argmax_if(a, alias)).collect(),
        },
        Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
            op: *op,
            expr: Box::new(wrap_in_argmax_if(inner, alias)),
        },
        _ => expr.clone(),
    }
}

/// Apply argMaxIfOrNull dedup to a search query in-place.
///
/// Uses `argMaxIfOrNull(col, _version, _deleted = false)` so only non-deleted
/// rows are considered and fully-deleted groups return NULL. WHERE filters
/// stay in place for prewhere/index pruning. HAVING filters out NULL groups
/// (all versions deleted) via a single IS NOT NULL check on the first
/// non-id select column.
///
/// Correctness note: keeping value filters in WHERE means a row whose latest
/// non-deleted version no longer matches the filter could appear if an older
/// version matches AND there are unmerged duplicates. This is rare with
/// `cleanup = 1` and acceptable for search results.
fn apply_argmax_dedup(
    alias: &str,
    select: &mut [SelectExpr],
    where_clause: &mut Option<Expr>,
    group_by: &mut Vec<Expr>,
    having: &mut Option<Expr>,
    order_by: &mut [OrderExpr],
) {
    // Wrap non-id, non-constant select expressions in argMaxIfOrNull.
    for sel in select.iter_mut() {
        let is_id = matches!(&sel.expr, Expr::Column { table, column, .. }
            if table == alias && column == "id");
        let is_constant = matches!(&sel.expr, Expr::Literal(_) | Expr::Param { .. });
        if !is_id && !is_constant {
            sel.expr = wrap_in_argmax_if(&sel.expr, alias);
        }
    }

    group_by.push(Expr::col(alias, "id"));

    // HAVING: filter out groups where all versions are deleted.
    // argMaxIfOrNull returns NULL when no non-deleted rows exist;
    // use the _deleted column itself as the sentinel.
    let not_deleted = Expr::eq(Expr::col(alias, DELETED_COLUMN), Expr::lit(false));
    *having = Some(Expr::func(
        "isNotNull",
        vec![Expr::func(
            "argMaxIfOrNull",
            vec![
                Expr::col(alias, "id"),
                Expr::col(alias, VERSION_COLUMN),
                not_deleted,
            ],
        )],
    ));

    // Wrap ORDER BY expressions that reference the alias in argMaxIfOrNull.
    for ord in order_by.iter_mut() {
        let refs_alias = references_only(&ord.expr, alias)
            && !matches!(&ord.expr, Expr::Column { column, .. } if column == "id");
        if refs_alias {
            ord.expr = wrap_in_argmax_if(&ord.expr, alias);
        }
    }

    // WHERE stays untouched -- value filters remain for prewhere pruning.
    let _ = where_clause;
}

// ── Main dispatch ────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn wrap_node_scans(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    select: &mut Vec<SelectExpr>,
    group_by: &mut Vec<Expr>,
    having: &mut Option<Expr>,
    order_by: &mut Vec<OrderExpr>,
    query_type: QueryType,
) {
    match from {
        TableRef::Scan { table, alias } if is_node_table(table) => {
            let alias = alias.clone();

            // Search: use argMaxIfOrNull to preserve LIMIT pushdown.
            if query_type == QueryType::Search {
                apply_argmax_dedup(&alias, select, where_clause, group_by, having, order_by);
                return;
            }

            // Default: subquery with LIMIT 1 BY and predicate pushdown.
            let table_name = table.clone();
            let (inner_filters, mut outer_filters) = partition_filters(where_clause.take(), &alias);

            let deleted_filter = Expr::eq(Expr::col(&alias, DELETED_COLUMN), Expr::lit(false));
            outer_filters.insert(0, deleted_filter);
            *where_clause = conjoin(outer_filters);

            *from = TableRef::subquery(
                Query {
                    select: vec![SelectExpr::star()],
                    from: TableRef::scan(table_name, &alias),
                    where_clause: conjoin(inner_filters),
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
            wrap_node_scans(
                left,
                where_clause,
                select,
                group_by,
                having,
                order_by,
                query_type,
            );
            wrap_node_scans(
                right,
                where_clause,
                select,
                group_by,
                having,
                order_by,
                query_type,
            );
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

    fn where_contains(clause: &Option<Expr>, needle: &str) -> bool {
        clause
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
        deduplicate(&mut node, QueryType::Traversal);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
        assert!(inner.order_by[0].desc);
        assert!(where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "_deleted"));
        assert!(!where_contains(&q.where_clause, "state"));
    }

    #[test]
    fn skips_edge_table() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "src")],
            from: TableRef::scan("gl_edge", "e"),
            ..Default::default()
        }));
        deduplicate(&mut node, QueryType::Traversal);

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
        deduplicate(&mut node, QueryType::Traversal);

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
        deduplicate(&mut node, QueryType::Traversal);

        let Node::Query(q) = &node;
        let inner_p = find_subquery(&q.from, "p").expect("project should be wrapped");
        assert!(find_subquery(&q.from, "e").is_none());
        assert!(where_contains(&inner_p.where_clause, "traversal_path"));
        assert!(where_contains(&q.where_clause, "_deleted"));
        assert!(!where_contains(&q.where_clause, "traversal_path"));
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
        deduplicate(&mut node, QueryType::Traversal);

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
        deduplicate(&mut node, QueryType::Traversal);

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "u").expect("user should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn search_uses_argmax() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("pipe", "id"), "pipe_id"),
                SelectExpr::new(Expr::col("pipe", "status"), "pipe_status"),
                SelectExpr::new(Expr::col("pipe", "ref"), "pipe_ref"),
            ],
            from: TableRef::scan("gl_pipeline", "pipe"),
            where_clause: Some(Expr::and(
                Expr::func(
                    "startsWith",
                    vec![Expr::col("pipe", "traversal_path"), Expr::string("1/")],
                ),
                Expr::eq(Expr::col("pipe", "status"), Expr::string("failed")),
            )),
            limit: Some(50),
            ..Default::default()
        }));
        deduplicate(&mut node, QueryType::Search);

        let Node::Query(q) = &node;
        // No subquery -- argMaxIfOrNull applied in-place.
        assert!(
            matches!(&q.from, TableRef::Scan { table, .. } if table == "gl_pipeline"),
            "search should not wrap in subquery"
        );
        // GROUP BY pipe.id
        assert!(!q.group_by.is_empty(), "should add GROUP BY");
        // HAVING IS NOT NULL on the sentinel column (filters fully-deleted groups)
        assert!(q.having.is_some(), "should add HAVING clause");
        let having_str = format!("{:?}", q.having);
        assert!(
            having_str.contains("isNotNull") && having_str.contains("argMaxIfOrNull"),
            "HAVING should use isNotNull(argMaxIfOrNull(...))"
        );
        // SELECT status should be wrapped in argMaxIfOrNull
        let status_sel = &q.select[1];
        let sel_str = format!("{:?}", status_sel.expr);
        assert!(
            sel_str.contains("argMaxIfOrNull"),
            "status should use argMaxIfOrNull"
        );
        // All WHERE filters stay for prewhere pruning
        assert!(where_contains(&q.where_clause, "startsWith"));
        assert!(where_contains(&q.where_clause, "status"));
        // LIMIT preserved
        assert_eq!(q.limit, Some(50));
    }
}

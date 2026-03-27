//! Row deduplication pass for ReplacingMergeTree node tables.
//!
//! ClickHouse's `ReplacingMergeTree` deduplicates during background merges,
//! but between merges queries can see stale duplicates. This pass ensures
//! query-time correctness using a per-query-type strategy:
//!
//! | Query type   | Strategy                 | Why                                        |
//! |--------------|--------------------------|--------------------------------------------|
//! | Search       | argMaxIfOrNull + GROUP BY     | Preserves LIMIT pushdown                   |
//! | Traversal    | LIMIT 1 BY subquery           | Needs all columns for hydration/properties |
//! | Aggregation  | LIMIT 1 BY subquery           | Needs property columns for countIf/sumIf   |
//! | Neighbors    | LIMIT 1 BY subquery (CTEs)    | Edge-only lowering, node dedup via _nf CTE |
//! | PathFinding  | LIMIT 1 BY subquery           | Recursive CTEs, multi-hop joins            |
//! | Hydration    | (skipped)                     | Separate pipeline, no dedup pass           |
//!
//! Edge tables are always excluded -- their full-tuple ORDER BY makes RMT
//! dedup effective, and wrapping them would kill LIMIT pushdown.
//!

use crate::ast::{Expr, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::input::{Input, QueryType};
use ontology::constants::{DELETED_COLUMN, EDGE_TABLE, GL_TABLE_PREFIX, VERSION_COLUMN};

fn is_node_table(table: &str) -> bool {
    table.starts_with(GL_TABLE_PREFIX) && table != EDGE_TABLE
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Apply row deduplication to all node table scans in the AST.
pub fn deduplicate(node: &mut Node, input: &Input) {
    match node {
        Node::Query(q) => {
            for cte in &mut q.ctes {
                dedup_query(&mut cte.query, input);
            }
            dedup_query(q, input);
        }
    }
}

fn dedup_query(q: &mut Query, input: &Input) {
    visit_derived_tables(&mut q.from, input);
    for arm in &mut q.union_all {
        dedup_query(arm, input);
    }
    dispatch(q, input);
}

fn visit_derived_tables(from: &mut TableRef, input: &Input) {
    match from {
        TableRef::Join { left, right, .. } => {
            visit_derived_tables(left, input);
            visit_derived_tables(right, input);
        }
        TableRef::Union { queries, .. } => {
            for q in queries.iter_mut() {
                dedup_query(q, input);
            }
        }
        TableRef::Subquery { query, .. } => dedup_query(query, input),
        TableRef::Scan { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-query-type dispatch
// ─────────────────────────────────────────────────────────────────────────────

fn dispatch(q: &mut Query, input: &Input) {
    match &q.from {
        TableRef::Scan { table, alias } if is_node_table(table) => {
            let alias = alias.clone();
            let table_name = table.clone();

            match input.query_type {
                QueryType::Search => apply_argmax_dedup(q, &alias),
                QueryType::Traversal
                | QueryType::Aggregation
                | QueryType::PathFinding
                | QueryType::Neighbors => {
                    apply_limit_by_dedup(&mut q.from, &mut q.where_clause, table_name, alias);
                }
                QueryType::Hydration => {}
            }
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { .. } => {
            wrap_join_scans(&mut q.from, &mut q.where_clause);
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate helpers
// ─────────────────────────────────────────────────────────────────────────────

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

/// Split WHERE into (pushable into dedup subquery, must stay outside).
fn partition_filters(where_clause: Option<Expr>, alias: &str) -> (Vec<Expr>, Vec<Expr>) {
    let Some(expr) = where_clause else {
        return (vec![], vec![]);
    };

    let conjuncts = expr.flatten_and();
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

// ─────────────────────────────────────────────────────────────────────────────
// Strategy: argMaxIfOrNull (search)
// ─────────────────────────────────────────────────────────────────────────────

/// Wrap column references in `argMaxIfOrNull(col, _version, _deleted = false)`.
fn wrap_in_argmax_if(expr: &Expr, alias: &str) -> Expr {
    match expr {
        Expr::Column { table, column, .. } if table == alias && column != "id" => {
            let not_deleted = Expr::eq(Expr::col(alias, DELETED_COLUMN), Expr::lit(false));
            Expr::func(
                "argMaxIfOrNull",
                vec![expr.clone(), Expr::col(alias, VERSION_COLUMN), not_deleted],
            )
        }
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

fn apply_argmax_dedup(q: &mut Query, alias: &str) {
    for sel in q.select.iter_mut() {
        let is_id = matches!(&sel.expr, Expr::Column { table, column, .. }
            if table == alias && column == "id");
        let is_constant = matches!(&sel.expr, Expr::Literal(_) | Expr::Param { .. });
        if !is_id && !is_constant {
            sel.expr = wrap_in_argmax_if(&sel.expr, alias);
        }
    }

    q.group_by.push(Expr::col(alias, "id"));

    let not_deleted = Expr::eq(Expr::col(alias, DELETED_COLUMN), Expr::lit(false));
    let mut having_parts = vec![Expr::func(
        "isNotNull",
        vec![Expr::func(
            "argMaxIfOrNull",
            vec![
                Expr::col(alias, "id"),
                Expr::col(alias, VERSION_COLUMN),
                not_deleted,
            ],
        )],
    )];

    // Value filters duplicated into HAVING for correctness.
    if let Some(where_expr) = &q.where_clause {
        for conjunct in where_expr.clone().flatten_and() {
            if references_only(&conjunct, alias) {
                having_parts.push(wrap_in_argmax_if(&conjunct, alias));
            }
        }
    }

    q.having = Expr::conjoin(having_parts);

    for ord in q.order_by.iter_mut() {
        let refs_alias = references_only(&ord.expr, alias)
            && !matches!(&ord.expr, Expr::Column { column, .. } if column == "id");
        if refs_alias {
            ord.expr = wrap_in_argmax_if(&ord.expr, alias);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy: LIMIT 1 BY subquery (traversal, aggregation, neighbors, path)
// ─────────────────────────────────────────────────────────────────────────────

fn make_dedup_subquery(table_name: String, alias: &str, inner_filters: Vec<Expr>) -> TableRef {
    TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(table_name, alias),
            where_clause: Expr::conjoin(inner_filters),
            order_by: vec![OrderExpr {
                expr: Expr::col(alias, VERSION_COLUMN),
                desc: true,
            }],
            limit_by: Some((1, vec![Expr::col(alias, "id")])),
            ..Default::default()
        },
        alias.to_string(),
    )
}

fn apply_limit_by_dedup(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    table_name: String,
    alias: String,
) {
    let (inner_filters, mut outer_filters) = partition_filters(where_clause.take(), &alias);

    let deleted_filter = Expr::eq(Expr::col(&alias, DELETED_COLUMN), Expr::lit(false));
    outer_filters.insert(0, deleted_filter);
    *where_clause = Expr::conjoin(outer_filters);
    *from = make_dedup_subquery(table_name, &alias, inner_filters);
}

/// Recurse into join children, wrapping node table scans with LIMIT 1 BY.
fn wrap_join_scans(from: &mut TableRef, where_clause: &mut Option<Expr>) {
    match from {
        TableRef::Scan { table, alias } if is_node_table(table) => {
            let table_name = table.clone();
            let alias = alias.clone();
            apply_limit_by_dedup(from, where_clause, table_name, alias);
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            wrap_join_scans(left, where_clause);
            wrap_join_scans(right, where_clause);
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::Cte;
    use ontology::constants::TRAVERSAL_PATH_COLUMN;

    fn input_for(query_type: QueryType) -> Input {
        Input {
            query_type,
            ..Default::default()
        }
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

    // ── LIMIT 1 BY tests ────────────────────────────────────────────────

    #[test]
    fn traversal_wraps_node_scan() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::eq(Expr::col("mr", "state"), Expr::string("merged"))),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal));

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
        assert!(inner.order_by[0].desc);
        assert!(where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "_deleted"));
        assert!(!where_contains(&q.where_clause, "state"));
    }

    #[test]
    fn aggregation_wraps_node_scan() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Aggregation));

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn neighbors_wraps_node_scan() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Neighbors));

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn path_finding_wraps_node_scan() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::PathFinding));

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn skips_edge_table() {
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "src")],
            from: TableRef::scan("gl_edge", "e"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal));

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
        deduplicate(&mut node, &input_for(QueryType::Traversal));

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
        deduplicate(&mut node, &input_for(QueryType::Traversal));

        let Node::Query(q) = &node;
        let inner_p = find_subquery(&q.from, "p").expect("project should be wrapped");
        assert!(find_subquery(&q.from, "e").is_none());
        assert!(has_limit_by(inner_p));
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
        deduplicate(&mut node, &input_for(QueryType::Traversal));

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
        deduplicate(&mut node, &input_for(QueryType::Traversal));

        let Node::Query(q) = &node;
        let inner = find_subquery(&q.from, "u").expect("user should be wrapped");
        assert!(has_limit_by(inner));
    }

    // ── argMaxIfOrNull tests ────────────────────────────────────────────

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
        deduplicate(&mut node, &input_for(QueryType::Search));

        let Node::Query(q) = &node;
        assert!(
            matches!(&q.from, TableRef::Scan { table, .. } if table == "gl_pipeline"),
            "search should not wrap in subquery"
        );
        assert!(!q.group_by.is_empty(), "should add GROUP BY");
        assert!(q.having.is_some(), "should add HAVING clause");
        let having_str = format!("{:?}", q.having);
        assert!(
            having_str.contains("isNotNull") && having_str.contains("argMaxIfOrNull"),
            "HAVING should use isNotNull(argMaxIfOrNull(...))"
        );
        assert!(
            having_str.contains("failed"),
            "HAVING should re-check value filters via argMaxIfOrNull"
        );
        let status_sel = &q.select[1];
        let sel_str = format!("{:?}", status_sel.expr);
        assert!(
            sel_str.contains("argMaxIfOrNull"),
            "status should use argMaxIfOrNull"
        );
        assert!(where_contains(&q.where_clause, "startsWith"));
        assert!(where_contains(&q.where_clause, "status"));
        assert_eq!(q.limit, Some(50));
    }
}

//! Row deduplication pass for ReplacingMergeTree node tables.
//!
//! ClickHouse's `ReplacingMergeTree` deduplicates during background merges,
//! but between merges queries can see stale duplicates. This pass ensures
//! query-time correctness using a per-query-type strategy:
//!
//! | Query type   | Strategy                 | Why                                        |
//! |--------------|--------------------------|--------------------------------------------|
//! | Search       | LIMIT 1 BY subquery           | Streaming reads + early LIMIT termination  |
//! | Traversal    | LIMIT 1 BY subquery           | Needs all columns for hydration/properties |
//! | Aggregation  | LIMIT 1 BY subquery           | Needs property columns for countIf/sumIf   |
//! | Neighbors    | LIMIT 1 BY subquery (CTEs)    | Edge-only lowering, node dedup via _nf CTE |
//! | PathFinding  | LIMIT 1 BY subquery           | Recursive CTEs, multi-hop joins            |
//! | Hydration    | argMaxIfOrNull + GROUP BY     | Search-like UNION ALL of table scans       |
//! | _nf_* CTEs   | LIMIT 1 BY subquery           | Streaming reads, same as other node scans  |
//!
//! Edge tables are always excluded -- their full-tuple ORDER BY makes RMT
//! dedup effective, and wrapping them would kill LIMIT pushdown.
//!

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::ast::{Expr, Node, OrderExpr, Query, SelectExpr, TableRef};
use crate::constants::{TRAVERSAL_PATH_COLUMN, node_filter_cte};
use crate::input::{Input, QueryType};
use ontology::Ontology;
use ontology::constants::{DEFAULT_PRIMARY_KEY, DELETED_COLUMN, VERSION_COLUMN};
use regex::Regex;

/// Matches `gl_*` or `v{N}_gl_*` (schema-version-prefixed) table names.
static GL_TABLE_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(v\d+_)?gl_").expect("valid regex"));

fn is_node_table(table: &str, edge_tables: &HashSet<String>) -> bool {
    GL_TABLE_RE.is_match(table) && !edge_tables.contains(table)
}

fn is_edge_table(table: &str, edge_tables: &HashSet<String>) -> bool {
    edge_tables.contains(table)
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Apply row deduplication to all node table scans in the AST.
///
/// When `input.options.skip_dedup` is true, skips the `LIMIT 1 BY` wrapping
/// for node tables but still applies `_deleted = false` filters on all tables.
/// This trades correctness (stale duplicates may appear) for lower latency.
pub fn deduplicate(node: &mut Node, input: &Input, ontology: &Ontology) {
    match node {
        Node::Insert(_) => {}
        Node::Query(q) => {
            if input.options.skip_dedup {
                // Only apply soft-delete filters, no dedup subqueries.
                add_deleted_filters_recursive(q);
            } else if input.options.use_final {
                // Use FINAL modifier on node table scans instead of LIMIT 1 BY.
                // FINAL tells ClickHouse to resolve RMT duplicates at read time.
                // We still need _deleted = false since FINAL with
                // `do_not_merge_across_partitions_select_final` may not collapse
                // delete tombstones across partitions.
                apply_final_recursive(q, input);
            } else {
                for cte in &mut q.ctes {
                    if cte.name.starts_with("_nf_") {
                        dedup_nf_cte(&mut cte.query, input, ontology);
                    } else {
                        dedup_query(&mut cte.query, input, ontology);
                    }
                }
                dedup_query(q, input, ontology);
            }
        }
    }
}

/// When skip_dedup is set, add `_deleted = false` to all node and edge table
/// scans without wrapping them in LIMIT 1 BY subqueries.
fn add_deleted_filters_recursive(q: &mut Query) {
    for cte in &mut q.ctes {
        add_deleted_filters_recursive(&mut cte.query);
    }
    for arm in &mut q.union_all {
        add_deleted_filters_recursive(arm);
    }
    visit_from_for_deleted(&mut q.from, &mut q.where_clause);
}

/// When use_final is set, mark all node table scans with FINAL and add
/// `_deleted = false` filters. Edge tables get only the _deleted filter
/// (they don't need FINAL -- their full-tuple ORDER BY makes RMT dedup effective).
fn apply_final_recursive(q: &mut Query, input: &Input) {
    for cte in &mut q.ctes {
        apply_final_recursive(&mut cte.query, input);
    }
    for arm in &mut q.union_all {
        apply_final_recursive(arm, input);
    }
    visit_from_for_final(
        &mut q.from,
        &mut q.where_clause,
        &input.compiler.edge_tables,
    );
}

/// Walk the FROM tree, set `final_ = true` on node table scans,
/// and add `_deleted = false` for all gl_* table scans.
fn visit_from_for_final(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    edge_tables: &HashSet<String>,
) {
    match from {
        TableRef::Scan {
            table,
            alias,
            final_,
        } => {
            if GL_TABLE_RE.is_match(table) {
                // _deleted = false on all tables.
                let deleted_filter =
                    Expr::eq(Expr::col(alias.as_str(), DELETED_COLUMN), Expr::lit(false));
                *where_clause = Some(match where_clause.take() {
                    Some(existing) => Expr::and(existing, deleted_filter),
                    None => deleted_filter,
                });
                // FINAL only on node tables, not edges.
                if !edge_tables.contains(table.as_str()) {
                    *final_ = true;
                }
            }
        }
        TableRef::Join {
            left, right, on: _, ..
        } => {
            visit_from_for_final(left, where_clause, edge_tables);
            visit_from_for_final(right, where_clause, edge_tables);
        }
        TableRef::Subquery { .. } | TableRef::Union { .. } => {}
    }
}

/// Walk the FROM tree and add `_deleted = false` for every gl_* table scan.
fn visit_from_for_deleted(from: &mut TableRef, where_clause: &mut Option<Expr>) {
    match from {
        TableRef::Scan { table, alias, .. } => {
            if GL_TABLE_RE.is_match(table) {
                let deleted_filter =
                    Expr::eq(Expr::col(alias.as_str(), DELETED_COLUMN), Expr::lit(false));
                *where_clause = Some(match where_clause.take() {
                    Some(existing) => Expr::and(existing, deleted_filter),
                    None => deleted_filter,
                });
            }
        }
        TableRef::Join {
            left, right, on: _, ..
        } => {
            visit_from_for_deleted(left, where_clause);
            visit_from_for_deleted(right, where_clause);
        }
        TableRef::Subquery { .. } | TableRef::Union { .. } => {}
    }
}

/// Deduplicate a `_nf_*` CTE using LIMIT 1 BY. These CTEs always select `id`
/// for edge semi-joins and may carry extra stable columns for optimizer CTEs.
/// Using LIMIT 1 BY with the sort key prefix lets
/// ClickHouse stream in primary key order instead of hash-aggregating
/// the entire table.
fn dedup_nf_cte(q: &mut Query, input: &Input, ontology: &Ontology) {
    if let TableRef::Scan { table, alias, .. } = &q.from
        && is_node_table(table, &input.compiler.edge_tables)
    {
        let table = table.clone();
        let alias = alias.clone();
        let select = std::mem::take(&mut q.select);
        let selects_traversal_path = select
            .iter()
            .any(|expr| expr.alias.as_deref() == Some(TRAVERSAL_PATH_COLUMN));

        // When the CTE is fed by a cascade (WHERE id IN (SELECT id FROM
        // _cascade_*)), use [id] as the sort key so the ORDER BY becomes
        // `id ASC, _version DESC`. This lets ClickHouse use the by_id
        // projection instead of the main table's primary key order,
        // avoiding a full-table sort on traversal_path.
        let has_cascade = q
            .where_clause
            .as_ref()
            .is_some_and(|w| w.contains_in_subquery());

        if selects_traversal_path && !has_cascade {
            apply_limit_by_dedup_with_inner_filters(
                &mut q.from,
                &mut q.where_clause,
                &table,
                ontology,
            );
        } else if has_cascade {
            apply_limit_by_dedup_id_only(&mut q.from, &mut q.where_clause, selects_traversal_path);
        } else {
            apply_limit_by_dedup(&mut q.from, &mut q.where_clause, &table, ontology);
        }
        // The LIMIT 1 BY subquery selects *. Narrow the outer select back to
        // the lowerer's requested CTE columns.
        q.select = if select.is_empty() {
            vec![SelectExpr::new(Expr::col(&alias, "id"), "id")]
        } else {
            select
        };
    }
}

fn dedup_query(q: &mut Query, input: &Input, ontology: &Ontology) {
    visit_derived_tables(&mut q.from, input, ontology);
    for arm in &mut q.union_all {
        dedup_query(arm, input, ontology);
    }
    dispatch(q, input, ontology);
    add_edge_deleted_filters(&q.from, &mut q.where_clause, input);
}

/// Add `_deleted = false` filters for every edge table scan in the FROM tree.
/// Edge tables use ReplacingMergeTree with `_deleted` but are not wrapped
/// in dedup subqueries (their full-tuple ORDER BY makes RMT dedup effective).
/// Between merges, soft-deleted edge rows can still appear.
///
/// Only handles Scan and Join variants. Union arms and Subquery inner queries
/// are covered by `dedup_query`'s recursion, which calls this function on
/// each nested query's own FROM/WHERE.
fn add_edge_deleted_filters(from: &TableRef, where_clause: &mut Option<Expr>, input: &Input) {
    match from {
        TableRef::Scan { table, alias, .. }
            if is_edge_table(table, &input.compiler.edge_tables) =>
        {
            let filter = not_deleted(alias);
            *where_clause = Some(match where_clause.take() {
                Some(existing) => Expr::and(existing, filter),
                None => filter,
            });
        }
        TableRef::Join { left, right, .. } => {
            add_edge_deleted_filters(left, where_clause, input);
            add_edge_deleted_filters(right, where_clause, input);
        }
        _ => {}
    }
}

fn visit_derived_tables(from: &mut TableRef, input: &Input, ontology: &Ontology) {
    match from {
        TableRef::Join { left, right, .. } => {
            visit_derived_tables(left, input, ontology);
            visit_derived_tables(right, input, ontology);
        }
        TableRef::Union { queries, .. } => {
            for q in queries.iter_mut() {
                dedup_query(q, input, ontology);
            }
        }
        TableRef::Subquery { query, .. } => dedup_query(query, input, ontology),
        TableRef::Scan { .. } => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-query-type dispatch
// ─────────────────────────────────────────────────────────────────────────────

fn dispatch(q: &mut Query, input: &Input, ontology: &Ontology) {
    match &q.from {
        TableRef::Scan { table, alias, .. }
            if is_node_table(table, &input.compiler.edge_tables) =>
        {
            let alias = alias.clone();
            let table = table.clone();

            match input.query_type {
                QueryType::Traversal
                | QueryType::Aggregation
                | QueryType::PathFinding
                | QueryType::Neighbors => {
                    apply_limit_by_dedup(&mut q.from, &mut q.where_clause, &table, ontology);
                }
                QueryType::Hydration => apply_argmax_dedup(q, &alias),
            }
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { .. } => {
            let cte_filters: HashMap<String, Option<Expr>> = q
                .ctes
                .iter()
                .map(|c| (c.name.clone(), c.query.where_clause.clone()))
                .collect();
            wrap_join_scans(
                &mut q.from,
                &mut q.where_clause,
                &cte_filters,
                input,
                ontology,
            );
        }
        TableRef::Union { .. } | TableRef::Subquery { .. } => {}
    }
}

fn not_deleted(alias: &str) -> Expr {
    Expr::eq(Expr::col(alias, DELETED_COLUMN), Expr::lit(false))
}

// ─────────────────────────────────────────────────────────────────────────────
// Predicate helpers
// ─────────────────────────────────────────────────────────────────────────────

fn is_deleted_filter(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryOp { left, .. }
            if matches!(left.as_ref(), Expr::Column { column, .. } if column == DELETED_COLUMN)
    )
}

/// Split WHERE into (pushable into dedup subquery, must stay outside).
///
/// Only filters on *structural* columns (those in the table's sort key, which
/// are invariant across row versions) are pushed inside the dedup subquery.
/// Filters on mutable columns must stay outside so they evaluate against
/// the deduplicated (latest-version) row, not a potentially stale version.
fn partition_filters(
    where_clause: Option<Expr>,
    alias: &str,
    sort_key: &[String],
) -> (Vec<Expr>, Vec<Expr>) {
    let Some(expr) = where_clause else {
        return (vec![], vec![]);
    };

    let sort_key_set: HashSet<&str> = sort_key.iter().map(|s| s.as_str()).collect();
    let conjuncts = expr.flatten_and();
    let mut inner = vec![];
    let mut outer = vec![];

    for c in conjuncts {
        if !is_deleted_filter(&c)
            && c.references_only(alias)
            && references_only_sort_key(&c, &sort_key_set)
        {
            inner.push(c);
        } else {
            outer.push(c);
        }
    }

    (inner, outer)
}

/// Check if every column referenced by the expression is in the sort key.
fn references_only_sort_key(expr: &Expr, sort_key: &HashSet<&str>) -> bool {
    let cols = expr.referenced_columns();
    cols.iter().all(|c| sort_key.contains(c.as_str()))
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy: argMaxIfOrNull (hydration, _nf CTEs)
// ─────────────────────────────────────────────────────────────────────────────

/// Wrap column references in `argMaxIfOrNull(col, _version, _deleted = false)`.
fn wrap_in_argmax_if(expr: &Expr, alias: &str) -> Expr {
    match expr {
        Expr::Column { table, column, .. } if table == alias && column != "id" => Expr::func(
            "argMaxIfOrNull",
            vec![
                expr.clone(),
                Expr::col(alias, VERSION_COLUMN),
                not_deleted(alias),
            ],
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

    let mut having_parts = vec![Expr::func(
        "isNotNull",
        vec![Expr::func(
            "argMaxIfOrNull",
            vec![
                Expr::col(alias, "id"),
                Expr::col(alias, VERSION_COLUMN),
                not_deleted(alias),
            ],
        )],
    )];

    // Value filters duplicated into HAVING for correctness.
    if let Some(where_expr) = &q.where_clause {
        for conjunct in where_expr.clone().flatten_and() {
            if conjunct.references_only(alias) {
                having_parts.push(wrap_in_argmax_if(&conjunct, alias));
            }
        }
    }

    q.having = Expr::conjoin(having_parts);

    for ord in q.order_by.iter_mut() {
        let refs_alias = ord.expr.references_only(alias)
            && !matches!(&ord.expr, Expr::Column { column, .. } if column == "id");
        if refs_alias {
            ord.expr = wrap_in_argmax_if(&ord.expr, alias);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy: LIMIT 1 BY subquery (search, traversal, aggregation, neighbors, path)
// ─────────────────────────────────────────────────────────────────────────────

fn make_dedup_subquery(
    table_name: String,
    alias: &str,
    inner_filters: Vec<Expr>,
    sort_key: &[String],
) -> TableRef {
    // ORDER BY <sort_key columns ASC ...>, _version DESC
    // Prefixing with the table's sort key lets ClickHouse use the primary
    // key ordering for streaming reads (ReadType: InOrder) instead of
    // forcing a full sort.
    let mut order_by: Vec<OrderExpr> = sort_key
        .iter()
        .map(|col| OrderExpr {
            expr: Expr::col(alias, col),
            desc: false,
        })
        .collect();
    order_by.push(OrderExpr {
        expr: Expr::col(alias, VERSION_COLUMN),
        desc: true,
    });

    TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(table_name, alias),
            where_clause: Expr::conjoin(inner_filters),
            order_by,
            limit_by: Some((1, vec![Expr::col(alias, "id")])),
            ..Default::default()
        },
        alias.to_string(),
    )
}

/// Cascade-optimized dedup: uses `ORDER BY id, _version DESC` so ClickHouse
/// can pick the `by_id` projection instead of the main table's primary key
/// order. All WHERE filters are pushed inside since cascade CTEs have an
/// `id IN (...)` filter that already restricts the scan.
fn apply_limit_by_dedup_id_only(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    push_all_filters_inside: bool,
) {
    let (table_name, alias) = match from {
        TableRef::Scan { table, alias, .. } => (table.clone(), alias.clone()),
        _ => return,
    };
    let id_sort_key: &[String] = &[DEFAULT_PRIMARY_KEY.to_string()];
    if push_all_filters_inside {
        wrap_scan_with_limit_by_inner_filters(from, where_clause, table_name, alias, id_sort_key);
    } else {
        wrap_scan_with_limit_by(from, where_clause, table_name, alias, None, id_sort_key);
    }
}

fn apply_limit_by_dedup(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    table: &str,
    ontology: &Ontology,
) {
    let (table_name, alias) = match from {
        TableRef::Scan { table, alias, .. } => (table.clone(), alias.clone()),
        _ => return,
    };
    let sort_key = ontology.sort_key_for_table(table).unwrap_or_default();
    wrap_scan_with_limit_by(from, where_clause, table_name, alias, None, sort_key);
}

fn apply_limit_by_dedup_with_inner_filters(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    table: &str,
    ontology: &Ontology,
) {
    let (table_name, alias) = match from {
        TableRef::Scan { table, alias, .. } => (table.clone(), alias.clone()),
        _ => return,
    };
    let sort_key = ontology.sort_key_for_table(table).unwrap_or_default();
    wrap_scan_with_limit_by_inner_filters(from, where_clause, table_name, alias, sort_key);
}

fn wrap_scan_with_limit_by(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    table_name: String,
    alias: String,
    extra_inner_filter: Option<Expr>,
    sort_key: &[String],
) {
    let (mut inner_filters, mut outer_filters) =
        partition_filters(where_clause.take(), &alias, sort_key);
    inner_filters.extend(extra_inner_filter);

    outer_filters.insert(0, not_deleted(&alias));
    *where_clause = Expr::conjoin(outer_filters);
    *from = make_dedup_subquery(table_name, &alias, inner_filters, sort_key);
}

fn wrap_scan_with_limit_by_inner_filters(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    table_name: String,
    alias: String,
    sort_key: &[String],
) {
    let inner_filters = where_clause
        .take()
        .map(Expr::flatten_and)
        .unwrap_or_default();
    *where_clause = Some(not_deleted(&alias));
    *from = make_dedup_subquery(table_name, &alias, inner_filters, sort_key);
}

/// Recurse into join children, wrapping node table scans with LIMIT 1 BY.
/// When a `_nf_{alias}` CTE exists, its WHERE conditions are inlined into
/// the dedup subquery instead of referencing the CTE via InSubquery. This
/// avoids ClickHouse re-evaluating the CTE body (which scans the same node
/// table again) since ClickHouse inlines CTEs rather than materializing them.
fn wrap_join_scans(
    from: &mut TableRef,
    where_clause: &mut Option<Expr>,
    cte_filters: &HashMap<String, Option<Expr>>,
    input: &Input,
    ontology: &Ontology,
) {
    match from {
        TableRef::Scan { table, alias, .. }
            if is_node_table(table, &input.compiler.edge_tables) =>
        {
            let table_name = table.clone();
            let alias_str = alias.clone();
            let sort_key = ontology.sort_key_for_table(&table_name).unwrap_or_default();
            let nf_cte = node_filter_cte(&alias_str);

            // Prefer inlining the _nf_* CTE's WHERE conditions directly
            // into the dedup subquery. Falls back to InSubquery when the
            // CTE is cascade-derived (WHERE references edge aliases like
            // _ce.source_id that don't exist in the dedup subquery scope).
            // Cascade-derived CTEs are identified by containing InSubquery
            // anywhere in their WHERE tree.
            let (nf_filter, is_cascade) =
                cte_filters.get(&nf_cte).map_or((None, false), |cte_where| {
                    let Some(cte_where) = cte_where.as_ref() else {
                        return (None, false);
                    };
                    if cte_where.contains_in_subquery() {
                        // Cascade-derived: fall back to CTE reference
                        (
                            Some(Expr::InSubquery {
                                expr: Box::new(Expr::col(&alias_str, DEFAULT_PRIMARY_KEY)),
                                cte_name: nf_cte,
                                column: DEFAULT_PRIMARY_KEY.to_string(),
                            }),
                            true,
                        )
                    } else {
                        // Lowerer-created: inline the WHERE conditions
                        (Some(cte_where.clone()), false)
                    }
                });

            // When cascade-derived, use [id] as sort key so ClickHouse
            // picks the by_id projection instead of the main table order.
            let effective_sort_key: Vec<String>;
            let sort_key = if is_cascade {
                effective_sort_key = vec![DEFAULT_PRIMARY_KEY.to_string()];
                &effective_sort_key
            } else {
                sort_key
            };

            wrap_scan_with_limit_by(
                from,
                where_clause,
                table_name,
                alias_str,
                nf_filter,
                sort_key,
            );
        }
        TableRef::Scan { .. } => {}
        TableRef::Join { left, right, .. } => {
            wrap_join_scans(left, where_clause, cte_filters, input, ontology);
            wrap_join_scans(right, where_clause, cte_filters, input, ontology);
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

    fn ontology() -> Ontology {
        Ontology::load_embedded().expect("embedded ontology must be valid")
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
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::eq(Expr::col("mr", "state"), Expr::string("merged"))),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
        // ORDER BY should end with _version DESC, prefixed by sort key columns ASC
        let last_ord = inner.order_by.last().unwrap();
        assert!(last_ord.desc, "last ORDER BY should be _version DESC");
        // Sort key columns come first (ASC)
        assert!(
            !inner.order_by[0].desc,
            "first ORDER BY should be sort key column ASC"
        );
        // state is a mutable column -- must stay OUTSIDE the dedup subquery
        assert!(!where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "_deleted"));
        assert!(where_contains(&q.where_clause, "state"));
    }

    #[test]
    fn traversal_pushes_sort_key_filter_inside() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                Expr::func(
                    "startsWith",
                    vec![
                        Expr::col("mr", TRAVERSAL_PATH_COLUMN),
                        Expr::string("1/100/"),
                    ],
                ),
                Expr::eq(Expr::col("mr", "state"), Expr::string("merged")),
            )),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
        // traversal_path is in the sort key -- pushed inside
        assert!(where_contains(&inner.where_clause, "traversal_path"));
        // state is mutable -- stays outside
        assert!(!where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "_deleted"));
    }

    #[test]
    fn traversal_pushes_id_filter_inside() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                Expr::eq(Expr::col("mr", "id"), Expr::lit(42)),
                Expr::eq(Expr::col("mr", "state"), Expr::string("merged")),
            )),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        // id is in the sort key -- pushed inside
        assert!(where_contains(&inner.where_clause, "\"id\""));
        // state is mutable -- stays outside
        assert!(!where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "state"));
    }

    #[test]
    fn aggregation_wraps_node_scan() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Aggregation), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn aggregation_pushes_sort_key_filter_inside() {
        // When fold_filters_into_aggregates retains a structural conjunct in
        // the outer WHERE for a single-aggregate target, deduplicate must
        // hoist it into the LIMIT 1 BY subquery so ClickHouse can use the
        // primary-key index to skip granules. Regression guard for the
        // 411x slowdown on count(Definition where project_id=X).
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                Expr::eq(Expr::col("mr", "id"), Expr::lit(42)),
                Expr::eq(Expr::col("mr", "state"), Expr::lit("opened")),
            )),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Aggregation), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        // id is in the sort key -- pushed inside.
        assert!(where_contains(&inner.where_clause, "\"id\""));
        // state is mutable -- stays in outer WHERE.
        assert!(!where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "state"));
    }

    #[test]
    fn neighbors_wraps_node_scan() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Neighbors), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn path_finding_wraps_node_scan() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
            from: TableRef::scan("gl_merge_request", "mr"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::PathFinding), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn skips_edge_table_but_adds_deleted_filter() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("e", "source_id"), "src")],
            from: TableRef::scan("gl_edge", "e"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "gl_edge"));
        assert!(
            where_contains(&q.where_clause, "_deleted"),
            "edge scan should have _deleted filter"
        );
    }

    #[test]
    fn skips_non_graph_tables() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("c", "id"), "id")],
            from: TableRef::scan("some_cte", "c"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "some_cte"));
    }

    #[test]
    fn wraps_node_in_join_not_edge() {
        let ont = ontology();
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
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner_p = find_subquery(&q.from, "p").expect("project should be wrapped");
        assert!(find_subquery(&q.from, "e").is_none());
        assert!(has_limit_by(inner_p));
        assert!(where_contains(&inner_p.where_clause, "traversal_path"));
        assert!(where_contains(&q.where_clause, "_deleted"));
        assert!(!where_contains(&q.where_clause, "traversal_path"));
    }

    #[test]
    fn nf_cte_uses_limit_by() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "_nf_mr",
                Query {
                    select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                    from: TableRef::scan("gl_merge_request", "mr"),
                    where_clause: Some(Expr::eq(Expr::col("mr", "state"), Expr::string("merged"))),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("_nf_mr", "b"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let cte_q = &q.ctes[0].query;
        // _nf_* CTEs should use LIMIT 1 BY, wrapped in a subquery
        let inner = find_subquery(&cte_q.from, "mr").expect("CTE scan should be wrapped");
        assert!(has_limit_by(inner), "inner should have LIMIT 1 BY");
        // state is mutable -- stays outside the dedup subquery
        assert!(!where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&cte_q.where_clause, "state"));
        assert!(where_contains(&cte_q.where_clause, "_deleted"));
        // CTE outer select should be narrowed to just `id`
        assert_eq!(cte_q.select.len(), 1);
        assert_eq!(cte_q.select[0].alias, Some("id".to_string()));
        assert!(matches!(&q.from, TableRef::Scan { table, .. } if table == "_nf_mr"));
    }

    #[test]
    fn non_nf_cte_uses_limit_by() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "some_other_cte",
                Query {
                    select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                    from: TableRef::scan("gl_merge_request", "mr"),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("some_other_cte", "b"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.ctes[0].query.from, "mr").expect("CTE scan wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn user_table_dedup() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![SelectExpr::new(Expr::col("u", "username"), "name")],
            from: TableRef::scan("gl_user", "u"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "u").expect("user should be wrapped");
        assert!(has_limit_by(inner));
    }

    #[test]
    fn mutable_filter_stays_outside_dedup_subquery() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("mr", "id"), "id"),
                SelectExpr::new(Expr::col("mr", "state"), "state"),
            ],
            from: TableRef::scan("gl_merge_request", "mr"),
            where_clause: Some(Expr::and(
                Expr::eq(Expr::col("mr", "state"), Expr::string("merged")),
                Expr::func(
                    "startsWith",
                    vec![Expr::col("mr", TRAVERSAL_PATH_COLUMN), Expr::string("1/")],
                ),
            )),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "mr").expect("should be wrapped");

        // traversal_path is structural (in sort key) -- pushed inside
        assert!(where_contains(&inner.where_clause, "traversal_path"));
        // state is mutable -- must stay outside for correctness
        assert!(!where_contains(&inner.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "state"));
        assert!(where_contains(&q.where_clause, "_deleted"));
    }

    // ── argMaxIfOrNull tests ────────────────────────────────────────────

    #[test]
    fn search_uses_limit_by() {
        let ont = ontology();
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
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let inner = find_subquery(&q.from, "pipe").expect("search should wrap in subquery");
        assert!(has_limit_by(inner), "inner subquery should have LIMIT 1 BY");
        // ORDER BY should end with _version DESC, prefixed by sort key columns ASC
        let last_ord = inner.order_by.last().unwrap();
        assert!(last_ord.desc, "last ORDER BY should be _version DESC");
        assert!(
            !inner.order_by[0].desc,
            "first ORDER BY should be sort key column ASC"
        );
        // traversal_path is in the sort key -- pushed inside
        assert!(where_contains(&inner.where_clause, "traversal_path"));
        // status is mutable -- stays outside
        assert!(!where_contains(&inner.where_clause, "status"));
        assert!(where_contains(&q.where_clause, "status"));
        assert!(where_contains(&q.where_clause, "_deleted"));
        assert_eq!(q.limit, Some(50));
    }

    #[test]
    fn hydration_uses_argmax() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            select: vec![
                SelectExpr::new(Expr::col("hydrate", "id"), "hydrate_id"),
                SelectExpr::new(Expr::string("User"), "hydrate_entity_type"),
                SelectExpr::new(
                    Expr::func(
                        "toJSONString",
                        vec![Expr::func(
                            "map",
                            vec![
                                Expr::string("username"),
                                Expr::func("toString", vec![Expr::col("hydrate", "username")]),
                            ],
                        )],
                    ),
                    "hydrate_props",
                ),
            ],
            from: TableRef::scan("gl_user", "hydrate"),
            where_clause: Some(Expr::func(
                "in",
                vec![Expr::col("hydrate", "id"), Expr::lit(1)],
            )),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Hydration), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(
            matches!(&q.from, TableRef::Scan { table, .. } if table == "gl_user"),
            "hydration should not wrap in subquery"
        );
        assert!(!q.group_by.is_empty(), "should add GROUP BY");
        assert!(q.having.is_some(), "should add HAVING clause");
        let having_str = format!("{:?}", q.having);
        assert!(
            having_str.contains("argMaxIfOrNull"),
            "HAVING should use argMaxIfOrNull"
        );
        let props_sel = &q.select[2];
        let sel_str = format!("{:?}", props_sel.expr);
        assert!(
            sel_str.contains("argMaxIfOrNull"),
            "props column should use argMaxIfOrNull"
        );
    }

    #[test]
    fn hydration_union_all_deduplicates_each_arm() {
        let ont = ontology();
        let user_arm = Query {
            select: vec![
                SelectExpr::new(Expr::col("hydrate", "id"), "hydrate_id"),
                SelectExpr::new(Expr::string("User"), "hydrate_entity_type"),
                SelectExpr::new(
                    Expr::func("toString", vec![Expr::col("hydrate", "username")]),
                    "hydrate_props",
                ),
            ],
            from: TableRef::scan("gl_user", "hydrate"),
            where_clause: Some(Expr::func(
                "in",
                vec![Expr::col("hydrate", "id"), Expr::lit(1)],
            )),
            ..Default::default()
        };
        let project_arm = Query {
            select: vec![
                SelectExpr::new(Expr::col("hydrate", "id"), "hydrate_id"),
                SelectExpr::new(Expr::string("Project"), "hydrate_entity_type"),
                SelectExpr::new(
                    Expr::func("toString", vec![Expr::col("hydrate", "name")]),
                    "hydrate_props",
                ),
            ],
            from: TableRef::scan("gl_project", "hydrate"),
            where_clause: Some(Expr::func(
                "in",
                vec![Expr::col("hydrate", "id"), Expr::lit(2)],
            )),
            ..Default::default()
        };
        let mut first = user_arm;
        first.union_all.push(project_arm);
        let mut node = Node::Query(Box::new(first));
        deduplicate(&mut node, &input_for(QueryType::Hydration), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        assert!(!q.group_by.is_empty(), "first arm should have GROUP BY");
        assert!(q.having.is_some(), "first arm should have HAVING");
        assert_eq!(q.union_all.len(), 1);
        let arm2 = &q.union_all[0];
        assert!(!arm2.group_by.is_empty(), "second arm should have GROUP BY");
        assert!(arm2.having.is_some(), "second arm should have HAVING");
    }

    #[test]
    fn cascade_fed_nf_cte_uses_id_only_sort_key() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "_nf_mr",
                Query {
                    select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                    from: TableRef::scan("gl_merge_request", "mr"),
                    where_clause: Some(Expr::and(
                        Expr::func(
                            "startsWith",
                            vec![
                                Expr::col("mr", TRAVERSAL_PATH_COLUMN),
                                Expr::string("1/100/"),
                            ],
                        ),
                        Expr::InSubquery {
                            expr: Box::new(Expr::col("mr", "id")),
                            cte_name: "_cascade_mr".to_string(),
                            column: "id".to_string(),
                        },
                    )),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("_nf_mr", "b"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let cte_q = &q.ctes[0].query;
        let inner = find_subquery(&cte_q.from, "mr").expect("CTE scan should be wrapped");
        assert!(has_limit_by(inner), "inner should have LIMIT 1 BY");
        // CASCADE-FED: ORDER BY should be [id ASC, _version DESC], NOT
        // [traversal_path ASC, id ASC, _version DESC]. This lets
        // ClickHouse use the by_id projection.
        assert_eq!(
            inner.order_by.len(),
            2,
            "should have exactly 2 ORDER BY columns (id, _version)"
        );
        let first = &inner.order_by[0];
        assert!(
            matches!(&first.expr, Expr::Column { column, .. } if column == "id"),
            "first ORDER BY should be id"
        );
        assert!(!first.desc, "id should be ASC");
        let second = &inner.order_by[1];
        assert!(second.desc, "second ORDER BY should be _version DESC");
    }

    #[test]
    fn non_cascade_nf_cte_uses_full_sort_key() {
        let ont = ontology();
        let mut node = Node::Query(Box::new(Query {
            ctes: vec![Cte::new(
                "_nf_mr",
                Query {
                    select: vec![SelectExpr::new(Expr::col("mr", "id"), "id")],
                    from: TableRef::scan("gl_merge_request", "mr"),
                    where_clause: Some(Expr::eq(Expr::col("mr", "state"), Expr::string("merged"))),
                    ..Default::default()
                },
            )],
            select: vec![SelectExpr::new(Expr::col("b", "id"), "id")],
            from: TableRef::scan("_nf_mr", "b"),
            ..Default::default()
        }));
        deduplicate(&mut node, &input_for(QueryType::Traversal), &ont);

        let Node::Query(q) = &node else {
            unreachable!()
        };
        let cte_q = &q.ctes[0].query;
        let inner = find_subquery(&cte_q.from, "mr").expect("CTE scan should be wrapped");
        assert!(has_limit_by(inner), "inner should have LIMIT 1 BY");
        // NON-CASCADE: ORDER BY should include traversal_path (full sort key)
        assert!(
            inner.order_by.len() > 2,
            "non-cascade should have full sort key + _version"
        );
    }

    #[test]
    fn gl_table_regex_matches_prefixed_and_unprefixed() {
        assert!(GL_TABLE_RE.is_match("gl_user"));
        assert!(GL_TABLE_RE.is_match("gl_edge"));
        assert!(GL_TABLE_RE.is_match("v1_gl_user"));
        assert!(GL_TABLE_RE.is_match("v99_gl_merge_request"));
        assert!(!GL_TABLE_RE.is_match("siphon_users"));
        assert!(!GL_TABLE_RE.is_match("checkpoint"));
        assert!(!GL_TABLE_RE.is_match("v1_checkpoint"));
    }
}

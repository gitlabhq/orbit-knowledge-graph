//! Shared emit helpers: latest-row scans, columns, predicates, node hydration, edge predicates.

use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use crate::passes::plan::*;
use crate::passes::shared::{
    deleted_false, denorm_tag_expr, filter_to_expr, id_list_predicate, id_range_predicate,
    rel_kind_filter, rel_kind_filter_values,
};

// ─────────────────────────────────────────────────────────────────────────────
// Shared primitives: latest-row scans, columns, predicates
// ─────────────────────────────────────────────────────────────────────────────

/// Predicates applied after `FINAL` has resolved each node's latest row.
pub(super) fn latest_node_predicates(alias: &str, np: &NodePlan) -> Vec<Expr> {
    let mut predicates = Vec::new();
    for (prop, filter) in &np.filters {
        predicates.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        predicates.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        predicates.push(id_range_predicate(alias, range));
    }
    predicates.push(deleted_false(alias));
    predicates
}

/// Predicates for a candidate-id prefilter. These run before `FINAL`, so they
/// may over-select stale rows, but the outer latest-row scan re-applies the
/// same predicates after `FINAL`.
pub(super) fn candidate_node_predicates(alias: &str, np: &NodePlan) -> Vec<Expr> {
    let mut predicates = Vec::new();
    for (prop, filter) in &np.filters {
        predicates.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        predicates.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        predicates.push(id_range_predicate(alias, range));
    }
    predicates.push(deleted_false(alias));
    predicates
}

/// WHERE predicates for a node: filters + _deleted=false.
/// Node columns for the outer SELECT, aliased as `{alias}_{col}` for the
/// graph formatter. Only for non-aggregation queries (aggregation builds
/// its own SELECT).
pub(super) fn node_select_columns(alias: &str, np: &NodePlan) -> Vec<SelectExpr> {
    if !np.emit_select {
        return vec![];
    }
    crate::passes::shared::requested_columns(&np.columns)
        .into_iter()
        .map(|col| SelectExpr::new(Expr::col(alias, &col), format!("{alias}_{col}")))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: node hydration
// ─────────────────────────────────────────────────────────────────────────────

/// Narrowing source for a node's latest-row scan: a `_narrow_*` CTE referenced
/// by the node scan's WHERE clause.
pub(super) enum NarrowSource {
    Cte(String),
}

pub(super) fn emit_node_join_with_narrowing(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    narrow: Option<NarrowSource>,
    sort_key: &[String],
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    emit_node_join_inner(
        from,
        np,
        edge_alias,
        edge_col,
        use_traversal_path_join,
        narrow,
        sort_key,
    )
}

/// JOIN a node's latest-row LIMIT BY scan into the FROM tree.
fn emit_node_join_inner(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    narrow: Option<NarrowSource>,
    sort_key: &[String],
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;

    let in_predicate = narrow.map(|NarrowSource::Cte(cte_name)| Expr::InSubquery {
        expr: Box::new(Expr::col(alias, DEFAULT_PRIMARY_KEY)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    });

    let selects = node_select_columns(alias, np);
    let mut wheres = latest_node_predicates(alias, np);
    if let Some(in_predicate) = in_predicate {
        wheres.push(in_predicate);
    }

    let mut order_by: Vec<OrderExpr> = sort_key
        .iter()
        .map(|col| OrderExpr::asc(Expr::col(alias, col)))
        .collect();
    order_by.push(OrderExpr::desc(Expr::col(alias, VERSION_COLUMN)));

    let limit_by_cols: Vec<Expr> = sort_key.iter().map(|col| Expr::col(alias, col)).collect();

    let node_scan = TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: TableRef::scan(table, alias),
            where_clause: Expr::conjoin(wheres),
            order_by,
            limit_by: Some((1, limit_by_cols)),
            ..Default::default()
        },
        alias,
    );

    let joined = TableRef::join(
        JoinType::Inner,
        from,
        node_scan,
        node_join_condition(alias, edge_alias, edge_col, use_traversal_path_join, np),
    );

    Ok((joined, selects, vec![]))
}

fn node_join_condition(
    alias: &str,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    np: &NodePlan,
) -> Expr {
    let mut on = Expr::eq(
        Expr::col(alias, DEFAULT_PRIMARY_KEY),
        Expr::col(edge_alias, edge_col),
    );
    if use_traversal_path_join && np.has_traversal_path {
        on = Expr::and(
            on,
            Expr::eq(
                Expr::col(alias, TRAVERSAL_PATH_COLUMN),
                Expr::col(edge_alias, TRAVERSAL_PATH_COLUMN),
            ),
        );
    }
    on
}

pub(super) fn emit_filter_subquery(
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    ctes: &mut Vec<Cte>,
) -> Result<Vec<Expr>> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;
    let cte_name = format!("_filter_{alias}");

    ctes.push(Cte::new(
        &cte_name,
        Query {
            select: vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)],
            from: TableRef::scan_final(table, alias),
            where_clause: Expr::conjoin(latest_node_predicates(alias, np)),
            ..Default::default()
        },
    ));

    Ok(vec![Expr::InSubquery {
        expr: Box::new(Expr::col(edge_alias, edge_col)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    }])
}

pub(super) fn node_ids_from_final_scan(alias: &str, table: &str, np: &NodePlan) -> Query {
    Query {
        select: vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)],
        from: TableRef::scan_final(table, alias),
        where_clause: Expr::conjoin(latest_node_predicates(alias, np)),
        ..Default::default()
    }
}

pub(super) fn node_ids_from_candidate_scan(
    alias: &str,
    table: &str,
    np: &NodePlan,
    extra_predicates: Vec<Expr>,
) -> Query {
    let mut predicates = candidate_node_predicates(alias, np);
    predicates.extend(extra_predicates);
    Query {
        select: vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)],
        distinct: true,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(predicates),
        ..Default::default()
    }
}

pub(super) fn fk_values_from_candidate_scan(
    alias: &str,
    table: &str,
    fk_column: &str,
    np: &NodePlan,
    extra_predicates: Vec<Expr>,
) -> Query {
    let mut predicates = candidate_node_predicates(alias, np);
    predicates.extend(extra_predicates);
    Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, fk_column),
            DEFAULT_PRIMARY_KEY,
        )],
        distinct: true,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(predicates),
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: edge predicates
// ─────────────────────────────────────────────────────────────────────────────

pub(super) fn push_edge_predicates(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    table_columns: &HashMap<String, HashSet<String>>,
    skip_deleted: bool,
) {
    let (start_col, end_col) = hop.direction.edge_columns();

    if let Some(f) = rel_kind_filter(alias, &hop.rel_types) {
        where_parts.push(f);
    }
    // Entity kind filters.
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        if let Some(np) = nodes.get(node_alias)
            && let Some(ref entity) = np.entity
        {
            let kind_col = if id_col == SOURCE_ID_COLUMN {
                SOURCE_KIND_COLUMN
            } else {
                TARGET_KIND_COLUMN
            };
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }
    if !skip_deleted {
        where_parts.push(deleted_false(alias));
    }

    // Push node-level filters down to the edge scan when the edge table
    // carries those columns (e.g. project_id, branch on gl_code_edge).
    // This lets ClickHouse use the primary key prefix for scoping.
    // Deduplicate by property name so we don't emit the same predicate
    // twice when both nodes share the filter (common case: both are
    // code entities in the same project).
    if let Some(edge_cols) = table_columns.get(&hop.edge_table) {
        let reserved: HashSet<&str> = ontology::constants::EDGE_RESERVED_COLUMNS
            .iter()
            .copied()
            .collect();
        let mut seen_props: HashSet<&str> = HashSet::new();
        for node_alias in [&hop.from_node, &hop.to_node] {
            if let Some(np) = nodes.get(node_alias) {
                for (prop, filter) in &np.filters {
                    if edge_cols.contains(prop)
                        && !reserved.contains(prop.as_str())
                        && seen_props.insert(prop.as_str())
                    {
                        where_parts.push(filter_to_expr(alias, prop, filter));
                    }
                }
            }
        }
    }
}

pub(super) fn dedup_edge_scan(
    edge_table: &str,
    alias: &str,
    table_columns: &HashMap<String, HashSet<String>>,
    inner_predicates: Vec<Expr>,
) -> TableRef {
    let Some(cols) = table_columns.get(edge_table) else {
        return TableRef::scan_final(edge_table, alias);
    };

    let identity: Vec<&str> = EDGE_RESERVED_COLUMNS
        .iter()
        .copied()
        .filter(|c| cols.contains(*c))
        .collect();

    let mut projected: Vec<&str> = cols
        .iter()
        .map(String::as_str)
        .filter(|c| !identity.contains(c) && *c != VERSION_COLUMN && *c != DELETED_COLUMN)
        .collect();
    projected.sort_unstable();

    let mut select = Vec::with_capacity(identity.len() + projected.len());
    for col in &identity {
        select.push(SelectExpr::col(alias, *col));
    }
    for col in projected {
        select.push(SelectExpr::new(
            Expr::func(
                "argMax",
                vec![Expr::col(alias, col), Expr::col(alias, VERSION_COLUMN)],
            ),
            col,
        ));
    }

    let group_by = identity
        .iter()
        .map(|c| Expr::col(alias, *c))
        .collect::<Vec<_>>();

    let having = Expr::eq(
        Expr::func(
            "argMax",
            vec![
                Expr::col(alias, DELETED_COLUMN),
                Expr::col(alias, VERSION_COLUMN),
            ],
        ),
        Expr::lit(false),
    );

    let where_clause = Expr::conjoin(inner_predicates);

    let query = Query {
        select,
        from: TableRef::scan(edge_table, alias),
        where_clause,
        group_by,
        having: Some(having),
        ..Default::default()
    };
    TableRef::subquery(query, alias)
}

/// Build a `LIMIT 1 BY (PK) ORDER BY (PK, _version DESC)` subquery for
/// single-hop edge aggregations, with WHERE predicates injected.
///
/// The predicates appear in the inner WHERE (for PK index pruning) and
/// are also stored by the caller for duplication into `-If` combinators.
pub(super) fn limit_by_edge_scan(
    edge_table: &str,
    alias: &str,
    sort_key: &[String],
    where_predicates: Vec<Expr>,
) -> TableRef {
    let mut order_by: Vec<OrderExpr> = sort_key
        .iter()
        .map(|col| OrderExpr::asc(Expr::col(alias, col)))
        .collect();
    order_by.push(OrderExpr::desc(Expr::col(alias, VERSION_COLUMN)));

    let limit_by_cols: Vec<Expr> = sort_key.iter().map(|col| Expr::col(alias, col)).collect();

    let query = Query {
        select: vec![SelectExpr::star()],
        from: TableRef::scan(edge_table, alias),
        where_clause: Expr::conjoin(where_predicates),
        order_by,
        limit_by: Some((1, limit_by_cols)),
        ..Default::default()
    };
    TableRef::subquery(query, alias)
}

/// Emit denorm tag filters computed from `plan.denorm_columns`.
///
/// Each node is tagged at most once (tracked by `tagged_nodes`).
pub(super) fn emit_denorm_tags(
    where_parts: &mut Vec<Expr>,
    plan: &Plan,
    hop: &Hop,
    edge_alias: &str,
    start_col: &str,
    end_col: &str,
    tagged_nodes: &mut HashSet<String>,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        if !tagged_nodes.insert(node_alias.clone()) {
            continue;
        }
        let Some(np) = plan.nodes.get(node_alias) else {
            continue;
        };
        let Some(ref entity) = np.entity else {
            continue;
        };
        let dir = if id_col == SOURCE_ID_COLUMN {
            "source"
        } else {
            "target"
        };
        for (prop, filter) in &np.filters {
            let key = (entity.clone(), prop.clone(), dir.to_string());
            if let Some((tag_col, tag_key)) = plan.denorm_columns.get(&key)
                && let Some(expr) = denorm_tag_expr(edge_alias, tag_col, tag_key, filter)
            {
                where_parts.push(expr);
            }
        }
    }
}

pub(super) fn node_id_pin_predicates(
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
) -> Vec<Expr> {
    let (start_col, end_col) = hop.direction.edge_columns();
    let mut out = Vec::new();
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        if let Some(ref range) = np.id_range {
            out.push(Expr::and(
                Expr::binary(Op::Ge, Expr::col(alias, id_col), Expr::int(range.start)),
                Expr::binary(Op::Le, Expr::col(alias, id_col), Expr::int(range.end)),
            ));
        }
        if !np.node_ids.is_empty() {
            out.push(id_list_predicate(alias, id_col, &np.node_ids));
        }
    }
    out
}

pub(super) fn emit_node_ids_on_edge(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    start_col: &str,
    end_col: &str,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        if let Some(ref range) = np.id_range {
            where_parts.push(Expr::and(
                Expr::binary(Op::Ge, Expr::col(alias, id_col), Expr::int(range.start)),
                Expr::binary(Op::Le, Expr::col(alias, id_col), Expr::int(range.end)),
            ));
        }
        if !np.node_ids.is_empty() {
            where_parts.push(id_list_predicate(alias, id_col, &np.node_ids));
        }
    }
}

/// Narrow edge scan via node filter CTEs.
/// For FilterOnly nodes, the `_filter_*` CTE is created later in the node
/// processing phase — we just reference it here. For Join nodes with property
/// filters, we create a lightweight narrowing CTE on the spot.
#[allow(clippy::too_many_arguments)]
pub(super) fn emit_filter_narrowing(
    where_parts: &mut Vec<Expr>,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    edge_alias: &str,
    start_col: &str,
    end_col: &str,
    ctes: &mut Vec<Cte>,
    narrowed: &mut HashSet<String>,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = nodes.get(node_alias) else {
            continue;
        };
        let has_point_selectivity = !np.node_ids.is_empty() || np.id_range.is_some();
        let has_selective_filters = np
            .filters
            .iter()
            .any(|(_, f)| f.selectivity == ontology::FieldSelectivity::High);
        let selective = has_point_selectivity || has_selective_filters;
        let should_narrow = match np.hydration {
            // FilterOnly nodes get their CTE + IN predicate from
            // emit_filter_subquery in the second loop.
            HydrationStrategy::FilterOnly => false,
            HydrationStrategy::Join => selective,
            HydrationStrategy::Skip => false,
        };
        if !should_narrow {
            continue;
        }
        let cte_name = format!("_filter_{node_alias}");
        // Create the CTE once per node for selective Join nodes.
        // FilterOnly CTEs are created by emit_filter_subquery in the
        // second loop (it handles the full filter/node_ids/id_range logic).
        if np.hydration == HydrationStrategy::Join && narrowed.insert(node_alias.clone()) {
            let table = np.table.as_deref().unwrap_or("");
            ctes.push(Cte::new(
                &cte_name,
                node_ids_from_final_scan(node_alias, table, np),
            ));
        }
        where_parts.push(Expr::InSubquery {
            expr: Box::new(Expr::col(edge_alias, id_col)),
            cte_name,
            column: "id".to_string(),
        });
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Variable-length: UNION ALL of edge chains
// ─────────────────────────────────────────────────────────────────────────────

pub(super) fn build_multi_hop_union(
    hop: &Hop,
    alias: &str,
    nodes: &HashMap<String, NodePlan>,
) -> (TableRef, Vec<Expr>) {
    let start = hop.min_hops.max(1);
    let (start_col, end_col) = hop.direction.edge_columns();
    let end_type_col = match hop.direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };

    let type_filter = rel_kind_filter_values(&hop.rel_types);

    let queries: Vec<Query> = (start..=hop.max_hops)
        .map(|depth| {
            build_depth_arm(
                depth,
                &hop.edge_table,
                start_col,
                end_col,
                end_type_col,
                hop.direction,
                &type_filter,
            )
        })
        .collect();

    let union = TableRef::union_all(queries, alias);

    // For incoming edges, the from_node is on the target side and the
    // to_node is on the source side (the depth arm already swaps the
    // projected source/target columns, so the outer alias exposes
    // source_id/source_kind as the "start" of the incoming traversal).
    let mut where_parts = Vec::new();
    let (from_kind_col, to_kind_col) = match hop.direction {
        Direction::Outgoing | Direction::Both => (SOURCE_KIND_COLUMN, TARGET_KIND_COLUMN),
        Direction::Incoming => (TARGET_KIND_COLUMN, SOURCE_KIND_COLUMN),
    };
    for (node_alias, kind_col) in [(&hop.from_node, from_kind_col), (&hop.to_node, to_kind_col)] {
        if let Some(np) = nodes.get(node_alias)
            && let Some(ref entity) = np.entity
        {
            where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
        }
    }
    where_parts.push(deleted_false(alias));

    (union, where_parts)
}

pub(super) fn build_depth_arm(
    depth: u32,
    edge_table: &str,
    start_col: &str,
    end_col: &str,
    end_type_col: &str,
    direction: Direction,
    type_filter: &Option<Vec<String>>,
) -> Query {
    let mut from = TableRef::scan(edge_table, "e1");
    // First edge: relationship kind + _deleted filter.
    let mut where_parts = Vec::new();
    if let Some(types) = type_filter
        && let Some(f) = Expr::col_in(
            "e1",
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    {
        where_parts.push(f);
    }
    where_parts.push(deleted_false("e1"));
    let where_clause = Expr::conjoin(where_parts);

    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let right = TableRef::scan(edge_table, &curr);
        let mut join_on = Expr::eq(Expr::col(&prev, end_col), Expr::col(&curr, start_col));
        // _deleted = false on every chained edge.
        join_on = Expr::and(join_on, deleted_false(&curr));
        if let Some(types) = type_filter
            && let Some(tc) = Expr::col_in(
                &curr,
                RELATIONSHIP_KIND_COLUMN,
                ChType::String,
                types
                    .iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            )
        {
            join_on = Expr::and(join_on, tc);
        }
        from = TableRef::join(JoinType::Inner, from, right, join_on);
    }

    let last = format!("e{depth}");

    let (rel_kind, src_id, src_kind, tgt_id, tgt_kind) = match direction {
        Direction::Outgoing | Direction::Both => (
            Expr::col("e1", RELATIONSHIP_KIND_COLUMN),
            Expr::col("e1", SOURCE_ID_COLUMN),
            Expr::col("e1", SOURCE_KIND_COLUMN),
            Expr::col(&last, TARGET_ID_COLUMN),
            Expr::col(&last, TARGET_KIND_COLUMN),
        ),
        Direction::Incoming => (
            Expr::col(&last, RELATIONSHIP_KIND_COLUMN),
            Expr::col(&last, SOURCE_ID_COLUMN),
            Expr::col(&last, SOURCE_KIND_COLUMN),
            Expr::col("e1", TARGET_ID_COLUMN),
            Expr::col("e1", TARGET_KIND_COLUMN),
        ),
    };

    let path_nodes = Expr::func(
        "array",
        (1..=depth)
            .map(|i| {
                let e = format!("e{i}");
                Expr::func(
                    "tuple",
                    vec![Expr::col(&e, end_col), Expr::col(&e, end_type_col)],
                )
            })
            .collect(),
    );

    Query {
        select: vec![
            SelectExpr::col("e1", start_col),
            SelectExpr::col(&last, end_col),
            SelectExpr::new(rel_kind, RELATIONSHIP_KIND_COLUMN),
            SelectExpr::new(src_id, SOURCE_ID_COLUMN),
            SelectExpr::new(src_kind, SOURCE_KIND_COLUMN),
            SelectExpr::new(tgt_id, TARGET_ID_COLUMN),
            SelectExpr::new(tgt_kind, TARGET_KIND_COLUMN),
            SelectExpr::new(path_nodes, PATH_NODES_COLUMN),
            SelectExpr::new(Expr::int(depth as i64), DEPTH_COLUMN),
            SelectExpr::col("e1", DELETED_COLUMN),
            SelectExpr::col("e1", TRAVERSAL_PATH_COLUMN),
        ],
        from,
        where_clause,
        ..Default::default()
    }
}

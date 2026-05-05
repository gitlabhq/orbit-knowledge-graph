//! Shared emit helpers: dedup, columns, predicates, node hydration, edge predicates.

use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::super::plan::*;
use super::super::shared::{
    dedup_query, dedup_subquery, deleted_false, denorm_tag_expr, filter_to_expr, id_list_predicate,
    id_range_predicate, rel_kind_filter, rel_kind_filter_values,
};

// ─────────────────────────────────────────────────────────────────────────────
// Shared primitives: dedup, columns, predicates
// ─────────────────────────────────────────────────────────────────────────────

/// Build a dedup subquery with user filters pushed inside for prewhere.
///
/// Filters, node_ids, and id_range go INTO the scan so ClickHouse can
/// use them as prewhere predicates to skip granules. The caller applies
/// `_deleted=false` OUTSIDE (after LIMIT 1 BY) so a deleted latest
/// version correctly suppresses the entity.
pub(super) fn build_dedup_subquery(
    alias: &str,
    table: &str,
    select: Vec<SelectExpr>,
    np: &NodePlan,
) -> Query {
    let mut scan_where = Vec::new();
    for (prop, filter) in &np.filters {
        scan_where.push(filter_to_expr(alias, prop, filter));
    }
    if !np.node_ids.is_empty() {
        scan_where.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
    }
    if let Some(ref range) = np.id_range {
        scan_where.push(id_range_predicate(alias, range));
    }
    dedup_query(alias, table, select, scan_where, DEFAULT_PRIMARY_KEY)
}

/// Build SelectExpr list from the pre-computed dedup_columns on NodePlan.
pub(super) fn collect_dedup_columns(alias: &str, np: &NodePlan) -> Vec<SelectExpr> {
    np.dedup_columns
        .iter()
        .map(|col| SelectExpr::col(alias, col.as_str()))
        .collect()
}

/// WHERE predicates for a node: filters + _deleted=false.
/// Node columns for the outer SELECT, aliased as `{alias}_{col}` for the
/// graph formatter. Only for non-aggregation queries (aggregation builds
/// its own SELECT).
pub(super) fn node_select_columns(alias: &str, np: &NodePlan) -> Vec<SelectExpr> {
    if !np.emit_select {
        return vec![];
    }
    super::super::shared::requested_columns(&np.columns)
        .into_iter()
        .map(|col| SelectExpr::new(Expr::col(alias, &col), format!("{alias}_{col}")))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: node hydration
// ─────────────────────────────────────────────────────────────────────────────

pub(super) fn emit_node_join_with_narrowing(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    narrow_cte: Option<&str>,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    emit_node_join_inner(
        from,
        np,
        edge_alias,
        edge_col,
        use_traversal_path_join,
        narrow_cte,
    )
}

/// JOIN a node's dedup subquery into the FROM tree.
///
/// `use_traversal_path_join`: true for FK paths (node-to-node), false for
/// edge paths (edge.traversal_path has different semantics than node's).
pub(super) fn emit_node_join(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    emit_node_join_inner(
        from,
        np,
        edge_alias,
        edge_col,
        use_traversal_path_join,
        None,
    )
}

fn emit_node_join_inner(
    from: TableRef,
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    use_traversal_path_join: bool,
    narrow_cte: Option<&str>,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;

    let dedup_cols = collect_dedup_columns(alias, np);
    let mut dedup_query = build_dedup_subquery(alias, table, dedup_cols, np);

    // IN-narrowing: restrict the dedup scan to IDs from the narrow CTE.
    if let Some(cte_name) = narrow_cte {
        let in_pred = Expr::InSubquery {
            expr: Box::new(Expr::col(alias, DEFAULT_PRIMARY_KEY)),
            cte_name: cte_name.to_string(),
            column: DEFAULT_PRIMARY_KEY.to_string(),
        };
        dedup_query.where_clause = match dedup_query.where_clause {
            Some(existing) => Some(Expr::and(existing, in_pred)),
            None => Some(in_pred),
        };
    }

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

    let joined = TableRef::join(
        JoinType::Inner,
        from,
        TableRef::Subquery {
            query: Box::new(dedup_query),
            alias: alias.to_string(),
        },
        on,
    );

    let selects = node_select_columns(alias, np);
    // Only _deleted=false in the outer WHERE; user filters are already
    // inside the dedup scan.
    let wheres = vec![deleted_false(alias)];

    Ok((joined, selects, wheres))
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

    let (from, deleted) = dedup_subquery(
        alias,
        table,
        vec![
            SelectExpr::col(alias, DEFAULT_PRIMARY_KEY),
            SelectExpr::col(alias, DELETED_COLUMN),
        ],
        {
            let mut sw = Vec::new();
            for (prop, filter) in &np.filters {
                sw.push(filter_to_expr(alias, prop, filter));
            }
            if !np.node_ids.is_empty() {
                sw.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
            }
            if let Some(ref range) = np.id_range {
                sw.push(id_range_predicate(alias, range));
            }
            sw
        },
        DEFAULT_PRIMARY_KEY,
    );

    let inner = Query {
        select: vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)],
        from,
        where_clause: Some(deleted),
        ..Default::default()
    };

    ctes.push(Cte::new(&cte_name, inner));

    Ok(vec![Expr::InSubquery {
        expr: Box::new(Expr::col(edge_alias, edge_col)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    }])
}

// ─────────────────────────────────────────────────────────────────────────────
// Emit helpers: edge predicates
// ─────────────────────────────────────────────────────────────────────────────

pub(super) fn push_edge_predicates(
    where_parts: &mut Vec<Expr>,
    alias: &str,
    hop: &Hop,
    nodes: &HashMap<String, NodePlan>,
    start_col: &str,
    end_col: &str,
) {
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
    where_parts.push(deleted_false(alias));
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
        let selective = !np.filters.is_empty() || !np.node_ids.is_empty() || np.id_range.is_some();
        let should_narrow = match np.hydration {
            HydrationStrategy::FilterOnly => true,
            HydrationStrategy::Join => selective,
            HydrationStrategy::Skip => false,
        };
        if !should_narrow {
            continue;
        }
        let cte_name = format!("_filter_{node_alias}");
        // Join nodes need their CTE created here; FilterOnly gets theirs later.
        if np.hydration == HydrationStrategy::Join && narrowed.insert(node_alias.clone()) {
            let table = np.table.as_deref().unwrap_or("");
            let dedup = build_dedup_subquery(
                node_alias,
                table,
                vec![
                    SelectExpr::col(node_alias, DEFAULT_PRIMARY_KEY),
                    SelectExpr::col(node_alias, DELETED_COLUMN),
                ],
                np,
            );
            ctes.push(Cte::new(
                &cte_name,
                Query {
                    select: vec![SelectExpr::col(node_alias, DEFAULT_PRIMARY_KEY)],
                    from: TableRef::subquery(dedup, node_alias),
                    where_clause: Some(deleted_false(node_alias)),
                    ..Default::default()
                },
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

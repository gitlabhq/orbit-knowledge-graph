use std::collections::HashMap;
use std::collections::HashSet;

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use crate::passes::plan::*;
use crate::passes::shared::latest_row_dedup;
use crate::passes::shared::{
    deleted_false, denorm_tag_expr, filter_to_expr, id_list_predicate, id_range_predicate,
    rel_kind_filter, rel_kind_filter_values,
};
pub(super) use crate::passes::shared::{latest_node_predicates, node_select_columns};

fn sort_key_predicates(alias: &str, np: &NodePlan, sort_key: &[String]) -> Vec<Expr> {
    let in_sort_key = |column: &str| sort_key.iter().any(|key| key == column);
    let mut predicates: Vec<Expr> = np
        .filters
        .iter()
        .filter(|(prop, filter)| in_sort_key(prop) && filter.filter.rhs_column.is_none())
        .map(|(prop, filter)| filter_to_expr(alias, prop, filter))
        .collect();
    if in_sort_key(DEFAULT_PRIMARY_KEY) {
        if !np.node_ids.is_empty() {
            predicates.push(id_list_predicate(alias, DEFAULT_PRIMARY_KEY, &np.node_ids));
        }
        if let Some(ref range) = np.id_range {
            predicates.push(id_range_predicate(alias, range));
        }
    }
    predicates
}

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
    node_column: &str,
    narrow: Option<NarrowSource>,
    sort_key: &[String],
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let table = np
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", np.alias)))?;
    let alias = &np.alias;

    let in_predicate = narrow.map(|NarrowSource::Cte(cte_name)| Expr::InSubquery {
        expr: Box::new(Expr::col(alias, node_column)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    });

    let selects = node_select_columns(alias, np);
    let scan = match in_predicate {
        Some(predicate) => limit_by_scan(
            table,
            alias,
            vec![SelectExpr::star()],
            sort_key,
            std::iter::once(predicate)
                .chain(sort_key_predicates(alias, np, sort_key))
                .collect(),
        ),
        None => TableRef::scan_final(table, alias),
    };
    let node_scan = TableRef::subquery(
        Query {
            select: vec![SelectExpr::star()],
            from: scan,
            where_clause: Expr::conjoin(latest_node_predicates(alias, np)),
            ..Default::default()
        },
        alias,
    );

    let joined = TableRef::join(
        JoinType::Inner,
        from,
        node_scan,
        Expr::eq(
            Expr::col(alias, node_column),
            Expr::col(edge_alias, edge_col),
        ),
    );

    Ok((joined, selects, vec![]))
}

// Authoritative filter: dedup with FINAL before filtering so a stale matching version can't resurrect a row.
pub(super) fn emit_filter_subquery(
    np: &NodePlan,
    edge_alias: &str,
    edge_col: &str,
    node_column: &str,
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
            select: vec![SelectExpr::new(
                Expr::col(alias, node_column),
                DEFAULT_PRIMARY_KEY,
            )],
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

fn node_ids_dedup_scan(
    alias: &str,
    table: &str,
    np: &NodePlan,
    sort_key: &[String],
) -> Result<Query> {
    if sort_key.is_empty() {
        return Err(QueryError::Lowering(format!(
            "no sort key for node table '{table}'; cannot emit LIMIT BY dedup"
        )));
    }
    let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
    Ok(Query {
        select: vec![SelectExpr::col(alias, DEFAULT_PRIMARY_KEY)],
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(latest_node_predicates(alias, np)),
        order_by,
        limit_by,
        ..Default::default()
    })
}

pub(super) fn node_values_from_candidate_scan(
    alias: &str,
    table: &str,
    column: &str,
    np: &NodePlan,
    extra_predicates: Vec<Expr>,
) -> Query {
    let mut predicates = latest_node_predicates(alias, np);
    predicates.extend(extra_predicates);
    Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, column),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(predicates),
        ..Default::default()
    }
}

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

/// Build a `LIMIT 1 BY <sort_key> ORDER BY <sort_key>, _version DESC` subquery
/// over a plain (non-`FINAL`) scan, with WHERE predicates injected for PK
/// pruning. Reproduces `ReplacingMergeTree` latest-row semantics while keeping
/// column pruning and projections eligible. `select` is the projection (e.g.
/// `*` for edge aggregations, or the requested columns for hydration);
/// `sort_key` is the dedup identity (the table's full ORDER BY key).
pub(super) fn limit_by_scan(
    table: &str,
    alias: &str,
    select: Vec<SelectExpr>,
    sort_key: &[String],
    where_predicates: Vec<Expr>,
) -> TableRef {
    let (order_by, limit_by) = latest_row_dedup(alias, sort_key);
    let query = Query {
        select,
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(where_predicates),
        order_by,
        limit_by,
        ..Default::default()
    };
    TableRef::subquery(query, alias)
}

pub(super) fn emit_denorm_tags(
    where_parts: &mut Vec<Expr>,
    plan: &Plan,
    hop: &Hop,
    edge_alias: &str,
    start_col: &str,
    end_col: &str,
    tagged: &mut HashSet<(String, String)>,
) {
    for (node_alias, id_col) in [(&hop.from_node, start_col), (&hop.to_node, end_col)] {
        let Some(np) = plan.nodes.get(node_alias) else {
            continue;
        };
        let direction = if id_col == SOURCE_ID_COLUMN {
            query_data_model::DenormalizedDirection::Source
        } else {
            query_data_model::DenormalizedDirection::Target
        };
        for (prop, filter) in &np.filters {
            let tag_id = (node_alias.clone(), prop.clone());
            if tagged.contains(&tag_id) {
                continue;
            }
            let Some(property) = filter.property else {
                continue;
            };
            let key = query_data_model::DenormalizedKey {
                property,
                direction,
            };
            // Skip hops that don't write this tag; pushing it there matches an
            // empty edge and silently drops the row.
            if !hop_carries_denorm(plan, hop, &key) {
                continue;
            }
            if let Some(facts) = plan.denormalized.get(&key)
                && let Some(expr) = denorm_tag_expr(
                    edge_alias,
                    &facts.edge_column,
                    &facts.tag_key,
                    &filter.filter,
                )
            {
                where_parts.push(expr);
                tagged.insert(tag_id);
            }
        }
    }
}

fn hop_carries_denorm(plan: &Plan, hop: &Hop, key: &query_data_model::DenormalizedKey) -> bool {
    // A wildcard hop's relationship is unknown at runtime, so no tag is safe.
    if crate::passes::normalize::is_wildcard(&hop.rel_types) {
        return false;
    }
    plan.denormalized.get(key).is_some_and(|facts| {
        hop.relationships
            .iter()
            .any(|relationship| facts.relationships.contains(relationship))
    })
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
    table_sort_keys: &HashMap<String, Vec<String>>,
) -> Result<()> {
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
            HydrationStrategy::FilterOnly => false,
            HydrationStrategy::Join => selective,
            HydrationStrategy::Skip => false,
        };
        if !should_narrow {
            continue;
        }
        let cte_name = format!("_filter_{node_alias}");
        if np.hydration == HydrationStrategy::Join && narrowed.insert(node_alias.clone()) {
            let table = np.table.as_deref().unwrap_or("");
            let sort_key = table_sort_keys
                .get(table)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            ctes.push(Cte::new(
                &cte_name,
                node_ids_dedup_scan(node_alias, table, np, sort_key)?,
            ));
        }
        where_parts.push(Expr::InSubquery {
            expr: Box::new(Expr::col(edge_alias, id_col)),
            cte_name,
            column: "id".to_string(),
        });
    }
    Ok(())
}

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

    let union = TableRef::union_all(queries, alias).with_relationship(hop.input_index);

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

#[allow(clippy::too_many_arguments)]
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

    let (rel_kind, src_id, src_kind, src_tags, tgt_id, tgt_kind, tgt_tags) = match direction {
        Direction::Outgoing | Direction::Both => (
            Expr::col("e1", RELATIONSHIP_KIND_COLUMN),
            Expr::col("e1", SOURCE_ID_COLUMN),
            Expr::col("e1", SOURCE_KIND_COLUMN),
            Expr::col("e1", SOURCE_TAGS_COLUMN),
            Expr::col(&last, TARGET_ID_COLUMN),
            Expr::col(&last, TARGET_KIND_COLUMN),
            Expr::col(&last, TARGET_TAGS_COLUMN),
        ),
        Direction::Incoming => (
            Expr::col(&last, RELATIONSHIP_KIND_COLUMN),
            Expr::col(&last, SOURCE_ID_COLUMN),
            Expr::col(&last, SOURCE_KIND_COLUMN),
            Expr::col(&last, SOURCE_TAGS_COLUMN),
            Expr::col("e1", TARGET_ID_COLUMN),
            Expr::col("e1", TARGET_KIND_COLUMN),
            Expr::col("e1", TARGET_TAGS_COLUMN),
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
            SelectExpr::new(src_tags, SOURCE_TAGS_COLUMN),
            SelectExpr::new(tgt_id, TARGET_ID_COLUMN),
            SelectExpr::new(tgt_kind, TARGET_KIND_COLUMN),
            SelectExpr::new(tgt_tags, TARGET_TAGS_COLUMN),
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

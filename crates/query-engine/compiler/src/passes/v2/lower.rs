//! Skeleton-first query lowerer (v2).
//!
//! Every query is lowered as:
//!   1. Edge chain (0..N hops) resolving the structural pattern → ID tuples
//!   2. Node table JOINs only for columns the user selected/grouped by
//!
//! No CTEs in the common case. Edges drive, nodes are lazy lookups.

use std::collections::{HashMap, HashSet};

use ontology::constants::*;

use crate::ast::*;
use crate::constants::*;
use crate::error::{QueryError, Result};
use crate::input::*;

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

pub fn lower_v2(input: &mut Input) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal if input.is_search() => lower_single_node(input),
        QueryType::Traversal => lower_skeleton(input),
        QueryType::Aggregation => lower_skeleton(input),
        _ => super::super::lower::lower(input),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Single-node (no edges): degenerate skeleton
// ─────────────────────────────────────────────────────────────────────────────

fn lower_single_node(input: &mut Input) -> Result<Node> {
    let node = input
        .nodes
        .first()
        .ok_or_else(|| QueryError::Lowering("no nodes in query".into()))?;
    let table = node
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.id)))?;
    let alias = &node.id;

    let mut select = vec![SelectExpr::new(Expr::col(alias, "id"), "id")];
    for col in requested_columns(node) {
        if col != "id" {
            select.push(SelectExpr::new(Expr::col(alias, &col), col.clone()));
        }
    }

    let from = TableRef::scan(table, alias);
    let mut where_parts = Vec::new();

    for (prop, filter) in &node.filters {
        where_parts.push(filter_to_expr(alias, prop, filter));
    }
    if !node.node_ids.is_empty() {
        where_parts.push(node_ids_predicate(alias, &node.node_ids));
    }
    if let Some(ref range) = node.id_range {
        where_parts.push(id_range_predicate(alias, range));
    }

    let q = Query {
        select,
        from,
        where_clause: Expr::conjoin(where_parts),
        limit: Some(input.limit),
        ..Default::default()
    };

    Ok(Node::Query(Box::new(q)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Skeleton-first lowering (traversal + aggregation with edges)
// ─────────────────────────────────────────────────────────────────────────────

fn lower_skeleton(input: &mut Input) -> Result<Node> {
    if input.relationships.is_empty() {
        return lower_single_node(input);
    }

    let (from, edge_aliases, mut where_parts) = build_edge_chain(input)?;
    // Edge metadata columns only for traversal — aggregation doesn't need them.
    let mut select = Vec::new();
    if input.query_type != QueryType::Aggregation {
        for ea in &edge_aliases {
            select.extend(edge_select_columns(ea));
        }
    }

    register_node_edge_mappings(input, &edge_aliases)?;
    inject_denorm_tags(&mut where_parts, input, &edge_aliases);
    inject_node_constraints_on_edges(&mut where_parts, input, &edge_aliases);

    let mut ctes = Vec::new();
    let (from, node_selects, node_where_parts) =
        hydrate_nodes(from, input, &edge_aliases, &mut ctes)?;
    select.extend(node_selects);
    where_parts.extend(node_where_parts);

    let (select, group_by, order_by) = if input.query_type == QueryType::Aggregation {
        build_aggregation(select, input)?
    } else {
        let order_by = build_order_by(input);
        (select, vec![], order_by)
    };

    let q = Query {
        ctes,
        select,
        from,
        where_clause: Expr::conjoin(where_parts),
        group_by,
        order_by,
        limit: Some(input.limit),
        ..Default::default()
    };

    Ok(Node::Query(Box::new(q)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 1: Edge chain
// ─────────────────────────────────────────────────────────────────────────────

fn build_edge_chain(input: &Input) -> Result<(TableRef, Vec<String>, Vec<Expr>)> {
    let mut where_parts = Vec::new();
    let mut edge_aliases = Vec::new();

    let first_rel = &input.relationships[0];
    let edge_table = resolve_edge_table(input, &first_rel.types);
    let alias = "e0".to_string();

    let mut from = TableRef::scan(&edge_table, &alias);
    edge_aliases.push(alias.clone());

    let (start_col, end_col) = first_rel.direction.edge_columns();
    push_edge_predicates(
        &mut where_parts,
        input,
        &alias,
        first_rel,
        start_col,
        end_col,
    );

    for (i, rel) in input.relationships.iter().enumerate().skip(1) {
        let prev_alias = &edge_aliases[i - 1];
        let edge_table = resolve_edge_table(input, &rel.types);
        let alias = format!("e{i}");

        let prev_end = input.relationships[i - 1].direction.edge_columns().1;
        let (curr_start, curr_end) = rel.direction.edge_columns();

        let right = TableRef::scan(&edge_table, &alias);
        let join_on = Expr::eq(
            Expr::col(prev_alias, prev_end),
            Expr::col(&alias, curr_start),
        );

        from = TableRef::Join {
            join_type: JoinType::Inner,
            left: Box::new(from),
            right: Box::new(right),
            on: join_on,
        };

        push_edge_predicates(&mut where_parts, input, &alias, rel, curr_start, curr_end);
        edge_aliases.push(alias);
    }

    Ok((from, edge_aliases, where_parts))
}

fn push_edge_predicates(
    where_parts: &mut Vec<Expr>,
    input: &Input,
    alias: &str,
    rel: &InputRelationship,
    start_col: &str,
    end_col: &str,
) {
    if let Some(kind_filter) = rel_kind_filter(alias, &rel.types) {
        where_parts.push(kind_filter);
    }

    if let Some(from_node) = input.nodes.iter().find(|n| n.id == rel.from)
        && let Some(ref entity) = from_node.entity
    {
        let kind_col = if start_col == SOURCE_ID_COLUMN {
            SOURCE_KIND_COLUMN
        } else {
            TARGET_KIND_COLUMN
        };
        where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
    }

    if let Some(to_node) = input.nodes.iter().find(|n| n.id == rel.to)
        && let Some(ref entity) = to_node.entity
    {
        let kind_col = if end_col == TARGET_ID_COLUMN {
            TARGET_KIND_COLUMN
        } else {
            SOURCE_KIND_COLUMN
        };
        where_parts.push(Expr::eq(Expr::col(alias, kind_col), Expr::string(entity)));
    }

    where_parts.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 2: Edge SELECT columns
// ─────────────────────────────────────────────────────────────────────────────

fn edge_select_columns(alias: &str) -> Vec<SelectExpr> {
    [
        (RELATIONSHIP_KIND_COLUMN, EDGE_TYPE_SUFFIX),
        (SOURCE_ID_COLUMN, EDGE_SRC_SUFFIX),
        (SOURCE_KIND_COLUMN, EDGE_SRC_TYPE_SUFFIX),
        (TARGET_ID_COLUMN, EDGE_DST_SUFFIX),
        (TARGET_KIND_COLUMN, EDGE_DST_TYPE_SUFFIX),
    ]
    .iter()
    .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{alias}_{suffix}")))
    .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: Node-to-edge mappings (for enforce)
// ─────────────────────────────────────────────────────────────────────────────

fn register_node_edge_mappings(input: &mut Input, edge_aliases: &[String]) -> Result<()> {
    for (i, rel) in input.relationships.iter().enumerate() {
        let alias = &edge_aliases[i];
        let (start_col, end_col) = rel.direction.edge_columns();
        input
            .compiler
            .node_edge_col
            .entry(rel.from.clone())
            .or_insert_with(|| (alias.clone(), start_col.to_string()));
        input
            .compiler
            .node_edge_col
            .entry(rel.to.clone())
            .or_insert_with(|| (alias.clone(), end_col.to_string()));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 4: Denormalized tag predicates
// ─────────────────────────────────────────────────────────────────────────────

fn inject_denorm_tags(where_parts: &mut Vec<Expr>, input: &Input, edge_aliases: &[String]) {
    if input.compiler.denormalized_columns.is_empty() {
        return;
    }
    for (i, rel) in input.relationships.iter().enumerate() {
        let alias = &edge_aliases[i];
        let (start_col, end_col) = rel.direction.edge_columns();

        if let Some(n) = input.nodes.iter().find(|n| n.id == rel.from) {
            let dir = if start_col == SOURCE_ID_COLUMN {
                "source"
            } else {
                "target"
            };
            where_parts.extend(denorm_tag_exprs(
                n,
                dir,
                alias,
                &input.compiler.denormalized_columns,
            ));
        }
        if let Some(n) = input.nodes.iter().find(|n| n.id == rel.to) {
            let dir = if end_col == TARGET_ID_COLUMN {
                "target"
            } else {
                "source"
            };
            where_parts.extend(denorm_tag_exprs(
                n,
                dir,
                alias,
                &input.compiler.denormalized_columns,
            ));
        }
    }
}

fn denorm_tag_exprs(
    node: &InputNode,
    dir_prefix: &str,
    edge_alias: &str,
    denorm_map: &HashMap<(String, String, String), (String, String)>,
) -> Vec<Expr> {
    let entity = match &node.entity {
        Some(e) => e,
        None => return vec![],
    };
    let mut exprs = Vec::new();
    for (prop, filter) in &node.filters {
        let key = (entity.clone(), prop.clone(), dir_prefix.to_string());
        let Some((edge_column, tag_key)) = denorm_map.get(&key) else {
            continue;
        };
        match filter.op {
            None | Some(FilterOp::Eq) => {
                let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                exprs.push(Expr::func(
                    "has",
                    vec![
                        Expr::col(edge_alias, edge_column),
                        Expr::string(format!("{tag_key}:{val}")),
                    ],
                ));
            }
            Some(FilterOp::In) => {
                if let Some(values) = filter.value.as_ref().and_then(|v| v.as_array()) {
                    let tags: Vec<String> = values
                        .iter()
                        .filter_map(|v| v.as_str().map(|s| format!("{tag_key}:{s}")))
                        .collect();
                    if tags.len() == 1 {
                        exprs.push(Expr::func(
                            "has",
                            vec![Expr::col(edge_alias, edge_column), Expr::string(&tags[0])],
                        ));
                    } else if !tags.is_empty() {
                        exprs.push(Expr::func(
                            "hasAny",
                            vec![
                                Expr::col(edge_alias, edge_column),
                                Expr::func("array", tags.iter().map(Expr::string).collect()),
                            ],
                        ));
                    }
                }
            }
            _ => {}
        }
    }
    exprs
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5: Node constraints on edges
// ─────────────────────────────────────────────────────────────────────────────

fn inject_node_constraints_on_edges(
    where_parts: &mut Vec<Expr>,
    input: &Input,
    edge_aliases: &[String],
) {
    for (i, rel) in input.relationships.iter().enumerate() {
        let alias = &edge_aliases[i];
        let (start_col, end_col) = rel.direction.edge_columns();

        if let Some(n) = input.nodes.iter().find(|n| n.id == rel.from)
            && !n.node_ids.is_empty()
        {
            where_parts.push(ids_on_edge(alias, start_col, &n.node_ids));
        }
        if let Some(n) = input.nodes.iter().find(|n| n.id == rel.to)
            && !n.node_ids.is_empty()
        {
            where_parts.push(ids_on_edge(alias, end_col, &n.node_ids));
        }
    }
}

fn ids_on_edge(edge_alias: &str, edge_col: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(edge_alias, edge_col), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            edge_alias,
            edge_col,
            ChType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 6: Node hydration
// ─────────────────────────────────────────────────────────────────────────────

fn hydrate_nodes(
    mut from: TableRef,
    input: &Input,
    edge_aliases: &[String],
    ctes: &mut Vec<Cte>,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let mut selects = Vec::new();
    let mut where_parts = Vec::new();
    let mut hydrated: HashSet<String> = HashSet::new();

    for (i, rel) in input.relationships.iter().enumerate() {
        let alias = &edge_aliases[i];
        let (start_col, end_col) = rel.direction.edge_columns();

        for (node_alias, edge_col) in [(&rel.from, start_col), (&rel.to, end_col)] {
            if hydrated.contains(node_alias) {
                continue;
            }
            if let Some(node) = input.nodes.iter().find(|n| n.id == *node_alias) {
                match hydration_strategy(node, input) {
                    HydrationStrategy::Join => {
                        let (new_from, ns, nw) =
                            join_node_table(from, node, alias, edge_col, input)?;
                        from = new_from;
                        selects.extend(ns);
                        where_parts.extend(nw);
                    }
                    HydrationStrategy::Subquery => {
                        where_parts.extend(filter_subquery(node, alias, edge_col, ctes)?);
                    }
                    HydrationStrategy::Skip => {}
                }
            }
            hydrated.insert(node_alias.clone());
        }
    }

    Ok((from, selects, where_parts))
}

/// Build a CTE + InSubquery for a node's non-denormalized filters.
/// The CTE is single-ref (only used in the WHERE), so ClickHouse
/// inlining is a non-issue — it's evaluated exactly once.
fn filter_subquery(
    node: &InputNode,
    edge_alias: &str,
    edge_col: &str,
    ctes: &mut Vec<Cte>,
) -> Result<Vec<Expr>> {
    let table = node
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.id)))?;
    let alias = &node.id;
    let cte_name = format!("_filter_{}", node.id);

    let mut inner_where = Vec::new();
    for (prop, filter) in &node.filters {
        inner_where.push(filter_to_expr(alias, prop, filter));
    }
    inner_where.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    let inner = Query {
        select: vec![SelectExpr::new(
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            DEFAULT_PRIMARY_KEY,
        )],
        from: TableRef::scan(table, alias),
        where_clause: Expr::conjoin(inner_where),
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    ctes.push(Cte::new(&cte_name, inner));

    Ok(vec![Expr::InSubquery {
        expr: Box::new(Expr::col(edge_alias, edge_col)),
        cte_name,
        column: DEFAULT_PRIMARY_KEY.to_string(),
    }])
}

/// Decide hydration strategy for each node:
/// - `Join`: inline JOIN for columns needed in GROUP BY, ORDER BY, or agg property
/// - `Subquery`: WHERE IN subquery for non-denormalized filters (no columns in SELECT)
/// - `Skip`: no hydration needed (all filters are denormalized, no columns needed)
enum HydrationStrategy {
    Join,
    Subquery,
    Skip,
}

fn hydration_strategy(node: &InputNode, input: &Input) -> HydrationStrategy {
    let is_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(&node.id));

    let is_agg_property_target = input.aggregations.iter().any(|a| {
        a.target.as_deref() == Some(&node.id)
            && a.property.is_some()
            && !matches!(a.function, AggFunction::Count)
    });

    let is_order_by_target = input.order_by.as_ref().is_some_and(|ob| ob.node == node.id);

    // Needs columns in SELECT → full JOIN.
    // Note: agg_property_target WITHOUT group_by gets a Join too, but
    // join_node_table only emits the aggregate property column, not all
    // default columns.
    if is_group_by || is_agg_property_target || is_order_by_target {
        return HydrationStrategy::Join;
    }

    let has_non_denorm_filters = node.filters.iter().any(|(prop, _)| {
        let entity = node.entity.as_deref().unwrap_or("");
        let k1 = (entity.to_string(), prop.clone(), "source".to_string());
        let k2 = (entity.to_string(), prop.clone(), "target".to_string());
        !input.compiler.denormalized_columns.contains_key(&k1)
            && !input.compiler.denormalized_columns.contains_key(&k2)
    });

    // Has filters that can't go on the edge → WHERE IN subquery.
    if has_non_denorm_filters {
        return HydrationStrategy::Subquery;
    }

    HydrationStrategy::Skip
}

fn join_node_table(
    from: TableRef,
    node: &InputNode,
    edge_alias: &str,
    edge_col: &str,
    input: &Input,
) -> Result<(TableRef, Vec<SelectExpr>, Vec<Expr>)> {
    let table = node
        .table
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no table", node.id)))?;
    let alias = &node.id;

    let inner_scan = TableRef::scan(table, alias);
    let dedup_query = Query {
        select: vec![SelectExpr::star()],
        from: inner_scan,
        order_by: vec![OrderExpr {
            expr: Expr::col(alias, VERSION_COLUMN),
            desc: true,
        }],
        limit_by: Some((1, vec![Expr::col(alias, DEFAULT_PRIMARY_KEY)])),
        ..Default::default()
    };

    let node_subquery = TableRef::Subquery {
        query: Box::new(dedup_query),
        alias: alias.to_string(),
    };

    let join_on = Expr::eq(
        Expr::col(alias, DEFAULT_PRIMARY_KEY),
        Expr::col(edge_alias, edge_col),
    );

    let joined = TableRef::Join {
        join_type: JoinType::Inner,
        left: Box::new(from),
        right: Box::new(node_subquery),
        on: join_on,
    };

    // Only emit columns that are needed for GROUP BY or aggregation.
    // Agg-property-target nodes only need their property columns.
    // Group-by nodes need their default columns. The hydration pipeline
    // handles full column sets for the response.
    let is_group_by = input
        .aggregations
        .iter()
        .any(|a| a.group_by.as_deref() == Some(alias));

    let needed_cols: Vec<String> = if is_group_by || input.query_type != QueryType::Aggregation {
        requested_columns(node)
    } else {
        // Agg-property-target: don't emit raw SELECT columns. The property
        // is only referenced inside aggregate functions (sum, avg, etc.),
        // not as standalone SELECT expressions. Emitting it raw would
        // violate GROUP BY constraints.
        // Order-by targets need their sort column.
        let mut cols = Vec::new();
        if let Some(ref ob) = input.order_by {
            if ob.node == *alias && !cols.contains(&ob.property) {
                cols.push(ob.property.clone());
            }
        }
        cols
    };

    let mut selects = Vec::new();
    for col in &needed_cols {
        selects.push(SelectExpr::new(
            Expr::col(alias, col),
            format!("{alias}_{col}"),
        ));
    }

    let mut wheres = Vec::new();
    for (prop, filter) in &node.filters {
        wheres.push(filter_to_expr(alias, prop, filter));
    }
    wheres.push(Expr::eq(
        Expr::col(alias, DELETED_COLUMN),
        Expr::param(ChType::Bool, false),
    ));

    Ok((joined, selects, wheres))
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 7: Aggregation
// ─────────────────────────────────────────────────────────────────────────────

fn build_aggregation(
    mut select: Vec<SelectExpr>,
    input: &Input,
) -> Result<(Vec<SelectExpr>, Vec<Expr>, Vec<OrderExpr>)> {
    let mut group_by = Vec::new();

    for agg in &input.aggregations {
        let agg_expr = match agg.function {
            AggFunction::Count => Expr::func("count", vec![]),
            AggFunction::Sum | AggFunction::Avg | AggFunction::Min | AggFunction::Max => {
                let target = agg.target.as_deref().unwrap_or("*");
                let prop = agg.property.as_deref().unwrap_or("id");
                let fname = match agg.function {
                    AggFunction::Sum => "sum",
                    AggFunction::Avg => "avg",
                    AggFunction::Min => "min",
                    AggFunction::Max => "max",
                    _ => unreachable!(),
                };
                Expr::func(fname, vec![Expr::col(target, prop)])
            }
            AggFunction::Collect => {
                let target = agg.target.as_deref().unwrap_or("*");
                let prop = agg.property.as_deref().unwrap_or("id");
                Expr::func("groupArray", vec![Expr::col(target, prop)])
            }
        };

        let alias = agg.alias.as_deref().unwrap_or("agg_result");
        select.push(SelectExpr::new(agg_expr, alias));

        if let Some(ref gb) = agg.group_by
            && let Some(gb_node) = input.nodes.iter().find(|n| n.id == *gb)
        {
            for col in requested_columns(gb_node) {
                let expr = Expr::col(gb, &col);
                if !group_by.contains(&expr) {
                    group_by.push(expr);
                }
            }
        }
    }

    let mut order_by = Vec::new();
    if let Some(ref agg_sort) = input.aggregation_sort
        && let Some(agg) = input.aggregations.get(agg_sort.agg_index)
    {
        let alias = agg.alias.as_deref().unwrap_or("agg_result");
        order_by.push(OrderExpr {
            expr: Expr::ident(alias),
            desc: matches!(agg_sort.direction, OrderDirection::Desc),
        });
    }

    Ok((select, group_by, order_by))
}

fn build_order_by(input: &Input) -> Vec<OrderExpr> {
    input
        .order_by
        .as_ref()
        .map(|ob| {
            vec![OrderExpr {
                expr: Expr::col(&ob.node, &ob.property),
                desc: matches!(ob.direction, OrderDirection::Desc),
            }]
        })
        .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn resolve_edge_table(input: &Input, rel_types: &[String]) -> String {
    for t in rel_types {
        if let Some(table) = input.compiler.edge_table_for_rel.get(t) {
            return table.clone();
        }
    }
    input.compiler.default_edge_table.clone()
}

fn rel_kind_filter(alias: &str, types: &[String]) -> Option<Expr> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        return None;
    }
    if types.len() == 1 {
        Some(Expr::eq(
            Expr::col(alias, RELATIONSHIP_KIND_COLUMN),
            Expr::string(&types[0]),
        ))
    } else {
        Expr::col_in(
            alias,
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    }
}

fn filter_to_expr(alias: &str, prop: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(alias, prop);
    let val = || filter.value.clone().unwrap_or(serde_json::Value::Null);
    let str_val = || filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    let typed = |v: serde_json::Value| -> Expr {
        Expr::param(data_type_to_ch(filter.data_type.as_ref()), v)
    };

    match filter.op {
        None | Some(FilterOp::Eq) => Expr::eq(col, typed(val())),
        Some(FilterOp::Gt) => Expr::binary(Op::Gt, col, typed(val())),
        Some(FilterOp::Gte) => Expr::binary(Op::Ge, col, typed(val())),
        Some(FilterOp::Lt) => Expr::binary(Op::Lt, col, typed(val())),
        Some(FilterOp::Lte) => Expr::binary(Op::Le, col, typed(val())),
        Some(FilterOp::In) => {
            if let Some(arr) = filter.value.as_ref().and_then(|v| v.as_array()) {
                Expr::col_in(
                    alias,
                    prop,
                    data_type_to_ch(filter.data_type.as_ref()),
                    arr.clone(),
                )
                .unwrap_or_else(|| Expr::param(ChType::Bool, false))
            } else {
                Expr::param(ChType::Bool, false)
            }
        }
        Some(FilterOp::Contains) => Expr::func(
            "positionCaseInsensitive",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::StartsWith) => Expr::func(
            "startsWith",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::EndsWith) => Expr::func(
            "endsWith",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::IsNull) => Expr::unary(Op::IsNull, col),
        Some(FilterOp::IsNotNull) => Expr::unary(Op::IsNotNull, col),
        Some(FilterOp::TokenMatch) => Expr::func(
            "hasToken",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::AllTokens) => Expr::func(
            "hasAllTokens",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
        Some(FilterOp::AnyTokens) => Expr::func(
            "hasAnyTokens",
            vec![col, Expr::param(ChType::String, str_val())],
        ),
    }
}

fn data_type_to_ch(dt: Option<&ontology::DataType>) -> ChType {
    match dt {
        Some(ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid) => {
            ChType::String
        }
        Some(ontology::DataType::Int) => ChType::Int64,
        Some(ontology::DataType::Float) => ChType::Float64,
        Some(ontology::DataType::Bool) => ChType::Bool,
        Some(ontology::DataType::DateTime | ontology::DataType::Date) => ChType::DateTime64,
        None => ChType::String,
    }
}

fn requested_columns(node: &InputNode) -> Vec<String> {
    match &node.columns {
        Some(ColumnSelection::List(cols)) => cols.clone(),
        Some(ColumnSelection::All) => vec!["*".to_string()],
        None => vec![],
    }
}

fn node_ids_predicate(alias: &str, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::col(alias, DEFAULT_PRIMARY_KEY), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            alias,
            DEFAULT_PRIMARY_KEY,
            ChType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(ChType::Bool, false))
    }
}

fn id_range_predicate(alias: &str, range: &InputIdRange) -> Expr {
    Expr::and(
        Expr::binary(
            Op::Ge,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::int(range.start),
        ),
        Expr::binary(
            Op::Le,
            Expr::col(alias, DEFAULT_PRIMARY_KEY),
            Expr::int(range.end),
        ),
    )
}

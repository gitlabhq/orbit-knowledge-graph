//! Lower: Input → AST
//!
//! Transforms validated input into a SQL-oriented AST.

use crate::ast::{ChType, Cte, Expr, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};

use crate::constants::{
    ANCHOR_ID_COLUMN, BACKWARD_ALIAS, BACKWARD_CTE, DEPTH_COLUMN, EDGE_ALIAS_SUFFIXES,
    END_ID_COLUMN, END_KIND_COLUMN, FORWARD_ALIAS, FORWARD_CTE, FRONTIER_EDGE_KINDS_COLUMN,
    PATH_NODES_COLUMN, PATHS_ALIAS, START_ID_COLUMN, edge_kinds_column, neighbor_id_column,
    neighbor_is_outgoing_column, neighbor_type_column, node_filter_cte, path_column,
    primary_key_column, redaction_id_column, redaction_type_column, relationship_type_column,
};
use crate::error::{QueryError, Result};
use crate::input::{
    ColumnSelection, Direction, FilterOp, Input, InputAggregation, InputFilter, InputNode,
    InputRelationship, OrderDirection, QueryType,
};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, EDGE_RESERVED_COLUMNS, EDGE_TABLE, RELATIONSHIP_KIND_COLUMN,
    SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN,
    TRAVERSAL_PATH_COLUMN,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Generate SELECT expressions for edge columns with the given table alias.
/// Skips `traversal_path` — it is only used by the security pass (injected
/// into WHERE, not SELECT) and is absent from the local DuckDB schema.
fn edge_select_exprs(alias: &str) -> Vec<SelectExpr> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .zip(EDGE_ALIAS_SUFFIXES.iter())
        .filter(|(col, _)| **col != TRAVERSAL_PATH_COLUMN)
        .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{alias}_{suffix}")))
        .collect()
}

fn edge_depth_select_expr(alias: &str) -> SelectExpr {
    SelectExpr::new(
        Expr::col(alias, DEPTH_COLUMN),
        format!("{alias}_{DEPTH_COLUMN}"),
    )
}

fn edge_path_nodes_select_expr(alias: &str) -> SelectExpr {
    SelectExpr::new(
        Expr::col(alias, PATH_NODES_COLUMN),
        format!("{alias}_{PATH_NODES_COLUMN}"),
    )
}

/// ClickHouse query cache TTL in seconds. Applied via SET when the query
/// includes a cursor, so that subsequent pages of the same query benefit
/// from CH-level caching of the raw SQL result.
/// Lower validated input into an AST node.
///
/// Writes metadata to `input.compiler` for downstream passes.
pub fn lower(input: &mut Input) -> Result<Node> {
    let node = match input.query_type {
        QueryType::Search => lower_search(input),
        QueryType::Traversal => lower_traversal_edge_only(input),
        QueryType::Aggregation => lower_aggregation(input),
        QueryType::PathFinding => lower_path_finding(input),
        QueryType::Neighbors => lower_neighbors(input),

        QueryType::Hydration => lower_hydration(input),
    }?;

    Ok(node)
}

// ─────────────────────────────────────────────────────────────────────────────
// Search
// ─────────────────────────────────────────────────────────────────────────────

fn lower_search(input: &Input) -> Result<Node> {
    let node = input
        .nodes
        .first()
        .ok_or_else(|| QueryError::Lowering("search requires a node".into()))?;
    let table = resolve_table(node)?;
    let from = TableRef::scan(&table, &node.id);

    let mut conds: Vec<Expr> = Vec::new();
    conds.extend(id_filter(&node.id, DEFAULT_PRIMARY_KEY, &node.node_ids));
    if let Some(r) = &node.id_range {
        conds.push(Expr::binary(
            Op::Ge,
            Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
            Expr::int(r.start),
        ));
        conds.push(Expr::binary(
            Op::Le,
            Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
            Expr::int(r.end),
        ));
    }
    for (prop, filter) in &node.filters {
        conds.push(filter_expr(&node.id, prop, filter));
    }
    let where_clause = Expr::and_all(conds.into_iter().map(Some));

    let mut select = Vec::new();
    if let Some(ColumnSelection::List(cols)) = &node.columns {
        for col in cols {
            select.push(SelectExpr::new(
                Expr::col(&node.id, col),
                format!("{}_{col}", node.id),
            ));
        }
    }

    let order_by = input.order_by.as_ref().map_or(vec![], |ob| {
        vec![OrderExpr {
            expr: Expr::col(&ob.node, &ob.property),
            desc: ob.direction == OrderDirection::Desc,
        }]
    });
    let limit = Some(input.limit);

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        order_by,
        limit,
        ..Default::default()
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal
// ─────────────────────────────────────────────────────────────────────────────

/// Edge-only traversal: edges are the FROM tables, node tables are
/// referenced only via IN subqueries for filtering. Node properties are
/// deferred to the hydration pipeline.
///
/// Single-hop: flat edge scan.
/// Multi-hop: UNION ALL of edge self-joins.
/// Multi-rel: secondary edges JOINed on shared columns.
fn lower_traversal_edge_only(input: &mut Input) -> Result<Node> {
    let first_rel = input.relationships.first().unwrap();
    let (start_col, end_col) = first_rel.direction.edge_columns();

    let mut select = Vec::new();
    let mut where_parts: Vec<Expr> = Vec::new();
    let mut ctes = Vec::new();
    let mut node_edge_col: HashMap<String, (String, String)> = HashMap::new();

    // Build driving edge: flat scan for single-hop, UNION ALL for multi-hop
    let mut from;
    let edge_alias;
    if first_rel.max_hops > 1 {
        edge_alias = "hop_e0";
        let union = build_hop_union_all(first_rel, edge_alias);
        let (from_col, to_col) = first_rel.direction.union_columns();
        from = union;
        select.extend(edge_select_exprs(edge_alias));
        select.push(edge_depth_select_expr(edge_alias));
        select.push(edge_path_nodes_select_expr(edge_alias));
        node_edge_col.insert(
            first_rel.from.clone(),
            (edge_alias.to_string(), from_col.to_string()),
        );
        node_edge_col.insert(
            first_rel.to.clone(),
            (edge_alias.to_string(), to_col.to_string()),
        );
    } else {
        edge_alias = "e0";
        let (edge, edge_type_cond) = edge_scan(edge_alias, &type_filter(&first_rel.types));
        from = edge;
        select.extend(edge_select_exprs(edge_alias));
        if let Some(tc) = edge_type_cond {
            where_parts.push(tc);
        }
        node_edge_col.insert(
            first_rel.from.clone(),
            (edge_alias.to_string(), start_col.to_string()),
        );
        node_edge_col.insert(
            first_rel.to.clone(),
            (edge_alias.to_string(), end_col.to_string()),
        );
    }

    // JOIN secondary relationships on shared columns
    for (i, rel) in input.relationships.iter().enumerate().skip(1) {
        let (shared_node, shared_alias, shared_col) =
            if let Some((a, c)) = node_edge_col.get(&rel.from) {
                (&rel.from, a.clone(), c.clone())
            } else if let Some((a, c)) = node_edge_col.get(&rel.to) {
                (&rel.to, a.clone(), c.clone())
            } else {
                continue;
            };

        if rel.max_hops > 1 {
            let alias = format!("hop_e{i}");
            let (from_col, to_col) = rel.direction.union_columns();
            let sec_shared_col = if shared_node == &rel.from {
                from_col
            } else {
                to_col
            };

            let join_cond = Expr::eq(
                Expr::col(&shared_alias, &shared_col),
                Expr::col(&alias, sec_shared_col),
            );
            let union = build_hop_union_all(rel, &alias);
            from = TableRef::join(JoinType::Inner, from, union, join_cond);
            select.extend(edge_select_exprs(&alias));
            select.push(edge_depth_select_expr(&alias));
            select.push(edge_path_nodes_select_expr(&alias));

            let other = if shared_node == &rel.from {
                &rel.to
            } else {
                &rel.from
            };
            let other_col = if other == &rel.from { from_col } else { to_col };
            node_edge_col
                .entry(other.clone())
                .or_insert((alias, other_col.to_string()));
        } else {
            let alias = format!("e{i}");
            let (sec_start, sec_end) = rel.direction.edge_columns();
            let sec_shared_col = if shared_node == &rel.from {
                sec_start
            } else {
                sec_end
            };

            let mut join_cond = Expr::eq(
                Expr::col(&shared_alias, &shared_col),
                Expr::col(&alias, sec_shared_col),
            );
            if let Some(tf) = Expr::col_in(
                &alias,
                RELATIONSHIP_KIND_COLUMN,
                ChType::String,
                rel.types.iter().map(|t| Value::String(t.clone())).collect(),
            ) {
                join_cond = Expr::and(join_cond, tf);
            }

            let (sec_scan, _) = edge_scan(&alias, &None);
            from = TableRef::join(JoinType::Inner, from, sec_scan, join_cond);
            select.extend(edge_select_exprs(&alias));

            let other = if shared_node == &rel.from {
                &rel.to
            } else {
                &rel.from
            };
            let other_col = if other == &rel.from {
                sec_start
            } else {
                sec_end
            };
            node_edge_col
                .entry(other.clone())
                .or_insert((alias, other_col.to_string()));
        }
    }

    // Surface edge-to-node mapping for enforce to emit _gkg_* columns.
    input.compiler.node_edge_col = node_edge_col;
    let node_edge_col = &input.compiler.node_edge_col;

    // Add IN subquery for each node that has conditions
    for node in &input.nodes {
        let has_conditions = !node.node_ids.is_empty() || !node.filters.is_empty();
        if !has_conditions {
            continue;
        }
        if let Some((alias, edge_col)) = node_edge_col.get(&node.id) {
            let table = resolve_table(node)?;
            let node_where = build_node_where(node);
            let cte_name = node_filter_cte(&node.id);
            let cte_query = Query {
                select: vec![SelectExpr::new(
                    Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                    DEFAULT_PRIMARY_KEY,
                )],
                from: TableRef::scan(&table, &node.id),
                where_clause: node_where,
                ..Default::default()
            };
            ctes.push(Cte::new(&cte_name, cte_query));
            where_parts.push(Expr::InSubquery {
                expr: Box::new(Expr::col(alias, edge_col.as_str())),
                cte_name,
                column: DEFAULT_PRIMARY_KEY.into(),
            });
        }
    }

    // When order_by references a node property other than "id", JOIN
    // that node's table so the column is accessible. For "id" we can use
    // the edge column directly (source_id / target_id).
    if let Some(ob) = &input.order_by
        && ob.property != DEFAULT_PRIMARY_KEY
    {
        let ob_node = input
            .nodes
            .iter()
            .find(|n| n.id == ob.node)
            .ok_or_else(|| {
                QueryError::Lowering(format!(
                    "order_by node '{}' not found in input nodes",
                    ob.node
                ))
            })?;
        let (edge_a, edge_c) = node_edge_col.get(&ob.node).ok_or_else(|| {
            QueryError::Lowering(format!(
                "order_by node '{}' not connected to driving edge",
                ob.node
            ))
        })?;
        let table = resolve_table(ob_node)?;
        let join_cond = Expr::eq(
            Expr::col(edge_a, edge_c.as_str()),
            Expr::col(&ob_node.id, DEFAULT_PRIMARY_KEY),
        );
        from = TableRef::join(
            JoinType::Inner,
            from,
            TableRef::scan(&table, &ob_node.id),
            join_cond,
        );
    }

    let where_clause = Expr::and_all(where_parts.into_iter().map(Some));
    let order_by = input.order_by.as_ref().map_or(vec![], |ob| {
        let expr = match (ob.property.as_str(), node_edge_col.get(&ob.node)) {
            (DEFAULT_PRIMARY_KEY, Some((alias, edge_col))) => Expr::col(alias, edge_col.as_str()),
            _ => Expr::col(&ob.node, &ob.property),
        };
        vec![OrderExpr {
            expr,
            desc: ob.direction == OrderDirection::Desc,
        }]
    });
    let limit = Some(input.limit);

    Ok(Node::Query(Box::new(Query {
        ctes,
        select,
        from,
        where_clause,
        order_by,
        limit,
        ..Default::default()
    })))
}

/// Build WHERE clause for a single node's filters and node_ids.
fn build_node_where(node: &InputNode) -> Option<Expr> {
    let mut parts: Vec<Expr> = Vec::new();
    if !node.node_ids.is_empty()
        && let Some(filter) = Expr::col_in(
            &node.id,
            DEFAULT_PRIMARY_KEY,
            ChType::Int64,
            node.node_ids
                .iter()
                .map(|&id| serde_json::Value::Number(id.into()))
                .collect(),
        )
    {
        parts.push(filter);
    }
    for (col, filter) in &node.filters {
        parts.push(filter_expr(&node.id, col, filter));
    }
    Expr::and_all(parts.into_iter().map(Some))
}

fn lower_aggregation(input: &mut Input) -> Result<Node> {
    let group_by_ids: HashSet<String> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.clone())
        .collect();

    // Edge-only targets are only possible for single-hop, single-rel
    // aggregations with relationships. No-rel (single-node) and multi-hop
    // fall back to the standard join approach.
    let has_multi_hop = input.relationships.iter().any(|r| r.max_hops > 1);
    let can_edge_only = !input.relationships.is_empty() && !has_multi_hop;

    let mut edge_only_targets: HashSet<String> = HashSet::new();
    if can_edge_only {
        for node in &input.nodes {
            if group_by_ids.contains(&node.id) {
                continue;
            }
            let all_property_less = input
                .aggregations
                .iter()
                .filter(|a| a.target.as_deref() == Some(&node.id))
                .all(|a| a.property.is_none());
            if all_property_less {
                edge_only_targets.insert(node.id.clone());
            }
        }
    }

    // Build the FROM tree.
    let (from, edge_aliases) = if input.relationships.is_empty() {
        // Single-node aggregation — no edges, just a node scan.
        let node = input
            .nodes
            .first()
            .ok_or_else(|| QueryError::Lowering("aggregation requires at least one node".into()))?;
        let table = resolve_table(node)?;
        (TableRef::scan(&table, &node.id), HashMap::new())
    } else {
        build_joins(&input.nodes, &input.relationships, &edge_only_targets)?
    };

    // Build WHERE from non-edge-only nodes + edge filters.
    let where_clause = build_where(
        &input.nodes,
        &input.relationships,
        &edge_aliases,
        &edge_only_targets,
    );

    // Build node_edge_col for edge-only targets and _nf_* CTEs.
    let mut node_edge_col: HashMap<String, (String, String)> = HashMap::new();
    let mut ctes = Vec::new();
    let mut where_parts: Vec<Expr> = where_clause.into_iter().collect();

    // Build _nf_* CTEs for non-group-by nodes with conditions. Edge-only
    // targets also get their node_edge_col mapping populated here.
    if input.relationships.len() == 1 {
        let rel = &input.relationships[0];
        let (start_col, end_col) = rel.direction.edge_columns();
        let edge_alias = edge_aliases
            .get(&0)
            .cloned()
            .unwrap_or_else(|| "e0".to_string());

        for node in &input.nodes {
            if group_by_ids.contains(&node.id) {
                continue;
            }
            let edge_col = if node.id == rel.from {
                start_col
            } else {
                end_col
            };

            if edge_only_targets.contains(&node.id) {
                node_edge_col.insert(node.id.clone(), (edge_alias.clone(), edge_col.to_string()));
            }

            let has_conditions = !node.node_ids.is_empty() || !node.filters.is_empty();
            if !has_conditions {
                continue;
            }
            let table = resolve_table(node)?;
            let node_where = build_node_where(node);
            let cte_name = node_filter_cte(&node.id);
            let cte_query = Query {
                select: vec![SelectExpr::new(
                    Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                    DEFAULT_PRIMARY_KEY,
                )],
                from: TableRef::scan(&table, &node.id),
                where_clause: node_where,
                ..Default::default()
            };
            ctes.push(Cte::new(&cte_name, cte_query));
            where_parts.push(Expr::InSubquery {
                expr: Box::new(Expr::col(&edge_alias, edge_col)),
                cte_name,
                column: DEFAULT_PRIMARY_KEY.into(),
            });
        }
    }

    input.compiler.node_edge_col = node_edge_col;

    let mut select = Vec::new();
    let mut group_by_exprs = Vec::new();

    for node in &input.nodes {
        if !group_by_ids.contains(&node.id) {
            continue;
        }
        if let Some(ColumnSelection::List(cols)) = &node.columns {
            for col in cols {
                let expr = Expr::col(&node.id, col);
                select.push(SelectExpr::new(expr.clone(), format!("{}_{col}", node.id)));
                group_by_exprs.push(expr);
            }
        }
    }

    for agg in &input.aggregations {
        let expr = agg_expr_with_edge_col(agg, &input.compiler.node_edge_col);
        select.push(SelectExpr::new(
            expr,
            agg.alias
                .clone()
                .unwrap_or_else(|| agg.function.as_sql().to_lowercase()),
        ));
    }

    let order_by = input
        .aggregation_sort
        .as_ref()
        .filter(|s| s.agg_index < input.aggregations.len())
        .map_or(vec![], |s| {
            let agg = &input.aggregations[s.agg_index];
            vec![OrderExpr {
                expr: agg_expr_with_edge_col(agg, &input.compiler.node_edge_col),
                desc: s.direction == OrderDirection::Desc,
            }]
        });

    let limit = Some(input.limit);

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause: Expr::conjoin(where_parts),
        group_by: group_by_exprs,
        order_by,
        limit,
        ctes,
        ..Default::default()
    })))
}

/// Build the aggregate expression. Uses edge columns for targets in
/// `node_edge_col` (edge-only), node table columns otherwise.
fn agg_expr_with_edge_col(
    agg: &InputAggregation,
    node_edge_col: &HashMap<String, (String, String)>,
) -> Expr {
    let arg = match (&agg.property, &agg.target) {
        (Some(prop), Some(target)) => {
            // Property aggregate — always references the node table.
            Expr::col(target, prop)
        }
        (None, Some(target)) => {
            // Property-less: use edge column if target is edge-only.
            if let Some((alias, col)) = node_edge_col.get(target.as_str()) {
                Expr::col(alias, col.as_str())
            } else {
                Expr::col(target, DEFAULT_PRIMARY_KEY)
            }
        }
        _ => Expr::int(1),
    };
    Expr::func(agg.function.as_sql(), vec![arg])
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding (bidirectional UNION ALL — no recursive CTE)
// ─────────────────────────────────────────────────────────────────────────────
//
// Generates a bidirectional frontier expansion:
//   - Forward CTE: expand from start node, depth 1..ceil(max_depth/2)
//   - Backward CTE: expand from end node, depth 1..floor(max_depth/2)
//   - Intersection: forward JOIN backward on meeting point
//   - Direct: forward arms that reach end directly (depth 1 only)
//
// Each frontier arm is a fixed chain of gl_edge JOINs (no recursion).
// ClickHouse can use primary key indexes on every edge scan because
// each join has concrete column equalities — unlike recursive CTEs where
// the working table is opaque to the optimizer (ClickHouse #75026).

fn lower_path_finding(input: &Input) -> Result<Node> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?;

    let start = find_node(&input.nodes, &path.from)?;
    let end = find_node(&input.nodes, &path.to)?;

    let start_entity = start
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("start node has no entity".into()))?;
    let end_entity = end
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("end node has no entity".into()))?;

    let rel_type_filter = type_filter(&path.rel_types);
    let max_depth = path.max_depth;
    let forward_depth = max_depth.div_ceil(2); // ceil(max_depth / 2)
    let backward_depth = max_depth / 2; // floor(max_depth / 2)

    let forward_cte = Cte::new(
        FORWARD_CTE,
        build_frontier(&start.node_ids, forward_depth, &rel_type_filter, true),
    );
    let backward_cte = if backward_depth > 0 {
        Some(Cte::new(
            BACKWARD_CTE,
            build_frontier(&end.node_ids, backward_depth, &rel_type_filter, false),
        ))
    } else {
        None
    };

    // Helper: build a start-node tuple from the forward frontier's anchor_id.
    let start_tuple = |table: &str| {
        Expr::func(
            "tuple",
            vec![
                Expr::col(table, ANCHOR_ID_COLUMN),
                Expr::string(start_entity),
            ],
        )
    };
    let end_tuple = |table: &str| {
        Expr::func(
            "tuple",
            vec![Expr::col(table, ANCHOR_ID_COLUMN), Expr::string(end_entity)],
        )
    };

    // Direct depth-1 paths: forward frontier reaching end directly.
    let direct_query = Query {
        select: vec![
            SelectExpr::new(Expr::col(FORWARD_ALIAS, DEPTH_COLUMN), DEPTH_COLUMN),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::func("array", vec![start_tuple(FORWARD_ALIAS)]),
                        Expr::col(FORWARD_ALIAS, PATH_NODES_COLUMN),
                    ],
                ),
                path_column(),
            ),
            SelectExpr::new(
                Expr::col(FORWARD_ALIAS, FRONTIER_EDGE_KINDS_COLUMN),
                edge_kinds_column(),
            ),
        ],
        from: TableRef::scan(FORWARD_CTE, FORWARD_ALIAS),
        where_clause: Expr::and_all([
            Some(Expr::binary(
                Op::Eq,
                Expr::col(FORWARD_ALIAS, DEPTH_COLUMN),
                Expr::int(1),
            )),
            Expr::col_in(
                FORWARD_ALIAS,
                END_ID_COLUMN,
                ChType::Int64,
                end.node_ids.iter().map(|id| Value::from(*id)).collect(),
            ),
        ]),
        ..Default::default()
    };

    // Intersection paths: forward meets backward at a common node.
    let intersection_query = Query {
        select: vec![
            SelectExpr::new(
                Expr::binary(
                    Op::Add,
                    Expr::col(FORWARD_ALIAS, DEPTH_COLUMN),
                    Expr::col(BACKWARD_ALIAS, DEPTH_COLUMN),
                ),
                DEPTH_COLUMN,
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::func("array", vec![start_tuple(FORWARD_ALIAS)]),
                        Expr::col(FORWARD_ALIAS, PATH_NODES_COLUMN),
                        Expr::func(
                            "arrayReverse",
                            vec![Expr::col(BACKWARD_ALIAS, PATH_NODES_COLUMN)],
                        ),
                        Expr::func("array", vec![end_tuple(BACKWARD_ALIAS)]),
                    ],
                ),
                path_column(),
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col(FORWARD_ALIAS, FRONTIER_EDGE_KINDS_COLUMN),
                        Expr::func(
                            "arrayReverse",
                            vec![Expr::col(BACKWARD_ALIAS, FRONTIER_EDGE_KINDS_COLUMN)],
                        ),
                    ],
                ),
                edge_kinds_column(),
            ),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan(FORWARD_CTE, FORWARD_ALIAS),
            TableRef::scan(BACKWARD_CTE, BACKWARD_ALIAS),
            Expr::eq(
                Expr::col(FORWARD_ALIAS, END_ID_COLUMN),
                Expr::col(BACKWARD_ALIAS, END_ID_COLUMN),
            ),
        ),
        where_clause: Some(Expr::binary(
            Op::Le,
            Expr::binary(
                Op::Add,
                Expr::col(FORWARD_ALIAS, DEPTH_COLUMN),
                Expr::col(BACKWARD_ALIAS, DEPTH_COLUMN),
            ),
            Expr::int(max_depth as i64),
        )),
        ..Default::default()
    };

    // Combine direct + intersection as a UNION ALL subquery.
    let paths_union = if backward_depth == 0 {
        TableRef::subquery(direct_query, PATHS_ALIAS)
    } else {
        TableRef::union_all(vec![direct_query, intersection_query], PATHS_ALIAS)
    };

    let limit = Some(input.limit);

    // Outer query: select from the paths UNION ALL, ordered by depth.
    // Security filters are applied by the security pass to every gl_edge scan
    // inside the forward/backward CTEs. No separate start/end table join
    // is needed: the edge anchors already filter by start/end node IDs.
    Ok(Node::Query(Box::new(Query {
        ctes: {
            let mut ctes = vec![];
            ctes.push(forward_cte);
            if let Some(bc) = backward_cte {
                ctes.push(bc);
            }
            ctes
        },
        select: vec![
            SelectExpr::new(Expr::col(PATHS_ALIAS, path_column()), path_column()),
            SelectExpr::new(
                Expr::col(PATHS_ALIAS, edge_kinds_column()),
                edge_kinds_column(),
            ),
            SelectExpr::new(Expr::col(PATHS_ALIAS, DEPTH_COLUMN), DEPTH_COLUMN),
        ],
        from: paths_union,
        order_by: vec![OrderExpr {
            expr: Expr::col(PATHS_ALIAS, DEPTH_COLUMN),
            desc: false,
        }],
        limit,
        ..Default::default()
    })))
}

/// Build a frontier CTE body: UNION ALL of hop arms for depths 1..max_depth.
///
/// `is_forward=true`:  anchors on source_id, traverses source→target
/// `is_forward=false`: anchors on target_id, traverses target→source
fn build_frontier(
    anchor_ids: &[i64],
    max_depth: u32,
    rel_type_filter: &Option<Vec<String>>,
    is_forward: bool,
) -> Query {
    let arms: Vec<Query> = (1..=max_depth)
        .map(|depth| build_frontier_arm(anchor_ids, depth, rel_type_filter, is_forward))
        .collect();

    // Wrap in a UNION ALL. For a single arm just return it directly.
    if arms.len() == 1 {
        arms.into_iter().next().unwrap()
    } else {
        let mut first = arms.into_iter();
        let base = first.next().unwrap();
        Query {
            union_all: first.collect(),
            ..base
        }
    }
}

/// Build one arm of a frontier: a chain of `depth` edge joins.
///
/// Forward arm (depth=2, anchor=start):
///   SELECT e2.target_id AS end_id, ...
///   FROM gl_edge e1 JOIN gl_edge e2 ON e1.target_id = e2.source_id
///   WHERE e1.source_id IN (start_ids)
fn build_frontier_arm(
    anchor_ids: &[i64],
    depth: u32,
    rel_type_filter: &Option<Vec<String>>,
    is_forward: bool,
) -> Query {
    // Column naming: forward traverses source→target, backward target→source.
    let (anchor_col, next_col, next_kind_col) = if is_forward {
        (SOURCE_ID_COLUMN, TARGET_ID_COLUMN, TARGET_KIND_COLUMN)
    } else {
        (TARGET_ID_COLUMN, SOURCE_ID_COLUMN, SOURCE_KIND_COLUMN)
    };

    let last = format!("e{depth}");

    // Build join chain: e1 JOIN e2 ON e1.next = e2.anchor JOIN e3 ...
    let (mut from, first_type_cond) = edge_scan("e1", rel_type_filter);
    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let (edge_table, edge_type_cond) = edge_scan(&curr, rel_type_filter);
        let mut join_cond = Expr::eq(Expr::col(&prev, next_col), Expr::col(&curr, anchor_col));
        if let Some(tc) = edge_type_cond {
            join_cond = Expr::and(join_cond, tc);
        }
        from = TableRef::join(JoinType::Inner, from, edge_table, join_cond);
    }

    // path_nodes: array of (id, kind) tuples for each hop's exit node.
    //
    // For backward arms, exclude the last hop (the meeting point) because
    // it's already the last element in forward's path_nodes. Only include
    // intermediate nodes between the end anchor and the meeting point.
    let path_node_range = if is_forward {
        1..=depth
    } else {
        1..=depth.saturating_sub(1)
    };
    let path_node_tuples: Vec<Expr> = path_node_range
        .map(|i| {
            let alias = format!("e{i}");
            Expr::func(
                "tuple",
                vec![
                    Expr::col(&alias, next_col),
                    Expr::col(&alias, next_kind_col),
                ],
            )
        })
        .collect();
    let path_nodes = if path_node_tuples.is_empty() {
        // Backward depth 1: no intermediates. Use typed empty array.
        Expr::func(
            "arrayResize",
            vec![
                Expr::func(
                    "array",
                    vec![Expr::func("tuple", vec![Expr::int(0), Expr::string("")])],
                ),
                Expr::int(0),
            ],
        )
    } else {
        Expr::func("array", path_node_tuples)
    };

    // edge_kinds: array of relationship types for each hop.
    let edge_kinds = Expr::func(
        "array",
        (1..=depth)
            .map(|i| Expr::col(format!("e{i}"), RELATIONSHIP_KIND_COLUMN))
            .collect(),
    );

    // Anchor condition: first edge connects to the anchor node(s).
    let anchor_cond = Expr::col_in(
        "e1",
        anchor_col,
        ChType::Int64,
        anchor_ids.iter().map(|id| Value::from(*id)).collect(),
    );

    Query {
        select: vec![
            SelectExpr::new(Expr::col("e1", anchor_col), ANCHOR_ID_COLUMN),
            SelectExpr::new(Expr::col(&last, next_col), END_ID_COLUMN),
            SelectExpr::new(Expr::col(&last, next_kind_col), END_KIND_COLUMN),
            SelectExpr::new(path_nodes, PATH_NODES_COLUMN),
            SelectExpr::new(edge_kinds, FRONTIER_EDGE_KINDS_COLUMN),
            SelectExpr::new(Expr::int(depth as i64), DEPTH_COLUMN),
        ],
        from,
        where_clause: Expr::and_all([anchor_cond, first_type_cond]),
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

fn lower_neighbors(input: &mut Input) -> Result<Node> {
    let neighbors_config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = find_node(&input.nodes, &neighbors_config.node)?;
    let center_table = resolve_table(center_node)?;
    let center_entity = center_node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node entity missing".into()))?
        .clone();

    let rel_type_filter = type_filter(&neighbors_config.rel_types);
    let edge_alias = "e";
    let center_id = center_node.id.clone();
    let center_uses_default_pk = center_node.redaction_id_column == DEFAULT_PRIMARY_KEY;
    let center_redaction_col = center_node.redaction_id_column.clone();
    let order_by = input.order_by.as_ref().map_or(vec![], |ob| {
        vec![OrderExpr {
            expr: Expr::col(&ob.node, &ob.property),
            desc: ob.direction == OrderDirection::Desc,
        }]
    });
    let limit = Some(input.limit);

    // Build _nf CTE for center node filtering (IDs + filters).
    // Dedup pass will wrap this CTE's scan for soft-delete correctness.
    let has_conditions = !center_node.node_ids.is_empty() || !center_node.filters.is_empty();
    let cte_name = node_filter_cte(&center_id);
    let ctes = if has_conditions {
        let node_where = build_node_where(center_node);
        vec![Cte::new(
            &cte_name,
            Query {
                select: vec![SelectExpr::new(
                    Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                    DEFAULT_PRIMARY_KEY,
                )],
                from: TableRef::scan(&center_table, &center_id),
                where_clause: node_where,
                ..Default::default()
            },
        )]
    } else {
        vec![]
    };

    // Edge-only: scan gl_edge directly, filter by center node IDs via IN subquery.
    let build_arm = |dir: Direction| -> Query {
        let (edge_table, edge_type_cond) = edge_scan(edge_alias, &rel_type_filter);
        let (center_edge_col, center_kind_col, neighbor_id, neighbor_type, is_outgoing) = match dir
        {
            Direction::Outgoing => (
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                1i64,
            ),
            Direction::Incoming => (
                TARGET_ID_COLUMN,
                TARGET_KIND_COLUMN,
                SOURCE_ID_COLUMN,
                SOURCE_KIND_COLUMN,
                0i64,
            ),
            Direction::Both => unreachable!(),
        };

        let mut where_parts: Vec<Expr> = Vec::new();

        // Entity kind filter on the edge's center side.
        where_parts.push(Expr::eq(
            Expr::col(edge_alias, center_kind_col),
            Expr::string(center_entity.as_str()),
        ));

        // Center node IN filter: either via CTE or literal IDs on the edge column.
        if has_conditions {
            where_parts.push(Expr::InSubquery {
                expr: Box::new(Expr::col(edge_alias, center_edge_col)),
                cte_name: cte_name.clone(),
                column: DEFAULT_PRIMARY_KEY.into(),
            });
        }

        if let Some(tc) = edge_type_cond {
            where_parts.push(tc);
        }

        let mut select = vec![
            SelectExpr::new(Expr::col(edge_alias, neighbor_id), neighbor_id_column()),
            SelectExpr::new(Expr::col(edge_alias, neighbor_type), neighbor_type_column()),
            SelectExpr::new(
                Expr::col(edge_alias, RELATIONSHIP_KIND_COLUMN),
                relationship_type_column(),
            ),
            SelectExpr::new(Expr::int(is_outgoing), neighbor_is_outgoing_column()),
        ];

        let mut from = edge_table;

        if center_uses_default_pk {
            select.push(SelectExpr::new(
                Expr::col(edge_alias, center_edge_col),
                redaction_id_column(&center_id),
            ));
        } else {
            // Indirect auth: JOIN center node table to read the auth column.
            from = TableRef::join(
                JoinType::Inner,
                from,
                TableRef::scan(&center_table, &center_id),
                Expr::eq(
                    Expr::col(edge_alias, center_edge_col),
                    Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                ),
            );
            select.push(SelectExpr::new(
                Expr::col(&center_id, &center_redaction_col),
                redaction_id_column(&center_id),
            ));
            select.push(SelectExpr::new(
                Expr::col(&center_id, DEFAULT_PRIMARY_KEY),
                primary_key_column(&center_id),
            ));
        }
        select.push(SelectExpr::new(
            Expr::string(center_entity.as_str()),
            redaction_type_column(&center_id),
        ));

        Query {
            select,
            from,
            where_clause: Expr::conjoin(where_parts),
            ..Default::default()
        }
    };

    if neighbors_config.direction == Direction::Both {
        let mut outgoing = build_arm(Direction::Outgoing);
        outgoing.union_all = vec![build_arm(Direction::Incoming)];
        outgoing.order_by = order_by;
        outgoing.limit = limit;
        outgoing.ctes = ctes;
        return Ok(Node::Query(Box::new(outgoing)));
    }

    let mut arm = build_arm(neighbors_config.direction);
    arm.order_by = order_by;
    arm.limit = limit;
    arm.ctes = ctes;
    Ok(Node::Query(Box::new(arm)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Hydration
// ─────────────────────────────────────────────────────────────────────────────

fn lower_hydration(input: &Input) -> Result<Node> {
    if input.nodes.is_empty() {
        return Err(QueryError::Lowering(
            "hydration requires at least one node".into(),
        ));
    }

    let first_node = &input.nodes[0];
    let mut first_query = build_hydration_arm(first_node)?;

    for node in &input.nodes[1..] {
        first_query.union_all.push(build_hydration_arm(node)?);
    }

    first_query.limit = Some(input.limit);

    Ok(Node::Query(Box::new(first_query)))
}

fn build_hydration_arm(node: &InputNode) -> Result<Query> {
    let table = node
        .table
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("hydration node has no table".into()))?;
    let entity = node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("hydration node has no entity".into()))?;
    let alias = &node.id;
    let pk = &node.id_property;

    let columns: Vec<&str> = match &node.columns {
        Some(ColumnSelection::List(cols)) => cols.iter().map(|s| s.as_str()).collect(),
        _ => vec![],
    };

    let prop_columns: Vec<&str> = columns.iter().filter(|&&c| c != pk).copied().collect();

    let json_expr = if prop_columns.is_empty() {
        Expr::string("{}")
    } else {
        let map_args: Vec<Expr> = prop_columns
            .iter()
            .flat_map(|&col| {
                [
                    Expr::string(col),
                    Expr::func("toString", vec![Expr::col(alias, col)]),
                ]
            })
            .collect();
        Expr::func("toJSONString", vec![Expr::func("map", map_args)])
    };

    let select = vec![
        SelectExpr::new(Expr::col(alias, pk), format!("{alias}_{pk}")),
        SelectExpr::new(Expr::string(entity), format!("{alias}_entity_type")),
        SelectExpr::new(json_expr, format!("{alias}_props")),
    ];

    let where_clause = id_filter(alias, pk, &node.node_ids);

    Ok(Query {
        select,
        from: TableRef::scan(table, alias),
        where_clause,
        ..Default::default()
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-hop Union Building
// ─────────────────────────────────────────────────────────────────────────────

/// Build a UNION ALL subquery for multi-hop traversal (1 to max_hops).
fn build_hop_union_all(rel: &InputRelationship, alias: &str) -> TableRef {
    let rel_type_filter = type_filter(&rel.types);
    let start = rel.min_hops.max(1);
    let queries = (start..=rel.max_hops)
        .map(|depth| build_hop_arm(depth, &rel_type_filter, rel.direction))
        .collect();
    TableRef::union_all(queries, alias)
}

/// Build one arm of the union: a chain of edge joins for a specific depth.
fn build_hop_arm(depth: u32, type_filter: &Option<Vec<String>>, direction: Direction) -> Query {
    let (start_col, end_col) = direction.edge_columns();
    let end_type_col = match direction {
        Direction::Outgoing | Direction::Both => TARGET_KIND_COLUMN,
        Direction::Incoming => SOURCE_KIND_COLUMN,
    };
    let last = format!("e{depth}");

    // Build chain: e1 -> e2 -> e3 -> ...
    let (mut from, first_type_cond) = edge_scan("e1", type_filter);

    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        // No traversal_path condition between consecutive edges: cross-namespace
        // relationships (e.g. RELATED_TO, CLOSES) can link entities in different
        // namespaces, so consecutive edges may have different paths.
        let (edge_table, edge_type_cond) = edge_scan(&curr, type_filter);
        let mut join_cond = Expr::eq(Expr::col(&prev, end_col), Expr::col(&curr, start_col));
        if let Some(tc) = edge_type_cond {
            join_cond = Expr::and(join_cond, tc);
        }
        from = TableRef::join(JoinType::Inner, from, edge_table, join_cond);
    }

    let (
        relationship_kind_expr,
        source_id_expr,
        source_kind_expr,
        target_id_expr,
        target_kind_expr,
    ) = match direction {
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

    Query {
        select: vec![
            SelectExpr::new(Expr::col("e1", start_col), START_ID_COLUMN),
            SelectExpr::new(Expr::col(&last, end_col), END_ID_COLUMN),
            SelectExpr::new(
                Expr::func(
                    "array",
                    (1..=depth)
                        .map(|index| {
                            let edge = format!("e{index}");
                            Expr::func(
                                "tuple",
                                vec![Expr::col(&edge, end_col), Expr::col(&edge, end_type_col)],
                            )
                        })
                        .collect(),
                ),
                PATH_NODES_COLUMN,
            ),
            SelectExpr::new(relationship_kind_expr, RELATIONSHIP_KIND_COLUMN),
            SelectExpr::new(source_id_expr, SOURCE_ID_COLUMN),
            SelectExpr::new(source_kind_expr, SOURCE_KIND_COLUMN),
            SelectExpr::new(target_id_expr, TARGET_ID_COLUMN),
            SelectExpr::new(target_kind_expr, TARGET_KIND_COLUMN),
            SelectExpr::new(Expr::int(depth as i64), DEPTH_COLUMN),
        ],
        from,
        where_clause: first_type_cond,
        ..Default::default()
    }
}

/// Returns `(table_ref, type_condition)` for an edge table scan.
/// The type condition should be folded into the JOIN ON or WHERE clause.
fn edge_scan(alias: &str, type_filter: &Option<Vec<String>>) -> (TableRef, Option<Expr>) {
    let table = TableRef::scan(EDGE_TABLE, alias);
    let cond = type_filter.as_ref().and_then(|types| {
        Expr::col_in(
            alias,
            RELATIONSHIP_KIND_COLUMN,
            ChType::String,
            types.iter().map(|t| Value::String(t.clone())).collect(),
        )
    });
    (table, cond)
}

fn type_filter(types: &[String]) -> Option<Vec<String>> {
    if types.is_empty() || (types.len() == 1 && types[0] == "*") {
        None
    } else {
        Some(types.to_vec())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Join Building
// ─────────────────────────────────────────────────────────────────────────────

/// Build a FROM tree that JOINs node tables and edge tables.
/// Nodes in `skip_nodes` are omitted from the tree — they are handled
/// edge-only via `node_edge_col` + `_nf_*` CTEs instead.
fn build_joins(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    skip_nodes: &HashSet<String>,
) -> Result<(TableRef, HashMap<usize, String>)> {
    // Find the first non-skipped node to start the FROM tree.
    let first_rel = rels
        .first()
        .ok_or_else(|| QueryError::Lowering("no relationships".into()))?;

    let start_node_id = if !skip_nodes.contains(&first_rel.from) {
        &first_rel.from
    } else if !skip_nodes.contains(&first_rel.to) {
        &first_rel.to
    } else {
        // Both nodes skipped — start from edge.
        let alias = "e0".to_string();
        let (edge, _) = edge_scan(&alias, &type_filter(&first_rel.types));
        let mut edge_aliases = HashMap::new();
        edge_aliases.insert(0, alias);
        return Ok((edge, edge_aliases));
    };

    let start = find_node(nodes, start_node_id)?;
    let start_table = resolve_table(start)?;
    let mut result = TableRef::scan(&start_table, &start.id);
    let mut edge_aliases = HashMap::new();
    let mut joined = HashSet::new();
    joined.insert(start.id.clone());

    for (i, rel) in rels.iter().enumerate() {
        // Multi-hop: use UNION ALL pattern with hop_e{i} alias.
        if rel.max_hops > 1 {
            let alias = format!("hop_e{i}");
            edge_aliases.insert(i, alias.clone());

            let union = build_hop_union_all(rel, &alias);
            let (from_col, to_col) = rel.direction.union_columns();

            let source_cond = Expr::eq(
                Expr::col(&rel.from, DEFAULT_PRIMARY_KEY),
                Expr::col(&alias, from_col),
            );
            let target_cond = Expr::eq(
                Expr::col(&alias, to_col),
                Expr::col(&rel.to, DEFAULT_PRIMARY_KEY),
            );

            let source_joined = joined.contains(&rel.from) || skip_nodes.contains(&rel.from);
            let target_joined = joined.contains(&rel.to) || skip_nodes.contains(&rel.to);

            let union_join_cond = match (source_joined, target_joined) {
                (true, true) => {
                    let mut conds = Vec::new();
                    if joined.contains(&rel.from) {
                        conds.push(source_cond.clone());
                    }
                    if joined.contains(&rel.to) {
                        conds.push(target_cond.clone());
                    }
                    Expr::and_all(conds.into_iter().map(Some))
                        .unwrap_or_else(|| source_cond.clone())
                }
                (true, false) => source_cond.clone(),
                (false, true) => target_cond.clone(),
                (false, false) => {
                    return Err(QueryError::Lowering(format!(
                        "disconnected relationship: neither '{}' nor '{}' are reachable",
                        rel.from, rel.to
                    )));
                }
            };

            result = TableRef::join(JoinType::Inner, result, union, union_join_cond);

            if !joined.contains(&rel.from) && !skip_nodes.contains(&rel.from) {
                let from_node = find_node(nodes, &rel.from)?;
                let source_table = resolve_table(from_node)?;
                result = TableRef::join(
                    JoinType::Inner,
                    result,
                    TableRef::scan(&source_table, &rel.from),
                    source_cond,
                );
                joined.insert(rel.from.clone());
            }
            if !joined.contains(&rel.to) && !skip_nodes.contains(&rel.to) {
                let target = find_node(nodes, &rel.to)?;
                let target_table = resolve_table(target)?;
                result = TableRef::join(
                    JoinType::Inner,
                    result,
                    TableRef::scan(&target_table, &rel.to),
                    target_cond,
                );
                joined.insert(rel.to.clone());
            }
            continue;
        }

        let alias = format!("e{i}");
        edge_aliases.insert(i, alias.clone());

        let (edge, edge_type_cond) = edge_scan(&alias, &type_filter(&rel.types));
        let source_cond = source_join_cond(&rel.from, &alias, rel.direction);
        let target_cond = target_join_cond(&alias, &rel.to, rel.direction);

        let source_joined = joined.contains(&rel.from) || skip_nodes.contains(&rel.from);
        let target_joined = joined.contains(&rel.to) || skip_nodes.contains(&rel.to);

        let mut edge_join_cond = match (source_joined, target_joined) {
            (true, true) => {
                // Only include join conds for non-skipped nodes.
                let mut conds = Vec::new();
                if joined.contains(&rel.from) {
                    conds.push(source_cond.clone());
                }
                if joined.contains(&rel.to) {
                    conds.push(target_cond.clone());
                }
                Expr::and_all(conds.into_iter().map(Some)).unwrap_or_else(|| source_cond.clone())
            }
            (true, false) => {
                if joined.contains(&rel.from) {
                    source_cond.clone()
                } else {
                    // source is skipped, we need the edge but no join to it
                    target_cond.clone()
                }
            }
            (false, true) => {
                if joined.contains(&rel.to) {
                    target_cond.clone()
                } else {
                    source_cond.clone()
                }
            }
            (false, false) => {
                return Err(QueryError::Lowering(format!(
                    "disconnected relationship: neither '{}' nor '{}' are reachable",
                    rel.from, rel.to
                )));
            }
        };
        if let Some(tc) = edge_type_cond {
            edge_join_cond = Expr::and(edge_join_cond, tc);
        }

        result = TableRef::join(JoinType::Inner, result, edge, edge_join_cond);

        // Join non-skipped, non-yet-joined nodes.
        if !joined.contains(&rel.from) && !skip_nodes.contains(&rel.from) {
            let from_node = find_node(nodes, &rel.from)?;
            let source_table = resolve_table(from_node)?;
            result = TableRef::join(
                JoinType::Inner,
                result,
                TableRef::scan(&source_table, &rel.from),
                source_cond,
            );
            joined.insert(rel.from.clone());
        }
        if !joined.contains(&rel.to) && !skip_nodes.contains(&rel.to) {
            let target = find_node(nodes, &rel.to)?;
            let target_table = resolve_table(target)?;
            result = TableRef::join(
                JoinType::Inner,
                result,
                TableRef::scan(&target_table, &rel.to),
                target_cond,
            );
            joined.insert(rel.to.clone());
        }
    }

    Ok((result, edge_aliases))
}

/// Build a WHERE clause from node conditions and edge filters.
/// Conditions for nodes in `skip_nodes` are excluded — those filters
/// are handled via `_nf_*` CTEs instead.
fn build_where(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
    skip_nodes: &HashSet<String>,
) -> Option<Expr> {
    let mut conds: Vec<Expr> = Vec::new();

    for node in nodes {
        if skip_nodes.contains(&node.id) {
            continue;
        }
        conds.extend(id_filter(&node.id, DEFAULT_PRIMARY_KEY, &node.node_ids));
        if let Some(r) = &node.id_range {
            conds.push(Expr::binary(
                Op::Ge,
                Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                Expr::int(r.start),
            ));
            conds.push(Expr::binary(
                Op::Le,
                Expr::col(&node.id, DEFAULT_PRIMARY_KEY),
                Expr::int(r.end),
            ));
        }
        for (prop, filter) in &node.filters {
            conds.push(filter_expr(&node.id, prop, filter));
        }
    }

    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            for (prop, filter) in &rel.filters {
                conds.push(filter_expr(alias, prop, filter));
            }
            if rel.max_hops > 1 && rel.min_hops > 1 {
                conds.push(Expr::binary(
                    Op::Ge,
                    Expr::col(alias, DEPTH_COLUMN),
                    Expr::int(rel.min_hops as i64),
                ));
            }
        }
    }

    Expr::and_all(conds.into_iter().map(Some))
}

/// Join from source node to edge table.
fn source_join_cond(node: &str, edge: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(
            Expr::col(node, DEFAULT_PRIMARY_KEY),
            Expr::col(edge, SOURCE_ID_COLUMN),
        ),
        Direction::Incoming => Expr::eq(
            Expr::col(node, DEFAULT_PRIMARY_KEY),
            Expr::col(edge, TARGET_ID_COLUMN),
        ),
        Direction::Both => Expr::or(
            Expr::eq(
                Expr::col(node, DEFAULT_PRIMARY_KEY),
                Expr::col(edge, SOURCE_ID_COLUMN),
            ),
            Expr::eq(
                Expr::col(node, DEFAULT_PRIMARY_KEY),
                Expr::col(edge, TARGET_ID_COLUMN),
            ),
        ),
    }
}

/// Join from edge table to target node.
fn target_join_cond(edge: &str, node: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(
            Expr::col(edge, TARGET_ID_COLUMN),
            Expr::col(node, DEFAULT_PRIMARY_KEY),
        ),
        Direction::Incoming => Expr::eq(
            Expr::col(edge, SOURCE_ID_COLUMN),
            Expr::col(node, DEFAULT_PRIMARY_KEY),
        ),
        Direction::Both => Expr::or(
            Expr::eq(
                Expr::col(edge, TARGET_ID_COLUMN),
                Expr::col(node, DEFAULT_PRIMARY_KEY),
            ),
            Expr::eq(
                Expr::col(edge, SOURCE_ID_COLUMN),
                Expr::col(node, DEFAULT_PRIMARY_KEY),
            ),
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE Clause
// ─────────────────────────────────────────────────────────────────────────────

fn id_filter(table: &str, col: &str, ids: &[i64]) -> Option<Expr> {
    Expr::col_in(
        table,
        col,
        ChType::Int64,
        ids.iter().map(|&id| Value::from(id)).collect(),
    )
}

fn filter_expr(table: &str, column: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(table, column);
    let val = || {
        let v = filter.value.clone().unwrap_or(Value::Null);
        Expr::param(ChType::from_value(&v), v)
    };

    match filter.op {
        None | Some(FilterOp::Eq) => Expr::eq(col, val()),
        Some(FilterOp::Gt) => Expr::binary(Op::Gt, col, val()),
        Some(FilterOp::Lt) => Expr::binary(Op::Lt, col, val()),
        Some(FilterOp::Gte) => Expr::binary(Op::Ge, col, val()),
        Some(FilterOp::Lte) => Expr::binary(Op::Le, col, val()),
        Some(FilterOp::In) => Expr::binary(Op::In, col, val()),
        Some(FilterOp::Contains) => like_pattern(col, filter, "%", "%"),
        Some(FilterOp::StartsWith) => like_pattern(col, filter, "", "%"),
        Some(FilterOp::EndsWith) => like_pattern(col, filter, "%", ""),
        Some(FilterOp::IsNull) => Expr::unary(Op::IsNull, col),
        Some(FilterOp::IsNotNull) => Expr::unary(Op::IsNotNull, col),
    }
}

fn like_pattern(col: Expr, filter: &InputFilter, prefix: &str, suffix: &str) -> Expr {
    let raw = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    let s = escape_like(raw);
    Expr::binary(Op::Like, col, Expr::string(format!("{prefix}{s}{suffix}")))
}

/// Escape LIKE metacharacters so user input is matched literally.
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn find_node<'a>(nodes: &'a [InputNode], id: &str) -> Result<&'a InputNode> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| QueryError::Lowering(format!("node '{id}' not found")))
}

fn resolve_table(node: &InputNode) -> Result<String> {
    node.table
        .clone()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no resolved table", node.id)))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(irrefutable_let_patterns)]
mod tests {
    use super::*;
    use crate::input::parse_input;
    use crate::passes::normalize;
    use crate::passes::validate;
    use ontology::Ontology;

    fn has_scan(t: &TableRef, tbl: &str) -> bool {
        match t {
            TableRef::Scan { table, .. } => table == tbl,
            TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
            TableRef::Union { queries, .. } => queries.iter().any(|q| has_scan(&q.from, tbl)),
            TableRef::Subquery { query, .. } => has_scan(&query.from, tbl),
        }
    }

    fn test_ontology() -> Ontology {
        use ontology::DataType;
        Ontology::new()
            .with_nodes([
                "User",
                "Project",
                "Note",
                "Group",
                "MergeRequest",
                "MergeRequestDiff",
            ])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF", "HAS_DIFF"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("state", DataType::String),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_default_columns("User", ["username", "state"])
            .with_fields(
                "Note",
                [
                    ("confidential", DataType::Bool),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_default_columns("Note", ["confidential"])
            .with_fields("Project", [("name", DataType::String)])
            .with_default_columns("Project", ["name"])
            .with_fields(
                "MergeRequest",
                [
                    ("title", DataType::String),
                    ("state", DataType::String),
                    ("iid", DataType::Int),
                ],
            )
            .with_default_columns("MergeRequest", ["title", "state"])
            .with_fields(
                "MergeRequestDiff",
                [
                    ("merge_request_id", DataType::Int),
                    ("state", DataType::String),
                ],
            )
            .with_default_columns("MergeRequestDiff", ["merge_request_id", "state"])
            .with_redaction("MergeRequestDiff", "merge_request", "merge_request_id")
    }

    fn validated_input(json: &str) -> Input {
        let ontology = test_ontology();
        let input = parse_input(json).unwrap();
        validate::Validator::new(&ontology)
            .check_references(&input)
            .unwrap();
        normalize::normalize(input, &ontology).unwrap()
    }

    #[test]
    fn test_lower_simple_traversal() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "entity": "Note"},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(q.limit, Some(25));
        // Edge-centric: 5 edge columns (no traversal_path) + redaction ID/type pairs
        assert!(q.select.len() >= 5);
        assert!(
            !q.select
                .iter()
                .any(|s| s.alias.as_deref() == Some("e0_path"))
        );
    }

    #[test]
    fn test_lower_aggregation() {
        let mut input = validated_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "entity": "Note"}, {"id": "u", "entity": "User", "columns": ["username"]}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        assert!(!q.group_by.is_empty());
        assert!(
            q.select
                .iter()
                .any(|s| matches!(&s.expr, Expr::FuncCall { name, .. } if name == "COUNT"))
        );
    }

    #[test]
    fn test_lower_aggregation_with_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "mr", "entity": "Note"},
                {"id": "u", "entity": "User", "columns": ["username", "state"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
            "limit": 20
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Should have group-by node columns: u_username, u_state
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));

        // Should have aggregation result
        assert!(aliases.contains(&&"mr_count".to_string()));

        // Should NOT have target node id column (mr is aggregated, not grouped)
        assert!(!aliases.contains(&&"mr_id".to_string()));

        // GROUP BY should include all selected columns from group-by node
        assert_eq!(q.group_by.len(), 2); // username, state
    }

    #[test]
    fn test_lower_aggregation_with_wildcard_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "n", "entity": "Note"},
                {"id": "u", "entity": "User", "columns": "*"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Should have all user columns from ontology
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));
        assert!(aliases.contains(&&"u_created_at".to_string()));

        // Should have aggregation result
        assert!(aliases.contains(&&"note_count".to_string()));

        // GROUP BY should include all entity columns
        assert!(q.group_by.len() >= 3); // 3 fields from ontology
    }

    #[test]
    fn test_lower_path_finding() {
        let mut input = validated_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // Lowering produces forward + backward CTEs only.
        // Hop frontier CTEs are added by the optimize pass.
        assert_eq!(q.ctes.len(), 2);
        assert_eq!(q.ctes[0].name, "forward");
        assert_eq!(q.ctes[1].name, "backward");
        assert!(q.ctes.iter().all(|c| !c.recursive));
    }

    #[test]
    fn test_lower_with_filters() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            },
            "limit": 30
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn test_lower_multi_relationship() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"},
                {"type": "CONTAINS", "from": "p", "to": "n"}
            ],
            "limit": 20
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        // Edge-centric: FROM is a single edge table scan, no node joins.
        // 2 relationships = 2 edge scans joined together.
        assert!(matches!(
            q.from,
            TableRef::Scan { .. } | TableRef::Join { .. }
        ));
        assert_eq!(q.limit, Some(20));
    }

    /// Count union subqueries in a table reference tree
    fn count_unions(table_ref: &TableRef) -> usize {
        match table_ref {
            TableRef::Union { .. } => 1,
            TableRef::Join { left, right, .. } => count_unions(left) + count_unions(right),
            TableRef::Scan { .. } | TableRef::Subquery { .. } => 0,
        }
    }

    /// Find union with a specific alias
    fn find_union_alias(table_ref: &TableRef, alias: &str) -> bool {
        match table_ref {
            TableRef::Union { alias: a, .. } => a == alias,
            TableRef::Join { left, right, .. } => {
                find_union_alias(left, alias) || find_union_alias(right, alias)
            }
            TableRef::Scan { .. } | TableRef::Subquery { .. } => false,
        }
    }

    fn find_union<'a>(table_ref: &'a TableRef, alias: &str) -> Option<&'a TableRef> {
        match table_ref {
            TableRef::Union { alias: a, .. } if a == alias => Some(table_ref),
            TableRef::Join { left, right, .. } => {
                find_union(left, alias).or_else(|| find_union(right, alias))
            }
            TableRef::Scan { .. } | TableRef::Union { .. } | TableRef::Subquery { .. } => None,
        }
    }

    #[test]
    fn test_lower_variable_length_path() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 1,
                "max_hops": 3
            }],
            "limit": 25
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        // Should have a union subquery for the multi-hop relationship
        assert_eq!(
            count_unions(&q.from),
            1,
            "expected one union subquery for multi-hop"
        );
        assert!(
            find_union_alias(&q.from, "hop_e0"),
            "expected union with alias hop_e0"
        );

        let select_aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(!select_aliases.contains(&&"hop_e0_path".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_type".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_src".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_src_type".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_dst".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_dst_type".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_depth".to_string()));
        assert!(select_aliases.contains(&&"hop_e0_path_nodes".to_string()));

        let Some(TableRef::Union { queries, .. }) = find_union(&q.from, "hop_e0") else {
            panic!("expected hop_e0 union table");
        };
        assert_eq!(
            queries.len(),
            3,
            "max_hops=3 should produce three union arms"
        );

        for query in queries {
            let aliases: Vec<_> = query
                .select
                .iter()
                .filter_map(|s| s.alias.as_deref())
                .collect();
            assert_eq!(
                aliases,
                vec![
                    "start_id",
                    "end_id",
                    "path_nodes",
                    "relationship_kind",
                    "source_id",
                    "source_kind",
                    "target_id",
                    "target_kind",
                    "depth",
                ]
            );
        }
    }

    #[test]
    fn test_lower_variable_length_with_min_hops() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 2,
                "max_hops": 3
            }],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        // Should have a union subquery for the multi-hop relationship
        assert_eq!(count_unions(&q.from), 1);
    }

    #[test]
    fn test_lower_mixed_single_and_multi_hop() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"},
                {"type": "CONTAINS", "from": "p", "to": "n", "min_hops": 1, "max_hops": 2}
            ],
            "limit": 20
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        // Should have one union subquery for the second relationship (multi-hop)
        assert_eq!(
            count_unions(&q.from),
            1,
            "expected one union subquery for multi-hop relationship"
        );
        assert!(
            find_union_alias(&q.from, "hop_e1"),
            "expected union with alias hop_e1 for second relationship"
        );
    }

    #[test]
    fn test_lower_single_hop_no_union() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{
                "type": "AUTHORED",
                "from": "u",
                "to": "n",
                "min_hops": 1,
                "max_hops": 1
            }],
            "limit": 25
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // Single hop should NOT generate a union subquery
        assert_eq!(
            count_unions(&q.from),
            0,
            "single hop should not generate union subquery"
        );
    }

    #[test]
    fn test_lower_search() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "filters": {
                    "username": {"op": "starts_with", "value": "admin"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            },
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        assert_eq!(q.limit, Some(10));
        let user_defaults = test_ontology()
            .get_node("User")
            .unwrap()
            .default_columns
            .len();
        assert_eq!(q.select.len(), user_defaults);
        assert!(q.where_clause.is_some());
        assert!(q.group_by.is_empty());
        assert_eq!(count_unions(&q.from), 0);
    }

    #[test]
    fn test_lower_search_simple() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "p",
                "entity": "Project"
            },
            "limit": 50
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        assert_eq!(q.limit, Some(50));
        let project_defaults = test_ontology()
            .get_node("Project")
            .unwrap()
            .default_columns
            .len();
        assert_eq!(q.select.len(), project_defaults);
    }

    #[test]
    fn test_lower_with_specific_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": ["username", "state"]
            },
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        assert_eq!(q.select.len(), 2);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));
    }

    #[test]
    fn test_lower_with_wildcard_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": "*"
            },
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // All fields from ontology (username, state, created_at)
        assert!(q.select.len() >= 3);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));
        assert!(aliases.contains(&&"u_created_at".to_string()));
    }

    #[test]
    fn test_lower_traversal_with_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "n", "entity": "Note", "columns": ["confidential"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 20
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // Edge-centric: edge columns + redaction IDs (no node property columns)
        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
        assert_eq!(q.limit, Some(20));
    }

    #[test]
    fn test_lower_no_columns_uses_defaults() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User"
            },
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let user_defaults = test_ontology()
            .get_node("User")
            .unwrap()
            .default_columns
            .len();
        assert_eq!(q.select.len(), user_defaults);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));
    }

    #[test]
    fn test_lower_columns_with_id_in_list() {
        let mut input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User",
                "columns": ["id", "username"]
            },
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // When id is explicitly in the list, it should appear once
        assert_eq!(q.select.len(), 2);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_id".to_string()));
        assert!(aliases.contains(&&"u_username".to_string()));
    }

    #[test]
    fn test_edge_select_exprs_generates_all_columns() {
        let exprs = edge_select_exprs("e0");

        assert_eq!(exprs.len(), 5);

        let aliases: Vec<_> = exprs.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(!aliases.contains(&&"e0_path".to_string()));
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
        assert!(aliases.contains(&&"e0_src_type".to_string()));
        assert!(aliases.contains(&&"e0_dst".to_string()));
        assert!(aliases.contains(&&"e0_dst_type".to_string()));
    }

    #[test]
    fn test_path_finding_cte_structure() {
        let mut input = validated_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2}
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // Final select: _gkg_path + _gkg_edge_kinds + depth
        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"_gkg_path".to_string()));
        assert!(aliases.contains(&&"_gkg_edge_kinds".to_string()));
        assert!(aliases.contains(&&"depth".to_string()));
        assert!(!aliases.contains(&&"_gkg_edges".to_string()));

        // Forward CTE columns: anchor_id, end_id, end_kind, path_nodes, edge_kinds, depth
        assert!(!q.ctes.is_empty());
        assert_eq!(q.ctes[0].name, "forward");
        let cte_select: Vec<_> = q.ctes[0]
            .query
            .select
            .iter()
            .filter_map(|s| s.alias.as_ref())
            .collect();
        assert!(cte_select.contains(&&"anchor_id".to_string()));
        assert!(cte_select.contains(&&"end_id".to_string()));
        assert!(cte_select.contains(&&"path_nodes".to_string()));
        assert!(cte_select.contains(&&"edge_kinds".to_string()));
        assert!(cte_select.contains(&&"depth".to_string()));

        // Non-recursive CTEs (no limit on CTE itself)
        assert!(!q.ctes[0].recursive);
    }

    #[test]
    fn test_neighbors_includes_edge_columns() {
        use crate::input::{Direction, InputNeighbors};

        let mut input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "u".to_string(),
                entity: Some("User".to_string()),
                table: Some("gl_user".to_string()),
                node_ids: vec![123],
                ..Default::default()
            }],
            neighbors: Some(InputNeighbors {
                node: "u".to_string(),
                direction: Direction::Outgoing,
                rel_types: vec![],
            }),
            limit: 10,
            ..Input::default()
        };

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Should have neighbor columns
        assert!(aliases.contains(&&"_gkg_neighbor_id".to_string()));
        assert!(aliases.contains(&&"_gkg_neighbor_type".to_string()));
        assert!(aliases.contains(&&"_gkg_relationship_type".to_string()));
        assert!(aliases.contains(&&"_gkg_neighbor_is_outgoing".to_string()));

        // Should NOT have raw edge columns (indirect auth uses static/dynamic nodes instead)
        assert!(!aliases.contains(&&"e_path".to_string()));
        assert!(!aliases.contains(&&"e_src".to_string()));
        assert!(!aliases.contains(&&"e_dst".to_string()));
    }

    #[test]
    fn test_lower_neighbors_both_direction() {
        use crate::input::{Direction, InputNeighbors};

        let mut input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "g".to_string(),
                entity: Some("Group".to_string()),
                table: Some("gl_group".to_string()),
                node_ids: vec![100],
                ..Default::default()
            }],
            neighbors: Some(InputNeighbors {
                node: "g".to_string(),
                direction: Direction::Both,
                rel_types: vec![],
            }),
            limit: 10,
            ..Input::default()
        };

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        assert!(aliases.contains(&&"_gkg_neighbor_is_outgoing".to_string()));
        assert!(aliases.contains(&&"_gkg_neighbor_id".to_string()));
        assert!(aliases.contains(&&"_gkg_neighbor_type".to_string()));
        assert!(aliases.contains(&&"_gkg_relationship_type".to_string()));
    }

    #[test]
    fn test_multi_relationship_has_multiple_edge_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"},
                {"type": "CONTAINS", "from": "p", "to": "n"}
            ],
            "limit": 20
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Edge-centric: should have edge columns for at least the first relationship
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
    }

    #[test]
    fn test_type_filter_variants() {
        fn has_type_filter(expr: &Expr) -> bool {
            match expr {
                Expr::BinaryOp { op, left, right } => match (op, left.as_ref(), right.as_ref()) {
                    (Op::Eq, Expr::Column { column, .. }, Expr::Param { .. })
                    | (Op::In, Expr::Column { column, .. }, Expr::Param { .. })
                        if column == "relationship_kind" =>
                    {
                        true
                    }
                    _ => has_type_filter(left) || has_type_filter(right),
                },
                _ => false,
            }
        }

        // Edge-centric puts the type filter in WHERE, not JOIN ON.
        // Single type — WHERE should contain relationship_kind = 'AUTHORED'
        let mut inp = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":"AUTHORED","from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&mut inp).unwrap() else {
            panic!()
        };
        assert!(
            q.where_clause.as_ref().is_some_and(has_type_filter),
            "expected type filter in WHERE"
        );

        // Multiple types — should use IN
        let mut inp = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":["AUTHORED","CONTAINS"],"from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&mut inp).unwrap() else {
            panic!()
        };
        assert!(
            q.where_clause.as_ref().is_some_and(has_type_filter),
            "expected type filter in WHERE"
        );

        // Wildcard — no type filter
        let mut inp = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":"*","from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&mut inp).unwrap() else {
            panic!()
        };
        assert!(
            q.where_clause.is_none() || !q.where_clause.as_ref().is_some_and(has_type_filter),
            "wildcard should not have type filter"
        );
    }

    fn contains_starts_with(expr: &Expr) -> bool {
        match expr {
            Expr::FuncCall { name, .. } if name == "startsWith" => true,
            Expr::BinaryOp { left, right, .. } => {
                contains_starts_with(left) || contains_starts_with(right)
            }
            Expr::UnaryOp { expr, .. } => contains_starts_with(expr),
            _ => false,
        }
    }

    fn table_ref_has_starts_with(table_ref: &TableRef) -> bool {
        match table_ref {
            TableRef::Join {
                on, left, right, ..
            } => {
                contains_starts_with(on)
                    || table_ref_has_starts_with(left)
                    || table_ref_has_starts_with(right)
            }
            TableRef::Union { queries, .. } => {
                queries.iter().any(|q| table_ref_has_starts_with(&q.from))
            }
            TableRef::Subquery { query, .. } => table_ref_has_starts_with(&query.from),
            TableRef::Scan { .. } => false,
        }
    }

    #[test]
    fn no_starts_with_in_single_hop_join() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        assert!(
            !table_ref_has_starts_with(&q.from),
            "single-hop join should not contain startsWith"
        );
    }

    #[test]
    fn no_starts_with_in_multi_hop_join() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "p",
                "min_hops": 1,
                "max_hops": 3
            }],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        assert!(
            !table_ref_has_starts_with(&q.from),
            "multi-hop join should not contain startsWith"
        );
    }

    #[test]
    fn no_starts_with_in_neighbors_join() {
        use crate::input::{Direction, InputNeighbors};

        let mut input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "g".to_string(),
                entity: Some("Group".to_string()),
                table: Some("gl_group".to_string()),
                node_ids: vec![100],
                ..Default::default()
            }],
            neighbors: Some(InputNeighbors {
                node: "g".to_string(),
                direction: Direction::Both,
                rel_types: vec![],
            }),
            limit: 10,
            ..Input::default()
        };

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };
        assert!(
            !table_ref_has_starts_with(&q.from),
            "neighbors join should not contain startsWith"
        );
    }

    #[test]
    fn test_order_by_node_property_joins_node_table() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "n", "entity": "Note", "columns": ["confidential"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"},
            "limit": 25
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        assert!(
            has_scan(&q.from, "gl_note"),
            "order_by on node property should JOIN the node table"
        );

        assert_eq!(q.order_by.len(), 1);
        if let Expr::Column { table, column } = &q.order_by[0].expr {
            assert_eq!(table, "n");
            assert_eq!(column, "created_at");
        } else {
            panic!("expected column expression in order_by");
        }
        assert!(q.order_by[0].desc);
    }

    #[test]
    fn test_order_by_id_uses_edge_column() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 25
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        assert!(
            !has_scan(&q.from, "gl_user"),
            "order_by id should not add a node table JOIN"
        );

        assert_eq!(q.order_by.len(), 1);
        if let Expr::Column { table, column } = &q.order_by[0].expr {
            assert_eq!(table, "e0");
            assert_eq!(column, "source_id");
        } else {
            panic!("expected column expression in order_by");
        }
    }

    #[test]
    fn test_order_by_target_node_property_joins_target_table() {
        // order_by on the "to" (target) side node
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "order_by": {"node": "u", "property": "username", "direction": "ASC"},
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // source-side node should get its table joined for username
        assert!(has_scan(&q.from, "gl_user"));
        assert!(!q.order_by[0].desc);
        if let Expr::Column { table, column } = &q.order_by[0].expr {
            assert_eq!(table, "u");
            assert_eq!(column, "username");
        } else {
            panic!("expected column expression");
        }
    }

    #[test]
    fn test_order_by_id_on_target_side_uses_edge_column() {
        // order_by id on the "to" side — should use target_id edge column
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "order_by": {"node": "n", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        if let Expr::Column { table, column } = &q.order_by[0].expr {
            assert_eq!(table, "e0");
            assert_eq!(column, "target_id");
        } else {
            panic!("expected column expression");
        }
    }

    #[test]
    fn test_order_by_with_filters_and_node_ids() {
        // order_by combined with filters and node_ids — both the
        // node filter CTE and the order_by JOIN should be present
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "node_ids": [1, 2]},
                {"id": "n", "entity": "Note"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "order_by": {"node": "n", "property": "created_at", "direction": "DESC"},
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // gl_note joined for order_by
        assert!(has_scan(&q.from, "gl_note"));
        // node_ids filter CTE present
        assert!(
            q.ctes.iter().any(|c| c.name == "_nf_u"),
            "node_ids filter CTE should exist"
        );
        // WHERE should have the IN subquery
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn test_order_by_multi_hop_still_works() {
        // Multi-hop uses UNION ALL; order_by should still work
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "p", "min_hops": 1, "max_hops": 2}],
            "order_by": {"node": "u", "property": "username", "direction": "ASC"},
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        // Multi-hop gets user table joined for ordering
        assert!(has_scan(&q.from, "gl_user"));
        assert_eq!(q.order_by.len(), 1);
    }

    #[test]
    fn test_order_by_merge_request_title() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "order_by": {"node": "mr", "property": "title", "direction": "ASC"},
            "limit": 25
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        assert!(
            has_scan(&q.from, "gl_mergerequest"),
            "order_by mr.title should JOIN gl_mergerequest"
        );
        assert!(!q.order_by[0].desc);
        if let Expr::Column { table, column } = &q.order_by[0].expr {
            assert_eq!(table, "mr");
            assert_eq!(column, "title");
        } else {
            panic!("expected column expression");
        }
    }

    #[test]
    fn traversal_stores_node_edge_col_in_metadata() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest"},
                {"id": "d", "entity": "MergeRequestDiff"}
            ],
            "relationships": [{"type": "HAS_DIFF", "from": "mr", "to": "d"}],
            "limit": 10
        }"#,
        );

        lower(&mut input).unwrap();

        let nec = &input.compiler.node_edge_col;
        assert_eq!(nec.len(), 2);
        assert_eq!(nec.get("mr"), Some(&("e0".into(), "source_id".into())));
        assert_eq!(nec.get("d"), Some(&("e0".into(), "target_id".into())));
    }

    #[test]
    fn traversal_does_not_emit_gkg_columns() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_deref()).collect();
        assert!(
            !aliases.iter().any(|a| a.starts_with("_gkg_")),
            "_gkg_* columns should be emitted by enforce, not lower"
        );
    }

    #[test]
    fn traversal_with_filters_stores_metadata_and_cte() {
        let mut input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "mr", "entity": "MergeRequest", "node_ids": [2000]},
                {"id": "d", "entity": "MergeRequestDiff"}
            ],
            "relationships": [{"type": "HAS_DIFF", "from": "mr", "to": "d"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&mut input).unwrap() else {
            panic!("expected Query");
        };

        assert!(q.ctes.iter().any(|c| c.name == "_nf_mr"));
        assert_eq!(input.compiler.node_edge_col.len(), 2);
    }

    // ── escape_like ─────────────────────────────────────────────────

    #[test]
    fn escape_like_preserves_plain_text() {
        assert_eq!(super::escape_like("hello"), "hello");
    }

    #[test]
    fn escape_like_escapes_percent() {
        assert_eq!(super::escape_like("100%"), "100\\%");
    }

    #[test]
    fn escape_like_escapes_underscore() {
        assert_eq!(super::escape_like("user_name"), "user\\_name");
    }

    #[test]
    fn escape_like_escapes_backslash() {
        assert_eq!(super::escape_like("path\\to"), "path\\\\to");
    }

    #[test]
    fn escape_like_escapes_all_metacharacters() {
        assert_eq!(super::escape_like("100%_\\"), "100\\%\\_\\\\");
    }
}

//! Lower: Input → AST
//!
//! Transforms validated input into a SQL-oriented AST.

use crate::ast::{ChType, Cte, Expr, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
use crate::constants::{
    EDGE_ALIAS_SUFFIXES, NEIGHBOR_ID_COLUMN, NEIGHBOR_IS_OUTGOING_COLUMN, NEIGHBOR_TYPE_COLUMN,
    RELATIONSHIP_TYPE_COLUMN,
};
use crate::error::{QueryError, Result};
use crate::input::{
    ColumnSelection, Direction, FilterOp, Input, InputAggregation, InputFilter, InputNode,
    InputRelationship, OrderDirection, QueryType,
};
use ontology::constants::{
    DEFAULT_PRIMARY_KEY, EDGE_RESERVED_COLUMNS, EDGE_TABLE, TRAVERSAL_PATH_COLUMN,
};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Generate SELECT expressions for all edge columns with the given table alias.
fn edge_select_exprs(alias: &str) -> Vec<SelectExpr> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .zip(EDGE_ALIAS_SUFFIXES.iter())
        .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{alias}_{suffix}")))
        .collect()
}

fn edge_depth_select_expr(alias: &str) -> SelectExpr {
    SelectExpr::new(Expr::col(alias, "depth"), format!("{alias}_depth"))
}

fn edge_path_nodes_select_expr(alias: &str) -> SelectExpr {
    SelectExpr::new(
        Expr::col(alias, "path_nodes"),
        format!("{alias}_path_nodes"),
    )
}

/// Derive LIMIT and OFFSET from the input's pagination fields.
/// If `range` is set, limit = end - start and offset = start.
/// Otherwise, limit = input.limit and offset = None.
fn pagination(input: &Input) -> (Option<u32>, Option<u32>) {
    if let Some(ref range) = input.range {
        (Some(range.end - range.start), Some(range.start))
    } else {
        (Some(input.limit), None)
    }
}

/// Lower validated input into an AST node.
///
/// Writes metadata to `input.compiler` for downstream passes.
pub fn lower(input: &mut Input) -> Result<Node> {
    match input.query_type {
        QueryType::Search => lower_search(input),
        QueryType::Traversal => lower_traversal(input),
        QueryType::Aggregation => lower_aggregation(input),
        QueryType::PathFinding => lower_path_finding(input),
        QueryType::Neighbors => lower_neighbors(input),
        QueryType::Hydration => lower_hydration(input),
    }
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
    let (limit, offset) = pagination(input);

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        order_by,
        limit,
        offset,
        ..Default::default()
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal
// ─────────────────────────────────────────────────────────────────────────────

fn lower_traversal(input: &Input) -> Result<Node> {
    lower_traversal_edge_only(input)
}

/// Edge-only traversal: edges are the FROM tables, node tables are
/// referenced only via IN subqueries for filtering. Node properties are
/// deferred to the hydration pipeline.
///
/// Single-hop: flat edge scan.
/// Multi-hop: UNION ALL of edge self-joins.
/// Multi-rel: secondary edges JOINed on shared columns.
fn lower_traversal_edge_only(input: &Input) -> Result<Node> {
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
                "relationship_kind",
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

    // Add _gkg_* ID/type columns for ALL nodes
    for node in &input.nodes {
        let entity = node.entity.as_deref().unwrap_or("Unknown");
        let id_expr = if let Some((alias, col)) = node_edge_col.get(&node.id) {
            Expr::col(alias, col.as_str())
        } else {
            Expr::param(ChType::Int64, 0i64)
        };
        select.push(SelectExpr::new(id_expr, format!("_gkg_{}_id", node.id)));
        select.push(SelectExpr::new(
            Expr::param(ChType::String, entity.to_string()),
            format!("_gkg_{}_type", node.id),
        ));
    }

    // Add IN subquery for each node that has conditions
    for node in &input.nodes {
        let has_conditions = !node.node_ids.is_empty() || !node.filters.is_empty();
        if !has_conditions {
            continue;
        }
        if let Some((alias, edge_col)) = node_edge_col.get(&node.id) {
            let table = resolve_table(node)?;
            let node_where = build_node_where(node);
            let cte_name = format!("_nf_{}", node.id);
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
        && ob.property != "id"
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
            ("id", Some((alias, edge_col))) => Expr::col(alias, edge_col.as_str()),
            _ => Expr::col(&ob.node, &ob.property),
        };
        vec![OrderExpr {
            expr,
            desc: ob.direction == OrderDirection::Desc,
        }]
    });
    let (limit, offset) = pagination(input);

    Ok(Node::Query(Box::new(Query {
        ctes,
        select,
        from,
        where_clause,
        order_by,
        limit,
        offset,
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
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships)?;
    let where_clause = build_full_where(&input.nodes, &input.relationships, &edge_aliases);

    let group_by_node_ids: HashSet<_> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.clone())
        .collect();

    let mut select = Vec::new();
    let mut group_by = Vec::new();

    for node in &input.nodes {
        if !group_by_node_ids.contains(&node.id) {
            continue;
        }
        if let Some(ColumnSelection::List(cols)) = &node.columns {
            for col in cols {
                let expr = Expr::col(&node.id, col);
                select.push(SelectExpr::new(expr.clone(), format!("{}_{col}", node.id)));
                group_by.push(expr);
            }
        }
    }

    for agg in &input.aggregations {
        select.push(SelectExpr::new(
            agg_expr(agg),
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
                expr: agg_expr(agg),
                desc: s.direction == OrderDirection::Desc,
            }]
        });

    let (limit, offset) = pagination(input);

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        group_by,
        order_by,
        limit,
        offset,
        ..Default::default()
    })))
}

fn agg_expr(agg: &InputAggregation) -> Expr {
    let arg = match (&agg.property, &agg.target) {
        (Some(prop), Some(target)) => Expr::col(target, prop),
        (None, Some(target)) => Expr::col(target, DEFAULT_PRIMARY_KEY),
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
        "forward",
        build_frontier(&start.node_ids, forward_depth, &rel_type_filter, true),
    );
    let backward_cte = if backward_depth > 0 {
        Some(Cte::new(
            "backward",
            build_frontier(&end.node_ids, backward_depth, &rel_type_filter, false),
        ))
    } else {
        None
    };

    // Helper: build a start-node tuple from the forward frontier's anchor_id.
    let start_tuple = |table: &str| {
        Expr::func(
            "tuple",
            vec![Expr::col(table, "anchor_id"), Expr::string(start_entity)],
        )
    };
    let end_tuple = |table: &str| {
        Expr::func(
            "tuple",
            vec![Expr::col(table, "anchor_id"), Expr::string(end_entity)],
        )
    };

    // Direct depth-1 paths: forward frontier reaching end directly.
    let direct_query = Query {
        select: vec![
            SelectExpr::new(Expr::col("f", "depth"), "depth"),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::func("array", vec![start_tuple("f")]),
                        Expr::col("f", "path_nodes"),
                    ],
                ),
                "_gkg_path",
            ),
            SelectExpr::new(Expr::col("f", "edge_kinds"), "_gkg_edge_kinds"),
        ],
        from: TableRef::scan("forward", "f"),
        where_clause: Expr::and_all([
            Some(Expr::binary(Op::Eq, Expr::col("f", "depth"), Expr::int(1))),
            Expr::col_in(
                "f",
                "end_id",
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
                Expr::binary(Op::Add, Expr::col("f", "depth"), Expr::col("b", "depth")),
                "depth",
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::func("array", vec![start_tuple("f")]),
                        Expr::col("f", "path_nodes"),
                        Expr::func("arrayReverse", vec![Expr::col("b", "path_nodes")]),
                        Expr::func("array", vec![end_tuple("b")]),
                    ],
                ),
                "_gkg_path",
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("f", "edge_kinds"),
                        Expr::func("arrayReverse", vec![Expr::col("b", "edge_kinds")]),
                    ],
                ),
                "_gkg_edge_kinds",
            ),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan("forward", "f"),
            TableRef::scan("backward", "b"),
            Expr::eq(Expr::col("f", "end_id"), Expr::col("b", "end_id")),
        ),
        where_clause: Some(Expr::binary(
            Op::Le,
            Expr::binary(Op::Add, Expr::col("f", "depth"), Expr::col("b", "depth")),
            Expr::int(max_depth as i64),
        )),
        ..Default::default()
    };

    // Combine direct + intersection as a UNION ALL subquery.
    let paths_union = if backward_depth == 0 {
        TableRef::subquery(direct_query, "paths")
    } else {
        TableRef::union_all(vec![direct_query, intersection_query], "paths")
    };

    let (limit, offset) = pagination(input);

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
            SelectExpr::new(Expr::col("paths", "_gkg_path"), "_gkg_path"),
            SelectExpr::new(Expr::col("paths", "_gkg_edge_kinds"), "_gkg_edge_kinds"),
            SelectExpr::new(Expr::col("paths", "depth"), "depth"),
        ],
        from: paths_union,
        order_by: vec![OrderExpr {
            expr: Expr::col("paths", "depth"),
            desc: false,
        }],
        limit,
        offset,
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
        ("source_id", "target_id", "target_kind")
    } else {
        ("target_id", "source_id", "source_kind")
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
            .map(|i| Expr::col(format!("e{i}"), "relationship_kind"))
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
            SelectExpr::new(Expr::col("e1", anchor_col), "anchor_id"),
            SelectExpr::new(Expr::col(&last, next_col), "end_id"),
            SelectExpr::new(Expr::col(&last, next_kind_col), "end_kind"),
            SelectExpr::new(path_nodes, "path_nodes"),
            SelectExpr::new(edge_kinds, "edge_kinds"),
            SelectExpr::new(Expr::int(depth as i64), "depth"),
        ],
        from,
        where_clause: Expr::and_all([anchor_cond, first_type_cond]),
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

fn lower_neighbors(input: &Input) -> Result<Node> {
    let neighbors_config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = find_node(&input.nodes, &neighbors_config.node)?;
    let center_table = resolve_table(center_node)?;
    let center_entity = center_node
        .entity
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("center node entity missing".into()))?;

    let type_filter = type_filter(&neighbors_config.rel_types);
    let edge_alias = "e";
    let where_clause = id_filter(&center_node.id, DEFAULT_PRIMARY_KEY, &center_node.node_ids);
    let order_by = input.order_by.as_ref().map_or(vec![], |ob| {
        vec![OrderExpr {
            expr: Expr::col(&ob.node, &ob.property),
            desc: ob.direction == OrderDirection::Desc,
        }]
    });
    let (limit, offset) = pagination(input);

    // For Direction::Both, split into UNION ALL of outgoing + incoming so
    // ClickHouse can select the optimal access path for each direction
    // (base table PK or by_source/by_target projections). An OR join
    // (source_id = X OR target_id = X) prevents index use.
    if neighbors_config.direction == Direction::Both {
        let build_arm = |dir: Direction| -> Query {
            let (edge_table, edge_type_cond) = edge_scan(edge_alias, &type_filter);
            let mut join_cond =
                source_join_cond_with_kind(&center_node.id, edge_alias, center_entity, dir);
            if let Some(tc) = edge_type_cond {
                join_cond = Expr::and(join_cond, tc);
            }
            let (neighbor_id, neighbor_type, is_outgoing) = match dir {
                Direction::Outgoing => ("target_id", "target_kind", 1),
                Direction::Incoming => ("source_id", "source_kind", 0),
                Direction::Both => unreachable!(),
            };
            Query {
                select: vec![
                    SelectExpr::new(Expr::col(edge_alias, neighbor_id), NEIGHBOR_ID_COLUMN),
                    SelectExpr::new(Expr::col(edge_alias, neighbor_type), NEIGHBOR_TYPE_COLUMN),
                    SelectExpr::new(
                        Expr::col(edge_alias, "relationship_kind"),
                        RELATIONSHIP_TYPE_COLUMN,
                    ),
                    SelectExpr::new(Expr::int(is_outgoing), NEIGHBOR_IS_OUTGOING_COLUMN),
                ],
                from: TableRef::join(
                    JoinType::Inner,
                    TableRef::scan(&center_table, &center_node.id),
                    edge_table,
                    join_cond,
                ),
                where_clause: where_clause.clone(),
                ..Default::default()
            }
        };

        let mut outgoing = build_arm(Direction::Outgoing);
        outgoing.union_all = vec![build_arm(Direction::Incoming)];
        outgoing.order_by = order_by;
        outgoing.limit = limit;
        outgoing.offset = offset;
        return Ok(Node::Query(Box::new(outgoing)));
    }

    let (edge_table, edge_type_cond) = edge_scan(edge_alias, &type_filter);
    let mut join_cond = source_join_cond_with_kind(
        &center_node.id,
        edge_alias,
        center_entity,
        neighbors_config.direction,
    );
    if let Some(tc) = edge_type_cond {
        join_cond = Expr::and(join_cond, tc);
    }
    let (neighbor_id, neighbor_type, is_outgoing) = match neighbors_config.direction {
        Direction::Outgoing => ("target_id", "target_kind", 1i64),
        Direction::Incoming => ("source_id", "source_kind", 0i64),
        Direction::Both => unreachable!(),
    };

    Ok(Node::Query(Box::new(Query {
        select: vec![
            SelectExpr::new(Expr::col(edge_alias, neighbor_id), NEIGHBOR_ID_COLUMN),
            SelectExpr::new(Expr::col(edge_alias, neighbor_type), NEIGHBOR_TYPE_COLUMN),
            SelectExpr::new(
                Expr::col(edge_alias, "relationship_kind"),
                RELATIONSHIP_TYPE_COLUMN,
            ),
            SelectExpr::new(Expr::int(is_outgoing), NEIGHBOR_IS_OUTGOING_COLUMN),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan(&center_table, &center_node.id),
            edge_table,
            join_cond,
        ),
        where_clause,
        order_by,
        limit,
        offset,
        ..Default::default()
    })))
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
        Direction::Outgoing | Direction::Both => "target_kind",
        Direction::Incoming => "source_kind",
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
            Expr::col("e1", "relationship_kind"),
            Expr::col("e1", "source_id"),
            Expr::col("e1", "source_kind"),
            Expr::col(&last, "target_id"),
            Expr::col(&last, "target_kind"),
        ),
        Direction::Incoming => (
            Expr::col(&last, "relationship_kind"),
            Expr::col(&last, "source_id"),
            Expr::col(&last, "source_kind"),
            Expr::col("e1", "target_id"),
            Expr::col("e1", "target_kind"),
        ),
    };

    Query {
        select: vec![
            SelectExpr::new(Expr::col("e1", start_col), "start_id"),
            SelectExpr::new(Expr::col(&last, end_col), "end_id"),
            SelectExpr::new(
                Expr::col(&last, TRAVERSAL_PATH_COLUMN),
                TRAVERSAL_PATH_COLUMN,
            ),
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
                "path_nodes",
            ),
            SelectExpr::new(relationship_kind_expr, "relationship_kind"),
            SelectExpr::new(source_id_expr, "source_id"),
            SelectExpr::new(source_kind_expr, "source_kind"),
            SelectExpr::new(target_id_expr, "target_id"),
            SelectExpr::new(target_kind_expr, "target_kind"),
            SelectExpr::new(Expr::int(depth as i64), "depth"),
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
            "relationship_kind",
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

fn build_joins(
    nodes: &[InputNode],
    rels: &[InputRelationship],
) -> Result<(TableRef, HashMap<usize, String>)> {
    let start = match rels.first() {
        Some(r) => find_node(nodes, &r.from)?,
        None => nodes
            .first()
            .ok_or_else(|| QueryError::Lowering("no nodes in input".into()))?,
    };
    let start_table = resolve_table(start)?;
    let mut result = TableRef::scan(&start_table, &start.id);
    let mut edge_aliases = HashMap::new();
    let mut joined = HashSet::new();
    joined.insert(start.id.clone());

    for (i, rel) in rels.iter().enumerate() {
        let target = find_node(nodes, &rel.to)?;
        let target_table = resolve_table(target)?;
        let source_joined = joined.contains(&rel.from);
        let target_joined = joined.contains(&rel.to);

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

            let union_join_cond = match (source_joined, target_joined) {
                (true, true) => Expr::and(source_cond.clone(), target_cond.clone()),
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

            if !source_joined {
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
            if !target_joined {
                result = TableRef::join(
                    JoinType::Inner,
                    result,
                    TableRef::scan(&target_table, &rel.to),
                    target_cond,
                );
                joined.insert(rel.to.clone());
            }
        } else {
            let alias = format!("e{i}");
            edge_aliases.insert(i, alias.clone());

            let (edge, edge_type_cond) = edge_scan(&alias, &type_filter(&rel.types));
            let source_cond = source_join_cond(&rel.from, &alias, rel.direction);
            let target_cond = target_join_cond(&alias, &rel.to, rel.direction);

            let mut edge_join_cond = match (source_joined, target_joined) {
                (true, true) => Expr::and(source_cond.clone(), target_cond.clone()),
                (true, false) => source_cond.clone(),
                (false, true) => target_cond.clone(),
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

            if !source_joined {
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
            if !target_joined {
                result = TableRef::join(
                    JoinType::Inner,
                    result,
                    TableRef::scan(&target_table, &rel.to),
                    target_cond,
                );
                joined.insert(rel.to.clone());
            }
        }
    }

    Ok((result, edge_aliases))
}

/// Join from source node to edge table.
fn source_join_cond(node: &str, edge: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(
            Expr::col(node, DEFAULT_PRIMARY_KEY),
            Expr::col(edge, "source_id"),
        ),
        Direction::Incoming => Expr::eq(
            Expr::col(node, DEFAULT_PRIMARY_KEY),
            Expr::col(edge, "target_id"),
        ),
        Direction::Both => Expr::or(
            Expr::eq(
                Expr::col(node, DEFAULT_PRIMARY_KEY),
                Expr::col(edge, "source_id"),
            ),
            Expr::eq(
                Expr::col(node, DEFAULT_PRIMARY_KEY),
                Expr::col(edge, "target_id"),
            ),
        ),
    }
}

/// Join from source node to edge table, with entity type filter.
/// Unlike `source_join_cond`, this also filters on source_kind/target_kind
/// to prevent ID collisions across entity types.
fn source_join_cond_with_kind(node: &str, edge: &str, entity: &str, dir: Direction) -> Expr {
    let id_and_kind = |id_col, kind_col| {
        Expr::and(
            Expr::eq(
                Expr::col(node, DEFAULT_PRIMARY_KEY),
                Expr::col(edge, id_col),
            ),
            Expr::eq(Expr::col(edge, kind_col), Expr::string(entity)),
        )
    };

    match dir {
        Direction::Outgoing => id_and_kind("source_id", "source_kind"),
        Direction::Incoming => id_and_kind("target_id", "target_kind"),
        Direction::Both => Expr::or(
            id_and_kind("source_id", "source_kind"),
            id_and_kind("target_id", "target_kind"),
        ),
    }
}

/// Join from edge table to target node.
fn target_join_cond(edge: &str, node: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(
            Expr::col(edge, "target_id"),
            Expr::col(node, DEFAULT_PRIMARY_KEY),
        ),
        Direction::Incoming => Expr::eq(
            Expr::col(edge, "source_id"),
            Expr::col(node, DEFAULT_PRIMARY_KEY),
        ),
        Direction::Both => Expr::or(
            Expr::eq(
                Expr::col(edge, "target_id"),
                Expr::col(node, DEFAULT_PRIMARY_KEY),
            ),
            Expr::eq(
                Expr::col(edge, "source_id"),
                Expr::col(node, DEFAULT_PRIMARY_KEY),
            ),
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE Clause
// ─────────────────────────────────────────────────────────────────────────────

fn build_full_where(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
) -> Option<Expr> {
    let mut conds: Vec<Expr> = Vec::new();

    // Node conditions: IDs, ranges, filters
    for node in nodes {
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

    // Edge filters
    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            for (prop, filter) in &rel.filters {
                conds.push(filter_expr(alias, prop, filter));
            }
            // min_hops filter for multi-hop
            if rel.max_hops > 1 && rel.min_hops > 1 {
                conds.push(Expr::binary(
                    Op::Ge,
                    Expr::col(alias, "depth"),
                    Expr::int(rel.min_hops as i64),
                ));
            }
        }
    }

    Expr::and_all(conds.into_iter().map(Some))
}

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
    let s = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    Expr::binary(Op::Like, col, Expr::string(format!("{prefix}{s}{suffix}")))
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
    use crate::normalize;
    use crate::validate;
    use ontology::Ontology;

    fn test_ontology() -> Ontology {
        use ontology::DataType;
        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
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
        // Edge-centric: 6 edge columns + redaction ID/type pairs (no node properties)
        assert!(q.select.len() >= 6);
        assert!(
            q.select
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
        assert!(select_aliases.contains(&&"hop_e0_path".to_string()));
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
                    "traversal_path",
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

        assert_eq!(exprs.len(), 6);

        let aliases: Vec<_> = exprs.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"e0_path".to_string()));
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

        fn has_scan(t: &TableRef, tbl: &str) -> bool {
            match t {
                TableRef::Scan { table, .. } => table == tbl,
                TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
                _ => false,
            }
        }
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

        fn has_scan(t: &TableRef, tbl: &str) -> bool {
            match t {
                TableRef::Scan { table, .. } => table == tbl,
                TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
                _ => false,
            }
        }
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

        fn has_scan(t: &TableRef, tbl: &str) -> bool {
            match t {
                TableRef::Scan { table, .. } => table == tbl,
                TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
                _ => false,
            }
        }
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

        fn has_scan(t: &TableRef, tbl: &str) -> bool {
            match t {
                TableRef::Scan { table, .. } => table == tbl,
                TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
                _ => false,
            }
        }
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

        fn has_scan(t: &TableRef, tbl: &str) -> bool {
            match t {
                TableRef::Scan { table, .. } => table == tbl,
                TableRef::Join { left, right, .. } => has_scan(left, tbl) || has_scan(right, tbl),
                _ => false,
            }
        }
        // Multi-hop gets user table joined for ordering
        assert!(has_scan(&q.from, "gl_user"));
        assert_eq!(q.order_by.len(), 1);
    }
}

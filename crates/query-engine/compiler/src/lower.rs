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
/// Note: Ontology-dependent transformations (wildcard expansion, enum coercion)
/// are handled in normalize.rs. Lowering is purely mechanical.
pub fn lower(input: &Input) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal | QueryType::Search => lower_traversal(input),
        QueryType::Aggregation => lower_aggregation(input),
        QueryType::PathFinding => lower_path_finding(input),
        QueryType::Neighbors => lower_neighbors(input),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal & Search
// ─────────────────────────────────────────────────────────────────────────────

fn lower_traversal(input: &Input) -> Result<Node> {
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships)?;
    let where_clause = build_full_where(&input.nodes, &input.relationships, &edge_aliases);

    let mut select = Vec::new();
    for node in &input.nodes {
        if let Some(ColumnSelection::List(cols)) = &node.columns {
            for col in cols {
                select.push(SelectExpr::new(
                    Expr::col(&node.id, col),
                    format!("{}_{col}", node.id),
                ));
            }
        }
    }
    add_edge_columns(&mut select, &input.relationships, &edge_aliases);

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

/// Add edge columns to SELECT for each relationship.
fn add_edge_columns(
    select: &mut Vec<SelectExpr>,
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
) {
    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            select.extend(edge_select_exprs(alias));
            if rel.max_hops > 1 {
                select.push(edge_depth_select_expr(alias));
                select.push(edge_path_nodes_select_expr(alias));
            }
        }
    }
}

fn lower_aggregation(input: &Input) -> Result<Node> {
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships)?;
    let where_clause = build_full_where(&input.nodes, &input.relationships, &edge_aliases);

    // Collect unique group_by node IDs
    let group_by_node_ids: HashSet<_> = input
        .aggregations
        .iter()
        .filter_map(|agg| agg.group_by.clone())
        .collect();

    // Build SELECT and GROUP BY columns for group_by nodes
    // Note: Wildcards are expanded to List by normalize, so we only handle None/List
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

    // Add aggregation expressions
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
            vec![OrderExpr {
                expr: agg_expr(&input.aggregations[s.agg_index]),
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
            let mut ctes = vec![forward_cte];
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
///
/// Backward arm (depth=2, anchor=end):
///   SELECT e2.source_id AS end_id, ...
///   FROM gl_edge e1 JOIN gl_edge e2 ON e1.source_id = e2.target_id
///   WHERE e1.target_id IN (end_ids)
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

    let from = TableRef::join(
        JoinType::Inner,
        TableRef::scan(&center_table, &center_node.id),
        edge_table,
        join_cond,
    );

    let center_matches_source = Expr::and(
        Expr::eq(
            Expr::col(&center_node.id, DEFAULT_PRIMARY_KEY),
            Expr::col(edge_alias, "source_id"),
        ),
        Expr::eq(
            Expr::col(edge_alias, "source_kind"),
            Expr::string(center_entity),
        ),
    );

    let neighbor_id_expr = match neighbors_config.direction {
        Direction::Outgoing => Expr::col(edge_alias, "target_id"),
        Direction::Incoming => Expr::col(edge_alias, "source_id"),
        Direction::Both => Expr::func(
            "if",
            vec![
                center_matches_source.clone(),
                Expr::col(edge_alias, "target_id"),
                Expr::col(edge_alias, "source_id"),
            ],
        ),
    };

    let neighbor_type_expr = match neighbors_config.direction {
        Direction::Outgoing => Expr::col(edge_alias, "target_kind"),
        Direction::Incoming => Expr::col(edge_alias, "source_kind"),
        Direction::Both => Expr::func(
            "if",
            vec![
                center_matches_source.clone(),
                Expr::col(edge_alias, "target_kind"),
                Expr::col(edge_alias, "source_kind"),
            ],
        ),
    };

    let select = vec![
        SelectExpr::new(neighbor_id_expr, NEIGHBOR_ID_COLUMN),
        SelectExpr::new(neighbor_type_expr, NEIGHBOR_TYPE_COLUMN),
        SelectExpr::new(
            Expr::col(edge_alias, "relationship_kind"),
            RELATIONSHIP_TYPE_COLUMN,
        ),
        SelectExpr::new(
            match neighbors_config.direction {
                Direction::Outgoing => Expr::int(1),
                Direction::Incoming => Expr::int(0),
                Direction::Both => Expr::func(
                    "if",
                    vec![center_matches_source, Expr::int(1), Expr::int(0)],
                ),
            },
            NEIGHBOR_IS_OUTGOING_COLUMN,
        ),
    ];

    let where_clause = id_filter(&center_node.id, DEFAULT_PRIMARY_KEY, &center_node.node_ids);

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
// Multi-hop Union Building
// ─────────────────────────────────────────────────────────────────────────────

/// Build a UNION ALL subquery for multi-hop traversal (1 to max_hops).
fn build_hop_union_all(rel: &InputRelationship, alias: &str) -> TableRef {
    let rel_type_filter = type_filter(&rel.types);
    let queries = (1..=rel.max_hops)
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
        normalize::normalize(input, &ontology, &Default::default()).unwrap()
    }

    #[test]
    fn test_lower_simple_traversal() {
        let ontology = test_ontology();
        let note_defaults = ontology.get_node("Note").unwrap().default_columns.len();
        let user_defaults = ontology.get_node("User").unwrap().default_columns.len();

        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(q.limit, Some(25));
        let edge_columns = 6;
        assert_eq!(q.select.len(), note_defaults + user_defaults + edge_columns,);
    }

    #[test]
    fn test_lower_aggregation() {
        let input = validated_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "entity": "Note"}, {"id": "u", "entity": "User", "columns": ["username"]}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        );

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };

        // Non-recursive CTEs: forward + backward
        assert_eq!(q.ctes.len(), 2);
        assert_eq!(q.ctes[0].name, "forward");
        assert_eq!(q.ctes[1].name, "backward");
        assert!(!q.ctes[0].recursive);
        assert!(!q.ctes[1].recursive);
    }

    #[test]
    fn test_lower_with_filters() {
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn test_lower_multi_relationship() {
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        fn count_joins(t: &TableRef) -> usize {
            match t {
                TableRef::Join { left, right, .. } => 1 + count_joins(left) + count_joins(right),
                TableRef::Scan { .. } | TableRef::Union { .. } | TableRef::Subquery { .. } => 0,
            }
        }
        assert!(count_joins(&q.from) >= 4);
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);

        // Should have a union subquery for the multi-hop relationship
        assert_eq!(count_unions(&q.from), 1);
        // Should have a WHERE clause that includes depth >= 2
        assert!(
            q.where_clause.is_some(),
            "expected min_hops filter in WHERE"
        );
    }

    #[test]
    fn test_lower_mixed_single_and_multi_hop() {
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "p",
                "entity": "Project"
            },
            "limit": 50
        }"#,
        );

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };

        assert_eq!(q.select.len(), 2);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));
    }

    #[test]
    fn test_lower_with_wildcard_columns() {
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };

        // u_username, n_confidential + 6 edge columns
        assert_eq!(q.select.len(), 8);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"n_confidential".to_string()));
        // Edge columns
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
    }

    #[test]
    fn test_lower_no_columns_uses_defaults() {
        let input = validated_input(
            r#"{
            "query_type": "search",
            "node": {
                "id": "u",
                "entity": "User"
            },
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2}
        }"#,
        );

        let Node::Query(q) = lower(&input).unwrap() else {
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

        let input = Input {
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

        let Node::Query(q) = lower(&input).unwrap() else {
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

        let input = Input {
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

        let Node::Query(q) = lower(&input).unwrap() else {
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Should have edge columns for both relationships (e0 and e1)
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
        assert!(aliases.contains(&&"e1_type".to_string()));
        assert!(aliases.contains(&&"e1_src".to_string()));
    }

    #[test]
    fn test_type_filter_variants() {
        /// Check if an expression tree contains `relationship_kind = Param(value)`
        /// or `relationship_kind IN Param(value)`.
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

        fn extract_join_on(from: &TableRef) -> Option<&Expr> {
            match from {
                TableRef::Join { on, left, .. } => {
                    // Left-deep tree: recurse left to find innermost (edge) join
                    extract_join_on(left).or(Some(on))
                }
                _ => None,
            }
        }

        // Single type — join ON should contain relationship_kind filter
        let q = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":"AUTHORED","from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&q).unwrap() else {
            panic!()
        };
        let on = extract_join_on(&q.from).expect("expected join");
        assert!(has_type_filter(on), "expected type filter in join ON");

        // Multiple types — should use IN
        let q = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":["AUTHORED","CONTAINS"],"from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&q).unwrap() else {
            panic!()
        };
        let on = extract_join_on(&q.from).expect("expected join");
        assert!(has_type_filter(on), "expected type filter in join ON");

        // Wildcard — no type filter
        let q = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":"*","from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&q).unwrap() else {
            panic!()
        };
        let on = extract_join_on(&q.from).expect("expected join");
        assert!(!has_type_filter(on), "wildcard should not have type filter");
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
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        assert!(
            !table_ref_has_starts_with(&q.from),
            "single-hop join should not contain startsWith"
        );
    }

    #[test]
    fn no_starts_with_in_multi_hop_join() {
        let input = validated_input(
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

        let Node::Query(q) = lower(&input).unwrap() else {
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

        let input = Input {
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

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        assert!(
            !table_ref_has_starts_with(&q.from),
            "neighbors join should not contain startsWith"
        );
    }
}

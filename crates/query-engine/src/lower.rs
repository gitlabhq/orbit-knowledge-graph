//! Lower: Input → AST
//!
//! Transforms validated input into a SQL-oriented AST.

use crate::ast::{Cte, Expr, JoinType, Node, Op, OrderExpr, Query, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{
    ColumnSelection, Direction, FilterOp, Input, InputAggregation, InputFilter, InputNode,
    InputRelationship, OrderDirection, QueryType,
};
use crate::result_context::{NEIGHBOR_ID_COLUMN, NEIGHBOR_TYPE_COLUMN, RELATIONSHIP_TYPE_COLUMN};
use ontology::{EDGE_RESERVED_COLUMNS, EDGE_TABLE};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Maps edge column names to output alias suffixes.
/// Uses EDGE_RESERVED_COLUMNS order: relationship_kind, source_id, source_kind, target_id, target_kind
const EDGE_ALIAS_SUFFIXES: &[&str] = &["type", "src", "src_type", "dst", "dst_type"];

/// Generate SELECT expressions for all edge columns with the given table alias.
fn edge_select_exprs(alias: &str) -> Vec<SelectExpr> {
    EDGE_RESERVED_COLUMNS
        .iter()
        .zip(EDGE_ALIAS_SUFFIXES.iter())
        .map(|(col, suffix)| SelectExpr::new(Expr::col(alias, *col), format!("{alias}_{suffix}")))
        .collect()
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

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        order_by,
        limit: Some(input.limit),
        ..Default::default()
    })))
}

/// Add edge columns to SELECT for each relationship.
fn add_edge_columns(
    select: &mut Vec<SelectExpr>,
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
) {
    for (i, _rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            select.extend(edge_select_exprs(alias));
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

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        group_by,
        order_by,
        limit: Some(input.limit),
        ..Default::default()
    })))
}

fn agg_expr(agg: &InputAggregation) -> Expr {
    let arg = match (&agg.property, &agg.target) {
        (Some(prop), Some(target)) => Expr::col(target, prop),
        (None, Some(target)) => Expr::col(target, "id"),
        _ => Expr::lit(1),
    };
    Expr::func(agg.function.as_sql(), vec![arg])
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding (recursive CTE)
// ─────────────────────────────────────────────────────────────────────────────

fn lower_path_finding(input: &Input) -> Result<Node> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?;

    let start = find_node(&input.nodes, &path.from)?;
    let end = find_node(&input.nodes, &path.to)?;
    let start_table = resolve_table(start)?;
    let end_table = resolve_table(end)?;

    let start_entity = start
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("start node has no entity".into()))?;

    // Recursive CTE with full path/edges materialization.
    // Limited to 1000 paths to prevent memory explosion in dense graphs.
    let mut base = path_base_query(&start.node_ids, &start_table, &start.id, start_entity);
    let forward = path_recursive_branch(path.max_depth, true, &end.node_ids, &path.rel_types);
    let reverse = path_recursive_branch(path.max_depth, false, &end.node_ids, &path.rel_types);
    base.union_all = vec![forward, reverse];
    base.limit = Some(1000);

    let recursive_cte = Cte::recursive("paths", base);

    Ok(Node::Query(Box::new(Query {
        ctes: vec![recursive_cte],
        select: vec![
            SelectExpr::new(Expr::col("paths", "path"), "_gkg_path"),
            SelectExpr::new(Expr::col("paths", "edges"), "_gkg_edges"),
            SelectExpr::new(Expr::col("paths", "depth"), "depth"),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan("paths", "paths"),
            TableRef::scan(&end_table, &end.id),
            Expr::eq(Expr::col("paths", "node_id"), Expr::col(&end.id, "id")),
        ),
        where_clause: id_filter(&end.id, "id", &end.node_ids),
        order_by: vec![OrderExpr {
            expr: Expr::col("paths", "depth"),
            desc: false,
        }],
        limit: Some(input.limit),
        ..Default::default()
    })))
}

/// Base query for path finding with full materialization.
fn path_base_query(start_ids: &[i64], table: &str, start_alias: &str, start_entity: &str) -> Query {
    let start_id = Expr::col(start_alias, "id");
    let start_tuple = Expr::func("tuple", vec![start_id.clone(), Expr::lit(start_entity)]);

    Query {
        select: vec![
            SelectExpr::new(start_id.clone(), "node_id"),
            SelectExpr::new(Expr::func("array", vec![start_id]), "path_ids"),
            SelectExpr::new(Expr::func("array", vec![start_tuple]), "path"),
            SelectExpr::new(
                Expr::func(
                    "arrayResize",
                    vec![path_edge_tuple_template(), Expr::lit(0)],
                ),
                "edges",
            ),
            SelectExpr::new(Expr::lit(0), "depth"),
        ],
        from: TableRef::scan(table, start_alias),
        where_clause: id_filter(start_alias, "id", start_ids),
        ..Default::default()
    }
}

/// Empty edge tuple template for initializing edges array.
fn path_edge_tuple_template() -> Expr {
    Expr::func(
        "array",
        vec![Expr::func(
            "tuple",
            vec![
                Expr::lit(""),
                Expr::lit(0i64),
                Expr::lit(""),
                Expr::lit(0i64),
                Expr::lit(""),
            ],
        )],
    )
}

/// Recursive branch with full path/edges materialization.
/// Includes depth limit, cycle detection, early termination, and edge type filtering.
fn path_recursive_branch(
    max_depth: u32,
    join_on_source: bool,
    target_ids: &[i64],
    rel_types: &[String],
) -> Query {
    let (next_id_col, next_type_col) = if join_on_source {
        ("target_id", "target_kind")
    } else {
        ("source_id", "source_kind")
    };
    let join_col = if join_on_source {
        "source_id"
    } else {
        "target_id"
    };

    let next_node_id = Expr::col("e", next_id_col);
    let next_tuple = Expr::func(
        "tuple",
        vec![next_node_id.clone(), Expr::col("e", next_type_col)],
    );
    let edge_tuple = Expr::func(
        "tuple",
        vec![
            Expr::col("e", "relationship_kind"),
            Expr::col("e", "source_id"),
            Expr::col("e", "source_kind"),
            Expr::col("e", "target_id"),
            Expr::col("e", "target_kind"),
        ],
    );

    // depth < max_depth
    let depth_check = Expr::binary(Op::Lt, Expr::col("p", "depth"), Expr::lit(max_depth as i64));

    // cycle detection: NOT has(path_ids, next_node)
    let cycle_check = Expr::unary(
        Op::Not,
        Expr::func(
            "has",
            vec![Expr::col("p", "path_ids"), next_node_id.clone()],
        ),
    );

    // early termination: stop if target already in path
    let early_term = if target_ids.is_empty() {
        None
    } else {
        let target_array = Expr::func(
            "array",
            target_ids.iter().map(|id| Expr::lit(*id)).collect(),
        );
        Some(Expr::unary(
            Op::Not,
            Expr::func("has", vec![target_array, Expr::col("p", "node_id")]),
        ))
    };

    // relationship type filter
    let rel_filter = if rel_types.is_empty() {
        None
    } else if rel_types.len() == 1 {
        Some(Expr::eq(
            Expr::col("e", "relationship_kind"),
            Expr::lit(rel_types[0].clone()),
        ))
    } else {
        Some(Expr::binary(
            Op::In,
            Expr::col("e", "relationship_kind"),
            Expr::lit(serde_json::Value::Array(
                rel_types
                    .iter()
                    .map(|t| serde_json::Value::String(t.clone()))
                    .collect(),
            )),
        ))
    };

    // Combine all conditions
    let where_clause =
        Expr::and_all([Some(depth_check), Some(cycle_check), early_term, rel_filter]);

    Query {
        select: vec![
            SelectExpr::new(next_node_id, "node_id"),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "path_ids"),
                        Expr::func("array", vec![Expr::col("e", next_id_col)]),
                    ],
                ),
                "path_ids",
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "path"),
                        Expr::func("array", vec![next_tuple]),
                    ],
                ),
                "path",
            ),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "edges"),
                        Expr::func("array", vec![edge_tuple]),
                    ],
                ),
                "edges",
            ),
            SelectExpr::new(
                Expr::binary(Op::Add, Expr::col("p", "depth"), Expr::lit(1i64)),
                "depth",
            ),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan("paths", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::eq(Expr::col("p", "node_id"), Expr::col("e", join_col)),
        ),
        where_clause,
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

    let edge_table = edge_scan(edge_alias, &type_filter);

    let from = TableRef::join(
        JoinType::Inner,
        TableRef::scan(&center_table, &center_node.id),
        edge_table,
        source_join_cond_with_kind(
            &center_node.id,
            edge_alias,
            center_entity,
            neighbors_config.direction,
        ),
    );

    let neighbor_id_expr = match neighbors_config.direction {
        Direction::Outgoing => Expr::col(edge_alias, "target_id"),
        Direction::Incoming => Expr::col(edge_alias, "source_id"),
        Direction::Both => Expr::func(
            "if",
            vec![
                Expr::eq(
                    Expr::col(&center_node.id, "id"),
                    Expr::col(edge_alias, "source_id"),
                ),
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
                Expr::eq(
                    Expr::col(&center_node.id, "id"),
                    Expr::col(edge_alias, "source_id"),
                ),
                Expr::col(edge_alias, "target_kind"),
                Expr::col(edge_alias, "source_kind"),
            ],
        ),
    };

    let mut select = vec![
        SelectExpr::new(neighbor_id_expr, NEIGHBOR_ID_COLUMN),
        SelectExpr::new(neighbor_type_expr, NEIGHBOR_TYPE_COLUMN),
        SelectExpr::new(
            Expr::col(edge_alias, "relationship_kind"),
            RELATIONSHIP_TYPE_COLUMN,
        ),
    ];
    select.extend(edge_select_exprs(edge_alias));

    let where_clause = id_filter(&center_node.id, "id", &center_node.node_ids);

    let order_by = input.order_by.as_ref().map_or(vec![], |ob| {
        vec![OrderExpr {
            expr: Expr::col(&ob.node, &ob.property),
            desc: ob.direction == OrderDirection::Desc,
        }]
    });

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        order_by,
        limit: Some(input.limit),
        ..Default::default()
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Multi-hop Union Building
// ─────────────────────────────────────────────────────────────────────────────

/// Build a UNION ALL subquery for multi-hop traversal (1 to max_hops).
fn build_hop_union(rel: &InputRelationship, alias: &str) -> TableRef {
    let rel_type_filter = type_filter(&rel.types);
    let queries = (1..=rel.max_hops)
        .map(|depth| build_hop_arm(depth, &rel_type_filter, rel.direction))
        .collect();
    TableRef::union(queries, alias)
}

/// Build one arm of the union: a chain of edge joins for a specific depth.
fn build_hop_arm(depth: u32, type_filter: &Option<Vec<String>>, direction: Direction) -> Query {
    let (start_col, end_col) = direction.edge_columns();

    // Build chain: e1 -> e2 -> e3 -> ...
    let mut from = edge_scan("e1", type_filter);

    for i in 2..=depth {
        let prev = format!("e{}", i - 1);
        let curr = format!("e{i}");
        let join_cond = Expr::eq(Expr::col(&prev, end_col), Expr::col(&curr, start_col));
        from = TableRef::join(
            JoinType::Inner,
            from,
            edge_scan(&curr, type_filter),
            join_cond,
        );
    }

    Query {
        select: vec![
            SelectExpr::new(Expr::col("e1", start_col), "start_id"),
            SelectExpr::new(Expr::col(format!("e{depth}"), end_col), "end_id"),
            SelectExpr::new(Expr::lit(depth as i64), "depth"),
        ],
        from,
        ..Default::default()
    }
}

fn edge_scan(alias: &str, type_filter: &Option<Vec<String>>) -> TableRef {
    match type_filter {
        Some(types) => TableRef::scan_with_filter(EDGE_TABLE, alias, types.clone()),
        None => TableRef::scan(EDGE_TABLE, alias),
    }
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
    let start = rels.first().map_or(&nodes[0], |r| {
        nodes.iter().find(|n| n.id == r.from).unwrap_or(&nodes[0])
    });
    let start_table = resolve_table(start)?;
    let mut result = TableRef::scan(&start_table, &start.id);
    let mut edge_aliases = HashMap::new();

    for (i, rel) in rels.iter().enumerate() {
        let target = find_node(nodes, &rel.to)?;
        let target_table = resolve_table(target)?;

        if rel.max_hops > 1 {
            // Multi-hop: UNION subquery
            let alias = format!("hop_e{i}");
            edge_aliases.insert(i, alias.clone());

            let union = build_hop_union(rel, &alias);
            let (from_col, to_col) = rel.direction.union_columns();

            result = TableRef::join(
                JoinType::Inner,
                result,
                union,
                Expr::eq(Expr::col(&rel.from, "id"), Expr::col(&alias, from_col)),
            );
            result = TableRef::join(
                JoinType::Inner,
                result,
                TableRef::scan(&target_table, &rel.to),
                Expr::eq(Expr::col(&alias, to_col), Expr::col(&rel.to, "id")),
            );
        } else {
            // Single-hop: direct edge join
            let alias = format!("e{i}");
            edge_aliases.insert(i, alias.clone());

            let edge = edge_scan(&alias, &type_filter(&rel.types));
            result = TableRef::join(
                JoinType::Inner,
                result,
                edge,
                source_join_cond(&rel.from, &alias, rel.direction),
            );
            result = TableRef::join(
                JoinType::Inner,
                result,
                TableRef::scan(&target_table, &rel.to),
                target_join_cond(&alias, &rel.to, rel.direction),
            );
        }
    }

    Ok((result, edge_aliases))
}

/// Join from source node to edge table.
fn source_join_cond(node: &str, edge: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(Expr::col(node, "id"), Expr::col(edge, "source_id")),
        Direction::Incoming => Expr::eq(Expr::col(node, "id"), Expr::col(edge, "target_id")),
        Direction::Both => Expr::or(
            Expr::eq(Expr::col(node, "id"), Expr::col(edge, "source_id")),
            Expr::eq(Expr::col(node, "id"), Expr::col(edge, "target_id")),
        ),
    }
}

/// Join from source node to edge table, with entity type filter.
/// Unlike `source_join_cond`, this also filters on source_kind/target_kind
/// to prevent ID collisions across entity types.
fn source_join_cond_with_kind(node: &str, edge: &str, entity: &str, dir: Direction) -> Expr {
    let id_and_kind = |id_col, kind_col| {
        Expr::binary(
            Op::And,
            Expr::eq(Expr::col(node, "id"), Expr::col(edge, id_col)),
            Expr::eq(Expr::col(edge, kind_col), Expr::lit(entity)),
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
        Direction::Outgoing => Expr::eq(Expr::col(edge, "target_id"), Expr::col(node, "id")),
        Direction::Incoming => Expr::eq(Expr::col(edge, "source_id"), Expr::col(node, "id")),
        Direction::Both => Expr::or(
            Expr::eq(Expr::col(edge, "target_id"), Expr::col(node, "id")),
            Expr::eq(Expr::col(edge, "source_id"), Expr::col(node, "id")),
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
        conds.extend(id_filter(&node.id, "id", &node.node_ids));
        if let Some(r) = &node.id_range {
            conds.push(Expr::binary(
                Op::Ge,
                Expr::col(&node.id, "id"),
                Expr::lit(r.start),
            ));
            conds.push(Expr::binary(
                Op::Le,
                Expr::col(&node.id, "id"),
                Expr::lit(r.end),
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
                    Expr::lit(rel.min_hops as i64),
                ));
            }
        }
    }

    Expr::and_all(conds.into_iter().map(Some))
}

fn id_filter(table: &str, col: &str, ids: &[i64]) -> Option<Expr> {
    match ids.len() {
        0 => None,
        1 => Some(Expr::eq(Expr::col(table, col), Expr::lit(ids[0]))),
        _ => {
            let arr = Value::Array(ids.iter().map(|&id| Value::from(id)).collect());
            Some(Expr::binary(Op::In, Expr::col(table, col), Expr::lit(arr)))
        }
    }
}

fn filter_expr(table: &str, column: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(table, column);
    let val = || Expr::Literal(filter.value.clone().unwrap_or(Value::Null));

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
    Expr::binary(Op::Like, col, Expr::lit(format!("{prefix}{s}{suffix}")))
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
            .with_fields(
                "Note",
                [
                    ("confidential", DataType::Bool),
                    ("created_at", DataType::DateTime),
                ],
            )
            .with_fields("Project", [("name", DataType::String)])
    }

    fn validated_input(json: &str) -> Input {
        let ontology = test_ontology();
        let input = parse_input(json).unwrap();
        validate::validate(&input, &ontology).unwrap();
        normalize::normalize(input, &ontology)
    }

    #[test]
    fn test_lower_simple_traversal() {
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
        println!("{:?}", q);
        assert_eq!(q.limit, Some(25));
        // 2 node columns + 5 edge columns (type, src, src_type, dst, dst_type)
        assert_eq!(q.select.len(), 7);
    }

    #[test]
    fn test_lower_aggregation() {
        let input = validated_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "entity": "Note"}, {"id": "u", "entity": "User"}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#,
        );

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);
        assert!(!q.group_by.is_empty());
        assert!(q
            .select
            .iter()
            .any(|s| matches!(&s.expr, Expr::FuncCall { name, .. } if name == "COUNT")));
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

        // Should have group-by node columns: u_id, u_username, u_state
        assert!(aliases.contains(&&"u_id".to_string()));
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));

        // Should have aggregation result
        assert!(aliases.contains(&&"mr_count".to_string()));

        // Should NOT have target node id column (mr is aggregated, not grouped)
        assert!(!aliases.contains(&&"mr_id".to_string()));

        // GROUP BY should include all selected columns from group-by node
        assert_eq!(q.group_by.len(), 3); // id, username, state
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
        assert!(aliases.contains(&&"u_id".to_string()));
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"u_state".to_string()));
        assert!(aliases.contains(&&"u_created_at".to_string()));

        // Should have aggregation result
        assert!(aliases.contains(&&"note_count".to_string()));

        // GROUP BY should include all entity columns
        assert!(q.group_by.len() >= 4); // id + 3 fields from ontology
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

        // Single recursive CTE named "paths"
        assert_eq!(q.ctes.len(), 1);
        assert_eq!(q.ctes[0].name, "paths");
        assert!(q.ctes[0].recursive);
    }

    #[test]
    fn test_lower_with_filters() {
        let input = validated_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "entity": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            }],
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
                TableRef::Scan { .. } => 0,
                TableRef::Union { .. } => 0,
            }
        }
        assert!(count_joins(&q.from) >= 4);
    }

    /// Count union subqueries in a table reference tree
    fn count_unions(table_ref: &TableRef) -> usize {
        match table_ref {
            TableRef::Union { .. } => 1,
            TableRef::Join { left, right, .. } => count_unions(left) + count_unions(right),
            TableRef::Scan { .. } => 0,
        }
    }

    /// Find union with a specific alias
    fn find_union_alias(table_ref: &TableRef, alias: &str) -> bool {
        match table_ref {
            TableRef::Union { alias: a, .. } => a == alias,
            TableRef::Join { left, right, .. } => {
                find_union_alias(left, alias) || find_union_alias(right, alias)
            }
            TableRef::Scan { .. } => false,
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
        assert_eq!(q.select.len(), 1);
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
        assert_eq!(q.select.len(), 1);
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

        // Should have: u_id (always), u_username, u_state
        assert_eq!(q.select.len(), 3);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_id".to_string()));
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

        // Should have id + all fields from ontology (username, state, created_at)
        assert!(q.select.len() >= 4);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_id".to_string()));
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

        // Should have: u_id, u_username, n_id, n_confidential + 5 edge columns
        assert_eq!(q.select.len(), 9);

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"u_id".to_string()));
        assert!(aliases.contains(&&"u_username".to_string()));
        assert!(aliases.contains(&&"n_id".to_string()));
        assert!(aliases.contains(&&"n_confidential".to_string()));
        // Edge columns
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
    }

    #[test]
    fn test_lower_no_columns_only_id() {
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

        // No columns specified - should only have id
        assert_eq!(q.select.len(), 1);
        assert_eq!(q.select[0].alias, Some("u_id".to_string()));
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

        assert_eq!(exprs.len(), 5);

        let aliases: Vec<_> = exprs.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
        assert!(aliases.contains(&&"e0_src_type".to_string()));
        assert!(aliases.contains(&&"e0_dst".to_string()));
        assert!(aliases.contains(&&"e0_dst_type".to_string()));
    }

    #[test]
    fn test_path_finding_full_materialization() {
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

        // Final select should have _gkg_path, _gkg_edges, and depth
        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"_gkg_path".to_string()));
        assert!(aliases.contains(&&"_gkg_edges".to_string()));
        assert!(aliases.contains(&&"depth".to_string()));

        // CTE should have full columns including path and edges
        assert!(!q.ctes.is_empty());
        let cte_select: Vec<_> = q.ctes[0]
            .query
            .select
            .iter()
            .filter_map(|s| s.alias.as_ref())
            .collect();
        assert!(cte_select.contains(&&"node_id".to_string()));
        assert!(cte_select.contains(&&"path_ids".to_string()));
        assert!(cte_select.contains(&&"path".to_string()));
        assert!(cte_select.contains(&&"edges".to_string()));
        assert!(cte_select.contains(&&"depth".to_string()));

        // CTE should have a limit to prevent memory explosion
        assert_eq!(q.ctes[0].query.limit, Some(1000));
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
                columns: None,
                filters: std::collections::HashMap::new(),
                node_ids: vec![123],
                id_range: None,
                id_property: "id".to_string(),
            }],
            relationships: vec![],
            aggregations: vec![],
            path: None,
            neighbors: Some(InputNeighbors {
                node: "u".to_string(),
                direction: Direction::Outgoing,
                rel_types: vec![],
            }),
            limit: 10,
            order_by: None,
            aggregation_sort: None,
        };

        let Node::Query(q) = lower(&input).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Should have neighbor columns
        assert!(aliases.contains(&&"_gkg_neighbor_id".to_string()));
        assert!(aliases.contains(&&"_gkg_neighbor_type".to_string()));
        assert!(aliases.contains(&&"_gkg_relationship_type".to_string()));

        // Should have edge columns from edge_select_exprs
        assert!(aliases.contains(&&"e_type".to_string()));
        assert!(aliases.contains(&&"e_src".to_string()));
        assert!(aliases.contains(&&"e_src_type".to_string()));
        assert!(aliases.contains(&&"e_dst".to_string()));
        assert!(aliases.contains(&&"e_dst_type".to_string()));
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
        fn extract_edge_type_filter(from: &TableRef) -> Option<Vec<String>> {
            match from {
                TableRef::Scan { type_filter, .. } => type_filter.clone(),
                TableRef::Join { left, right, .. } => {
                    extract_edge_type_filter(left).or_else(|| extract_edge_type_filter(right))
                }
                TableRef::Union { .. } => None,
            }
        }

        // Single type
        let q = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":"AUTHORED","from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&q).unwrap() else {
            panic!()
        };
        assert_eq!(
            extract_edge_type_filter(&q.from),
            Some(vec!["AUTHORED".into()])
        );

        // Multiple types
        let q = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":["AUTHORED","CONTAINS"],"from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&q).unwrap() else {
            panic!()
        };
        assert_eq!(
            extract_edge_type_filter(&q.from),
            Some(vec!["AUTHORED".into(), "CONTAINS".into()])
        );

        // Wildcard - no filter
        let q = validated_input(
            r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User"},{"id":"n","entity":"Note"}],"relationships":[{"type":"*","from":"u","to":"n"}]}"#,
        );
        let Node::Query(q) = lower(&q).unwrap() else {
            panic!()
        };
        assert_eq!(extract_edge_type_filter(&q.from), None);
    }
}

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
use ontology::{Ontology, EDGE_RESERVED_COLUMNS, EDGE_TABLE};
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
pub fn lower(input: &Input, ontology: &Ontology) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal | QueryType::Search => lower_traversal(input, ontology),
        QueryType::Aggregation => lower_aggregation(input, ontology),
        QueryType::PathFinding => lower_path_finding(input, ontology),
        QueryType::Neighbors => lower_neighbors(input, ontology),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal & Search
// ─────────────────────────────────────────────────────────────────────────────

fn lower_traversal(input: &Input, ontology: &Ontology) -> Result<Node> {
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships, ontology)?;
    let where_clause = build_full_where(&input.nodes, &input.relationships, &edge_aliases);

    let mut select = build_select_columns(&input.nodes, ontology);
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

/// Build SELECT columns based on node column selections.
///
/// For each node:
/// - If `columns` is None: only return `{node_id}_id` (mandatory columns added by return.rs)
/// - If `columns` is `All` ("*"): return all columns from the ontology for that entity
/// - If `columns` is `List`: return the specified columns
///
/// Column aliases follow the pattern `{node_id}_{column_name}`.
fn build_select_columns(nodes: &[InputNode], ontology: &Ontology) -> Vec<SelectExpr> {
    let mut select = Vec::new();

    for node in nodes {
        match &node.columns {
            None => {
                // No columns specified - just add the id column (type added by return.rs)
                select.push(SelectExpr::new(
                    Expr::col(&node.id, "id"),
                    format!("{}_id", node.id),
                ));
            }
            Some(ColumnSelection::All) => {
                // Wildcard - get all columns from ontology
                if let Some(entity) = &node.entity {
                    // Always include id first
                    select.push(SelectExpr::new(
                        Expr::col(&node.id, "id"),
                        format!("{}_id", node.id),
                    ));

                    // Add all entity columns from ontology
                    if let Some(node_entity) = ontology.get_node(entity) {
                        for field in &node_entity.fields {
                            // Skip 'id' since we already added it
                            if field.name != "id" {
                                select.push(SelectExpr::new(
                                    Expr::col(&node.id, &field.name),
                                    format!("{}_{}", node.id, field.name),
                                ));
                            }
                        }
                    }
                }
            }
            Some(ColumnSelection::List(columns)) => {
                // Specific columns - always include id first if not in list
                let has_id = columns.iter().any(|c| c == "id");
                if !has_id {
                    select.push(SelectExpr::new(
                        Expr::col(&node.id, "id"),
                        format!("{}_id", node.id),
                    ));
                }

                // Add requested columns
                for col in columns {
                    select.push(SelectExpr::new(
                        Expr::col(&node.id, col),
                        format!("{}_{}", node.id, col),
                    ));
                }
            }
        }
    }

    select
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

fn lower_aggregation(input: &Input, ontology: &Ontology) -> Result<Node> {
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships, ontology)?;
    let where_clause = build_full_where(&input.nodes, &input.relationships, &edge_aliases);

    let mut select = Vec::new();
    let mut group_by = Vec::new();
    let mut seen_groups = HashSet::new();

    for agg in &input.aggregations {
        // Add GROUP BY column once per unique group
        if let Some(gb) = &agg.group_by {
            if seen_groups.insert(gb.clone()) {
                group_by.push(Expr::col(gb, "id"));
                select.push(SelectExpr::new(Expr::col(gb, "id"), format!("{gb}_id")));
            }
        }
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
// Path Finding (unrolled CTEs)
// ─────────────────────────────────────────────────────────────────────────────

fn lower_path_finding(input: &Input, ontology: &Ontology) -> Result<Node> {
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?;

    let start = find_node(&input.nodes, &path.from)?;
    let end = find_node(&input.nodes, &path.to)?;
    let start_table = resolve_table(ontology, start)?;
    let end_table = resolve_table(ontology, end)?;

    let start_entity = start
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering("start node has no entity".into()))?;

    // Build unrolled CTEs: d0 (base), d1, d2, ... d{max_depth}
    let mut ctes = Vec::with_capacity(path.max_depth as usize + 1);

    // d0: base query
    ctes.push(Cte::new(
        "d0",
        path_base_query(&start.node_ids, &start_table, &start.id, start_entity),
    ));

    // d1..dN: each references the previous depth
    for depth in 1..=path.max_depth {
        let prev_cte = format!("d{}", depth - 1);
        ctes.push(Cte::new(
            format!("d{depth}"),
            path_depth_query(&prev_cte, depth),
        ));
    }

    // Final query: UNION ALL of all depths, filter to target
    let union_queries: Vec<Query> = (0..=path.max_depth)
        .map(|d| path_select_from_cte(&format!("d{d}")))
        .collect();

    Ok(Node::Query(Box::new(Query {
        ctes,
        select: vec![
            SelectExpr::new(Expr::col("all_paths", "path"), "_gkg_path"),
            SelectExpr::new(Expr::col("all_paths", "edges"), "_gkg_edges"),
            SelectExpr::new(Expr::col("all_paths", "depth"), "depth"),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::union(union_queries, "all_paths"),
            TableRef::scan(&end_table, &end.id),
            Expr::eq(Expr::col("all_paths", "node_id"), Expr::col(&end.id, "id")),
        ),
        where_clause: id_filter(&end.id, "id", &end.node_ids),
        order_by: vec![OrderExpr {
            expr: Expr::col("all_paths", "depth"),
            desc: false,
        }],
        limit: Some(input.limit),
        ..Default::default()
    })))
}

/// Base query (d0): start the path with the first node.
fn path_base_query(start_ids: &[i64], table: &str, start_alias: &str, start_entity: &str) -> Query {
    let start_id = Expr::col(start_alias, "id");
    let start_tuple = Expr::func("tuple", vec![start_id.clone(), Expr::lit(start_entity)]);

    Query {
        select: vec![
            SelectExpr::new(start_id.clone(), "node_id"),
            SelectExpr::new(Expr::func("array", vec![start_id]), "path_ids"),
            SelectExpr::new(Expr::func("array", vec![start_tuple]), "path"),
            // Empty edges array - no edges at depth 0
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

/// Build a tuple template for path edges using EDGE_RESERVED_COLUMNS order.
fn path_edge_tuple_template() -> Expr {
    Expr::func(
        "array",
        vec![Expr::func(
            "tuple",
            vec![
                Expr::lit(""),   // relationship_kind
                Expr::lit(0i64), // source_id
                Expr::lit(""),   // source_kind
                Expr::lit(0i64), // target_id
                Expr::lit(""),   // target_kind
            ],
        )],
    )
}

/// Build the edge tuple for the current hop using EDGE_RESERVED_COLUMNS order.
fn path_edge_tuple() -> Expr {
    Expr::func(
        "tuple",
        EDGE_RESERVED_COLUMNS
            .iter()
            .map(|col| Expr::col("e", *col))
            .collect(),
    )
}

/// Depth N query: extend paths from the previous CTE by one hop.
fn path_depth_query(prev_cte: &str, depth: u32) -> Query {
    // next_node_id = if(p.node_id = e.source_id, e.target_id, e.source_id)
    let next_node_id = Expr::func(
        "if",
        vec![
            Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source_id")),
            Expr::col("e", "target_id"),
            Expr::col("e", "source_id"),
        ],
    );

    // next_node_type = if(p.node_id = e.source_id, e.target_kind, e.source_kind)
    let next_node_type = Expr::func(
        "if",
        vec![
            Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source_id")),
            Expr::col("e", "target_kind"),
            Expr::col("e", "source_kind"),
        ],
    );

    let next_tuple = Expr::func("tuple", vec![next_node_id.clone(), next_node_type]);

    Query {
        select: vec![
            SelectExpr::new(next_node_id.clone(), "node_id"),
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "path_ids"),
                        Expr::func("array", vec![next_node_id.clone()]),
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
            // Append current edge to edges array
            SelectExpr::new(
                Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "edges"),
                        Expr::func("array", vec![path_edge_tuple()]),
                    ],
                ),
                "edges",
            ),
            SelectExpr::new(Expr::lit(depth as i64), "depth"),
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan(prev_cte, "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            Expr::or(
                Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source_id")),
                Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "target_id")),
            ),
        ),
        // Cycle detection: next node must not already be in path
        where_clause: Some(Expr::unary(
            Op::Not,
            Expr::func("has", vec![Expr::col("p", "path_ids"), next_node_id]),
        )),
        ..Default::default()
    }
}

/// Select all columns from a CTE for use in UNION ALL.
fn path_select_from_cte(cte_name: &str) -> Query {
    Query {
        select: vec![
            SelectExpr::new(Expr::col(cte_name, "node_id"), "node_id"),
            SelectExpr::new(Expr::col(cte_name, "path_ids"), "path_ids"),
            SelectExpr::new(Expr::col(cte_name, "path"), "path"),
            SelectExpr::new(Expr::col(cte_name, "edges"), "edges"),
            SelectExpr::new(Expr::col(cte_name, "depth"), "depth"),
        ],
        from: TableRef::scan(cte_name, cte_name),
        ..Default::default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

fn lower_neighbors(input: &Input, ontology: &Ontology) -> Result<Node> {
    let neighbors_config = input
        .neighbors
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("neighbors config missing".into()))?;

    let center_node = find_node(&input.nodes, &neighbors_config.node)?;
    let center_table = resolve_table(ontology, center_node)?;

    let type_filter = if neighbors_config.rel_types.is_empty() {
        None
    } else {
        single_type_filter(&neighbors_config.rel_types)
    };

    let edge_alias = "e";

    let edge_table = edge_scan(edge_alias, &type_filter);

    let from = TableRef::join(
        JoinType::Inner,
        TableRef::scan(&center_table, &center_node.id),
        edge_table,
        source_join_cond(&center_node.id, edge_alias, neighbors_config.direction),
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
    let type_filter = single_type_filter(&rel.types);
    let queries = (1..=rel.max_hops)
        .map(|depth| build_hop_arm(depth, &type_filter, rel.direction))
        .collect();
    TableRef::union(queries, alias)
}

/// Build one arm of the union: a chain of edge joins for a specific depth.
fn build_hop_arm(depth: u32, type_filter: &Option<String>, direction: Direction) -> Query {
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

fn edge_scan(alias: &str, type_filter: &Option<String>) -> TableRef {
    match type_filter {
        Some(tf) => TableRef::scan_with_filter(EDGE_TABLE, alias, tf),
        None => TableRef::scan(EDGE_TABLE, alias),
    }
}

fn single_type_filter(types: &[String]) -> Option<String> {
    (types.len() == 1 && types[0] != "*").then(|| types[0].clone())
}

// ─────────────────────────────────────────────────────────────────────────────
// Join Building
// ─────────────────────────────────────────────────────────────────────────────

fn build_joins(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    ontology: &Ontology,
) -> Result<(TableRef, HashMap<usize, String>)> {
    let start = rels.first().map_or(&nodes[0], |r| {
        nodes.iter().find(|n| n.id == r.from).unwrap_or(&nodes[0])
    });
    let start_table = resolve_table(ontology, start)?;
    let mut result = TableRef::scan(&start_table, &start.id);
    let mut edge_aliases = HashMap::new();

    for (i, rel) in rels.iter().enumerate() {
        let target = find_node(nodes, &rel.to)?;
        let target_table = resolve_table(ontology, target)?;

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

            let edge = edge_scan(&alias, &single_type_filter(&rel.types));
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

fn resolve_table(ontology: &Ontology, node: &InputNode) -> Result<String> {
    let entity = node
        .entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no entity", node.id)))?;
    Ok(ontology.table_name(entity)?)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;
    use crate::validate;

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
        let input = parse_input(json).unwrap();
        validate::validate(&input, &test_ontology()).unwrap();
        input
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };
        println!("{:?}", q);
        // Should have d0, d1, d2, d3 CTEs (base + 3 depth levels)
        assert_eq!(q.ctes.len(), 4);
        assert_eq!(q.ctes[0].name, "d0");
        assert_eq!(q.ctes[3].name, "d3");
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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
    fn test_path_finding_includes_edges_column() {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };

        // Final select should have _gkg_path, _gkg_edges, depth
        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();
        assert!(aliases.contains(&&"_gkg_path".to_string()));
        assert!(aliases.contains(&&"_gkg_edges".to_string()));
        assert!(aliases.contains(&&"depth".to_string()));

        // CTEs should have edges column
        assert!(!q.ctes.is_empty());
        let d0_select: Vec<_> = q.ctes[0]
            .query
            .select
            .iter()
            .filter_map(|s| s.alias.as_ref())
            .collect();
        assert!(d0_select.contains(&&"edges".to_string()));
    }

    #[test]
    fn test_neighbors_includes_edge_columns() {
        use crate::input::{Direction, InputNeighbors};

        let input = Input {
            query_type: QueryType::Neighbors,
            nodes: vec![InputNode {
                id: "u".to_string(),
                entity: Some("User".to_string()),
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
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

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };

        let aliases: Vec<_> = q.select.iter().filter_map(|s| s.alias.as_ref()).collect();

        // Should have edge columns for both relationships (e0 and e1)
        assert!(aliases.contains(&&"e0_type".to_string()));
        assert!(aliases.contains(&&"e0_src".to_string()));
        assert!(aliases.contains(&&"e1_type".to_string()));
        assert!(aliases.contains(&&"e1_src".to_string()));
    }
}

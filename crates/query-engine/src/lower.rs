//! Lower: Input → AST
//!
//! Converts LLM JSON input into a SQL-oriented AST.

use crate::ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{
    Direction, FilterOp, Input, InputAggregation, InputFilter, InputNode, InputRelationship,
    OrderDirection, QueryType,
};
use ontology::{Ontology, EDGE_TABLE};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

// ─────────────────────────────────────────────────────────────────────────────
// Error helpers
// ─────────────────────────────────────────────────────────────────────────────

fn err(msg: impl Into<String>) -> QueryError {
    QueryError::Lowering(msg.into())
}

fn missing_node(id: &str, context: &str) -> QueryError {
    err(format!(
        "{context} references node \"{id}\" which is not defined"
    ))
}

fn needs_entity(id: &str) -> QueryError {
    err(format!(
        "node \"{id}\" requires an entity type to determine which table to query"
    ))
}

// ─────────────────────────────────────────────────────────────────────────────
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Lower parsed input into an AST node.
pub fn lower(input: &Input, ontology: &Ontology) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal | QueryType::Pattern => lower_traversal(input, ontology),
        QueryType::Aggregation => lower_aggregation(input, ontology),
        QueryType::PathFinding => lower_path_finding(input, ontology),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal queries
// ─────────────────────────────────────────────────────────────────────────────

fn lower_traversal(input: &Input, ontology: &Ontology) -> Result<Node> {
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships, ontology)?;
    let where_clause = build_where(&input.nodes, &input.relationships, &edge_aliases, ontology)?;
    let order_by = build_order_by(&input.order_by, &input.nodes, ontology)?;

    let select = input
        .nodes
        .iter()
        .map(|n| SelectExpr {
            expr: Expr::col(&n.id, "id"),
            alias: Some(format!("{}_id", n.id)),
        })
        .collect();

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        group_by: vec![],
        order_by,
        limit: Some(input.limit),
    })))
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation queries
// ─────────────────────────────────────────────────────────────────────────────

fn lower_aggregation(input: &Input, ontology: &Ontology) -> Result<Node> {
    let (from, edge_aliases) = build_joins(&input.nodes, &input.relationships, ontology)?;
    let where_clause = build_where(&input.nodes, &input.relationships, &edge_aliases, ontology)?;

    let mut select = Vec::new();
    let mut group_by = Vec::new();
    let mut grouped = HashSet::new();

    for agg in &input.aggregations {
        // Validate property against ontology
        if let (Some(prop), Some(target)) = (&agg.property, &agg.target) {
            if let Some(entity) = find_node_entity(&input.nodes, target) {
                ontology.validate_field(&entity, prop)?;
            }
        }

        // Add GROUP BY column (deduplicated)
        if let Some(gb) = &agg.group_by {
            if grouped.insert(gb.clone()) {
                group_by.push(Expr::col(gb, "id"));
                select.push(SelectExpr {
                    expr: Expr::col(gb, "id"),
                    alias: Some(format!("{gb}_id")),
                });
            }
        }

        // Add aggregate expression
        select.push(SelectExpr {
            expr: build_agg_expr(agg),
            alias: Some(
                agg.alias
                    .clone()
                    .unwrap_or_else(|| agg.function.as_sql().to_lowercase()),
            ),
        });
    }

    let order_by = build_agg_order_by(&input.aggregation_sort, &input.aggregations);

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        group_by,
        order_by,
        limit: Some(input.limit),
    })))
}

fn build_agg_expr(agg: &InputAggregation) -> Expr {
    let arg = match (&agg.property, &agg.target) {
        (Some(prop), Some(target)) => Expr::col(target, prop),
        (None, Some(target)) => Expr::col(target, "id"),
        _ => Expr::lit(1),
    };
    Expr::func(agg.function.as_sql(), vec![arg])
}

fn build_agg_order_by(
    sort: &Option<crate::input::InputAggSort>,
    aggs: &[InputAggregation],
) -> Vec<OrderExpr> {
    let Some(s) = sort else { return vec![] };
    if s.agg_index >= aggs.len() {
        return vec![];
    }
    vec![OrderExpr {
        expr: build_agg_expr(&aggs[s.agg_index]),
        desc: s.direction == OrderDirection::Desc,
    }]
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding queries (recursive CTE)
// ─────────────────────────────────────────────────────────────────────────────

fn lower_path_finding(input: &Input, ontology: &Ontology) -> Result<Node> {
    let path = input.path.as_ref().ok_or_else(|| {
        err("path_finding query requires a 'path' configuration with 'from' and 'to' nodes")
    })?;

    let start = find_node(&input.nodes, &path.from)
        .ok_or_else(|| missing_node(&path.from, "path 'from'"))?;
    let end =
        find_node(&input.nodes, &path.to).ok_or_else(|| missing_node(&path.to, "path 'to'"))?;

    let start_entity = start
        .entity
        .as_ref()
        .ok_or_else(|| needs_entity(&start.id))?;
    let end_entity = end.entity.as_ref().ok_or_else(|| needs_entity(&end.id))?;

    // Get table names from ontology (validates that entity types exist)
    let start_table = ontology.table_name(start_entity)?;
    let end_table = ontology.table_name(end_entity)?;

    Ok(Node::RecursiveCte(Box::new(RecursiveCte {
        name: "path_cte".into(),
        base: build_path_base(&start.node_ids, &start_table),
        recursive: build_path_recursive(&end_table, path.max_depth),
        max_depth: path.max_depth,
        final_query: build_path_final(&end.node_ids, input.limit),
    })))
}

fn build_path_base(start_ids: &[i64], table: &str) -> Query {
    Query {
        select: vec![
            SelectExpr {
                expr: Expr::col("n", "id"),
                alias: Some("node_id".into()),
            },
            SelectExpr {
                expr: Expr::func("ARRAY", vec![Expr::col("n", "id")]),
                alias: Some("path".into()),
            },
            SelectExpr {
                expr: Expr::lit(0),
                alias: Some("depth".into()),
            },
        ],
        // Node tables are entity-specific, no type filter needed
        from: TableRef::scan(table, "n"),
        where_clause: node_ids_condition("n", "id", start_ids),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    }
}

fn build_path_recursive(table: &str, max_depth: u32) -> Query {
    Query {
        select: vec![
            SelectExpr {
                expr: Expr::col("n", "id"),
                alias: Some("node_id".into()),
            },
            SelectExpr {
                expr: Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "path"),
                        Expr::func("ARRAY", vec![Expr::col("n", "id")]),
                    ],
                ),
                alias: Some("path".into()),
            },
            SelectExpr {
                expr: Expr::binary(Op::Add, Expr::col("p", "depth"), Expr::lit(1)),
                alias: Some("depth".into()),
            },
        ],
        from: TableRef::join(
            JoinType::Inner,
            TableRef::join(
                JoinType::Inner,
                TableRef::scan("path_cte", "p"),
                TableRef::scan(EDGE_TABLE, "e"),
                // Edge table uses "source" column
                Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source")),
            ),
            // Node tables are entity-specific, no type filter needed
            TableRef::scan(table, "n"),
            // Edge table uses "target" column
            Expr::eq(Expr::col("e", "target"), Expr::col("n", "id")),
        ),
        where_clause: Expr::and_all([
            Some(Expr::binary(
                Op::Lt,
                Expr::col("p", "depth"),
                Expr::lit(max_depth as i64),
            )),
            Some(Expr::unary(
                Op::Not,
                Expr::func("has", vec![Expr::col("p", "path"), Expr::col("n", "id")]),
            )),
        ]),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    }
}

fn build_path_final(end_ids: &[i64], limit: u32) -> Query {
    Query {
        select: vec![
            SelectExpr {
                expr: Expr::col("path_cte", "path"),
                alias: Some("path".into()),
            },
            SelectExpr {
                expr: Expr::col("path_cte", "depth"),
                alias: Some("depth".into()),
            },
        ],
        from: TableRef::scan("path_cte", "path_cte"),
        where_clause: node_ids_condition("path_cte", "node_id", end_ids),
        group_by: vec![],
        order_by: vec![OrderExpr {
            expr: Expr::col("path_cte", "depth"),
            desc: false,
        }],
        limit: Some(limit),
    }
}

/// Build an IN or = condition for node IDs.
fn node_ids_condition(table: &str, column: &str, ids: &[i64]) -> Option<Expr> {
    match ids.len() {
        0 => None,
        1 => Some(Expr::eq(Expr::col(table, column), Expr::lit(ids[0]))),
        _ => {
            let arr = Value::Array(ids.iter().map(|&id| Value::from(id)).collect());
            Some(Expr::binary(
                Op::In,
                Expr::col(table, column),
                Expr::lit(arr),
            ))
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Join building (FROM clause)
// ─────────────────────────────────────────────────────────────────────────────

fn build_joins(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    ontology: &Ontology,
) -> Result<(TableRef, HashMap<usize, String>)> {
    if nodes.is_empty() {
        return Err(err("at least one node required"));
    }

    // Start from the "from" node of first relationship, or first node
    let start = match rels.first() {
        Some(rel) => {
            find_node(nodes, &rel.from).ok_or_else(|| missing_node(&rel.from, "relationship"))?
        }
        None => &nodes[0],
    };

    let start_entity = start
        .entity
        .as_ref()
        .ok_or_else(|| needs_entity(&start.id))?;
    let start_table = ontology.table_name(start_entity)?;
    // Node tables are entity-specific, no type filter needed
    let mut result = TableRef::scan(&start_table, &start.id);

    let mut edge_aliases = HashMap::new();

    for (i, rel) in rels.iter().enumerate() {
        let edge_alias = format!("e{i}");
        edge_aliases.insert(i, edge_alias.clone());

        // Validate and get type filter
        let type_filter = validate_rel_types(&rel.types, ontology)?;

        // Join edge table
        let edge_cond = edge_join_condition(&rel.from, &edge_alias, rel.direction);
        let edge_table = match &type_filter {
            Some(tf) => TableRef::scan_with_filter(EDGE_TABLE, &edge_alias, tf),
            None => TableRef::scan(EDGE_TABLE, &edge_alias),
        };
        result = TableRef::join(JoinType::Inner, result, edge_table, edge_cond);

        // Join target node (no type filter needed - table is entity-specific)
        let target_entity =
            find_node_entity(nodes, &rel.to).ok_or_else(|| needs_entity(&rel.to))?;
        let target_table = ontology.table_name(&target_entity)?;
        let target_cond = target_join_condition(&edge_alias, &rel.to, rel.direction);
        let target_ref = TableRef::scan(&target_table, &rel.to);
        result = TableRef::join(JoinType::Inner, result, target_ref, target_cond);
    }

    Ok((result, edge_aliases))
}

fn validate_rel_types(types: &[String], ontology: &Ontology) -> Result<Option<String>> {
    for t in types {
        if t != "*" {
            ontology.validate_type(t)?;
        }
    }
    // Only return a filter if there's exactly one non-wildcard type
    if types.len() == 1 && types[0] != "*" {
        Ok(Some(types[0].clone()))
    } else {
        Ok(None)
    }
}

fn edge_join_condition(from_node: &str, edge_alias: &str, dir: Direction) -> Expr {
    // Edge table columns: source (from), target (to), relationship_kind (type)
    match dir {
        Direction::Outgoing => {
            Expr::eq(Expr::col(from_node, "id"), Expr::col(edge_alias, "source"))
        }
        Direction::Incoming => {
            Expr::eq(Expr::col(from_node, "id"), Expr::col(edge_alias, "target"))
        }
        Direction::Both => Expr::or_all([
            Some(Expr::eq(
                Expr::col(from_node, "id"),
                Expr::col(edge_alias, "source"),
            )),
            Some(Expr::eq(
                Expr::col(from_node, "id"),
                Expr::col(edge_alias, "target"),
            )),
        ])
        .unwrap(),
    }
}

fn target_join_condition(edge_alias: &str, to_node: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(Expr::col(edge_alias, "target"), Expr::col(to_node, "id")),
        Direction::Incoming => Expr::eq(Expr::col(edge_alias, "source"), Expr::col(to_node, "id")),
        Direction::Both => Expr::or_all([
            Some(Expr::eq(
                Expr::col(edge_alias, "target"),
                Expr::col(to_node, "id"),
            )),
            Some(Expr::eq(
                Expr::col(edge_alias, "source"),
                Expr::col(to_node, "id"),
            )),
        ])
        .unwrap(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE clause building
// ─────────────────────────────────────────────────────────────────────────────

fn build_where(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
    ontology: &Ontology,
) -> Result<Option<Expr>> {
    let mut conds = Vec::new();

    for node in nodes {
        // Node ID filters
        conds.extend(build_id_conditions(
            &node.id,
            &node.node_ids,
            &node.id_range,
        ));

        // Property filters
        for (prop, filter) in &node.filters {
            if let Some(entity) = &node.entity {
                ontology.validate_field(entity, prop)?;
            }
            conds.push(filter_to_expr(&node.id, prop, filter));
        }
    }

    // Edge filters
    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            for (prop, filter) in &rel.filters {
                conds.push(filter_to_expr(alias, prop, filter));
            }
        }
    }

    Ok(Expr::and_all(conds.into_iter().map(Some)))
}

fn build_id_conditions(
    table: &str,
    ids: &[i64],
    range: &Option<crate::input::InputIdRange>,
) -> Vec<Expr> {
    let mut conds = Vec::new();

    match ids.len() {
        0 => {}
        1 => conds.push(Expr::eq(Expr::col(table, "id"), Expr::lit(ids[0]))),
        _ => {
            let arr = Value::Array(ids.iter().map(|&id| Value::from(id)).collect());
            conds.push(Expr::binary(Op::In, Expr::col(table, "id"), Expr::lit(arr)));
        }
    }

    if let Some(r) = range {
        conds.push(Expr::binary(
            Op::Ge,
            Expr::col(table, "id"),
            Expr::lit(r.start),
        ));
        conds.push(Expr::binary(
            Op::Le,
            Expr::col(table, "id"),
            Expr::lit(r.end),
        ));
    }

    conds
}

fn filter_to_expr(table: &str, column: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(table, column);
    let val = || Expr::Literal(filter.value.clone().unwrap_or(Value::Null));

    match filter.op {
        None | Some(FilterOp::Eq) => Expr::eq(col, val()),
        Some(FilterOp::Gt) => Expr::binary(Op::Gt, col, val()),
        Some(FilterOp::Lt) => Expr::binary(Op::Lt, col, val()),
        Some(FilterOp::Gte) => Expr::binary(Op::Ge, col, val()),
        Some(FilterOp::Lte) => Expr::binary(Op::Le, col, val()),
        Some(FilterOp::In) => Expr::binary(Op::In, col, val()),
        Some(FilterOp::Contains) => like_expr(col, filter, "%", "%"),
        Some(FilterOp::StartsWith) => like_expr(col, filter, "", "%"),
        Some(FilterOp::EndsWith) => like_expr(col, filter, "%", ""),
        Some(FilterOp::IsNull) => Expr::unary(Op::IsNull, col),
        Some(FilterOp::IsNotNull) => Expr::unary(Op::IsNotNull, col),
    }
}

fn like_expr(col: Expr, filter: &InputFilter, prefix: &str, suffix: &str) -> Expr {
    let s = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
    Expr::binary(Op::Like, col, Expr::lit(format!("{prefix}{s}{suffix}")))
}

// ─────────────────────────────────────────────────────────────────────────────
// ORDER BY building
// ─────────────────────────────────────────────────────────────────────────────

fn build_order_by(
    order_by: &Option<crate::input::InputOrderBy>,
    nodes: &[InputNode],
    ontology: &Ontology,
) -> Result<Vec<OrderExpr>> {
    let Some(ob) = order_by else {
        return Ok(vec![]);
    };

    if let Some(entity) = find_node_entity(nodes, &ob.node) {
        ontology.validate_field(&entity, &ob.property)?;
    }

    Ok(vec![OrderExpr {
        expr: Expr::col(&ob.node, &ob.property),
        desc: ob.direction == OrderDirection::Desc,
    }])
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn find_node<'a>(nodes: &'a [InputNode], id: &str) -> Option<&'a InputNode> {
    nodes.iter().find(|n| n.id == id)
}

fn find_node_entity(nodes: &[InputNode], id: &str) -> Option<String> {
    find_node(nodes, id).and_then(|n| n.entity.clone())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;

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

    #[test]
    fn test_lower_simple_traversal() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "entity": "Note"},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25
        }"#,
        )
        .unwrap();

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };
        assert_eq!(q.limit, Some(25));
        assert_eq!(q.select.len(), 2);
    }

    #[test]
    fn test_lower_aggregation() {
        let input = parse_input(r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "entity": "Note"}, {"id": "u", "entity": "User"}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "limit": 10
        }"#).unwrap();

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };
        assert!(!q.group_by.is_empty());
        assert!(q
            .select
            .iter()
            .any(|s| matches!(&s.expr, Expr::FuncCall { name, .. } if name == "COUNT")));
    }

    #[test]
    fn test_lower_path_finding() {
        let input = parse_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "Project", "node_ids": [100]},
                {"id": "end", "entity": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        )
        .unwrap();

        let Node::RecursiveCte(cte) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected RecursiveCte");
        };
        assert_eq!(cte.max_depth, 3);
        assert_eq!(cte.name, "path_cte");
    }

    #[test]
    fn test_lower_with_filters() {
        let input = parse_input(
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
        )
        .unwrap();

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };
        assert!(q.where_clause.is_some());
    }

    #[test]
    fn test_lower_multi_hop() {
        let input = parse_input(
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
        )
        .unwrap();

        let Node::Query(q) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected Query");
        };

        fn count_joins(t: &TableRef) -> usize {
            match t {
                TableRef::Join { left, right, .. } => 1 + count_joins(left) + count_joins(right),
                TableRef::Scan { .. } => 0,
            }
        }
        assert!(count_joins(&q.from) >= 4);
    }
}

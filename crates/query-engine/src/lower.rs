//! Lower: Input → AST
//!
//! Transformation from validated input to SQL-oriented AST.
//! Returns errors for missing nodes, entities, or ontology lookups.

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
// Public entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Lower validated input into an AST node.
///
/// Returns errors if nodes, entities, or ontology lookups fail.
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
    let where_clause = build_where(&input.nodes, &input.relationships, &edge_aliases);
    let order_by = build_order_by(&input.order_by);

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
    let where_clause = build_where(&input.nodes, &input.relationships, &edge_aliases);

    let mut select = Vec::new();
    let mut group_by = Vec::new();
    let mut grouped = HashSet::new();

    for agg in &input.aggregations {
        if let Some(gb) = &agg.group_by {
            if grouped.insert(gb.clone()) {
                group_by.push(Expr::col(gb, "id"));
                select.push(SelectExpr {
                    expr: Expr::col(gb, "id"),
                    alias: Some(format!("{gb}_id")),
                });
            }
        }

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
    let path = input
        .path
        .as_ref()
        .ok_or_else(|| QueryError::Lowering("path config missing".into()))?;
    let start = node(&input.nodes, &path.from)?;
    let end = node(&input.nodes, &path.to)?;

    let start_table = table_name(ontology, entity(start)?)?;
    let end_table = table_name(ontology, entity(end)?)?;

    Ok(Node::RecursiveCte(Box::new(RecursiveCte {
        name: "path_cte".into(),
        base: build_path_base(&start.node_ids, &start_table),
        recursive: build_path_recursive(path.max_depth),
        max_depth: path.max_depth,
        final_query: build_path_final(&end.node_ids, &end_table, input.limit),
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
                expr: Expr::func("array", vec![Expr::col("n", "id")]),
                alias: Some("path".into()),
            },
            SelectExpr {
                expr: Expr::lit(0),
                alias: Some("depth".into()),
            },
        ],
        from: TableRef::scan(table, "n"),
        where_clause: node_ids_condition("n", "id", start_ids),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    }
}

/// Build recursive part that traverses through ANY entity type in BOTH directions.
/// Uses edges directly without joining to specific entity tables,
/// enabling multi-hop paths across different entity types.
/// Follows edges bidirectionally: both source->target and target->source.
fn build_path_recursive(max_depth: u32) -> Query {
    // We use a subquery to UNION both edge directions, then join with path_cte
    // next_node = e.target when p.node_id = e.source (forward)
    // next_node = e.source when p.node_id = e.target (backward)
    //
    // Using CASE expression: if(p.node_id = e.source, e.target, e.source)
    let next_node = Expr::func(
        "if",
        vec![
            Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "source_id")),
            Expr::col("e", "target_id"),
            Expr::col("e", "source_id"),
        ],
    );

    Query {
        select: vec![
            SelectExpr {
                expr: next_node.clone(),
                alias: Some("node_id".into()),
            },
            SelectExpr {
                expr: Expr::func(
                    "arrayConcat",
                    vec![
                        Expr::col("p", "path"),
                        Expr::func("array", vec![next_node.clone()]),
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
            TableRef::scan("path_cte", "p"),
            TableRef::scan(EDGE_TABLE, "e"),
            // Join on either direction: node_id matches source OR target
            Expr::or_all([
                Some(Expr::eq(
                    Expr::col("p", "node_id"),
                    Expr::col("e", "source_id"),
                )),
                Some(Expr::eq(
                    Expr::col("p", "node_id"),
                    Expr::col("e", "target_id"),
                )),
            ])
            .expect("or_all has elements"),
        ),
        where_clause: Expr::and_all([
            Some(Expr::binary(
                Op::Lt,
                Expr::col("p", "depth"),
                Expr::lit(max_depth as i64),
            )),
            Some(Expr::unary(
                Op::Not,
                Expr::func("has", vec![Expr::col("p", "path"), next_node]),
            )),
        ]),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    }
}

/// Build final query that filters paths ending at the target entity.
/// Joins with the end entity table to ensure the final node is of the correct type
/// and to apply security filters.
fn build_path_final(end_ids: &[i64], end_table: &str, limit: u32) -> Query {
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
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan("path_cte", "path_cte"),
            TableRef::scan(end_table, "end_node"),
            Expr::eq(
                Expr::col("path_cte", "node_id"),
                Expr::col("end_node", "id"),
            ),
        ),
        where_clause: node_ids_condition("end_node", "id", end_ids),
        group_by: vec![],
        order_by: vec![OrderExpr {
            expr: Expr::col("path_cte", "depth"),
            desc: false,
        }],
        limit: Some(limit),
    }
}

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
    let start = match rels.first() {
        Some(rel) => node(nodes, &rel.from)?,
        None => &nodes[0],
    };

    let start_table = table_name(ontology, entity(start)?)?;
    let mut result = TableRef::scan(&start_table, &start.id);
    let mut edge_aliases = HashMap::new();

    for (i, rel) in rels.iter().enumerate() {
        let edge_alias = format!("e{i}");
        edge_aliases.insert(i, edge_alias.clone());

        let type_filter = rel_type_filter(&rel.types);

        let edge_cond = edge_join_condition(&rel.from, &edge_alias, rel.direction);
        let edge_table = match &type_filter {
            Some(tf) => TableRef::scan_with_filter(EDGE_TABLE, &edge_alias, tf),
            None => TableRef::scan(EDGE_TABLE, &edge_alias),
        };
        result = TableRef::join(JoinType::Inner, result, edge_table, edge_cond);

        let target = node(nodes, &rel.to)?;
        let target_table = table_name(ontology, entity(target)?)?;
        let target_cond = target_join_condition(&edge_alias, &rel.to, rel.direction);
        result = TableRef::join(
            JoinType::Inner,
            result,
            TableRef::scan(&target_table, &rel.to),
            target_cond,
        );
    }

    Ok((result, edge_aliases))
}

fn rel_type_filter(types: &[String]) -> Option<String> {
    if types.len() == 1 && types[0] != "*" {
        Some(types[0].clone())
    } else {
        None
    }
}

fn edge_join_condition(from_node: &str, edge_alias: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => Expr::eq(
            Expr::col(from_node, "id"),
            Expr::col(edge_alias, "source_id"),
        ),
        Direction::Incoming => Expr::eq(
            Expr::col(from_node, "id"),
            Expr::col(edge_alias, "target_id"),
        ),
        Direction::Both => Expr::or_all([
            Some(Expr::eq(
                Expr::col(from_node, "id"),
                Expr::col(edge_alias, "source_id"),
            )),
            Some(Expr::eq(
                Expr::col(from_node, "id"),
                Expr::col(edge_alias, "target_id"),
            )),
        ])
        .expect("validated: or_all has elements"),
    }
}

fn target_join_condition(edge_alias: &str, to_node: &str, dir: Direction) -> Expr {
    match dir {
        Direction::Outgoing => {
            Expr::eq(Expr::col(edge_alias, "target_id"), Expr::col(to_node, "id"))
        }
        Direction::Incoming => {
            Expr::eq(Expr::col(edge_alias, "source_id"), Expr::col(to_node, "id"))
        }
        Direction::Both => Expr::or_all([
            Some(Expr::eq(
                Expr::col(edge_alias, "target_id"),
                Expr::col(to_node, "id"),
            )),
            Some(Expr::eq(
                Expr::col(edge_alias, "source_id"),
                Expr::col(to_node, "id"),
            )),
        ])
        .expect("validated: or_all has elements"),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE clause building
// ─────────────────────────────────────────────────────────────────────────────

fn build_where(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
) -> Option<Expr> {
    let mut conds = Vec::new();

    for node in nodes {
        conds.extend(build_id_conditions(
            &node.id,
            &node.node_ids,
            &node.id_range,
        ));
        for (prop, filter) in &node.filters {
            conds.push(filter_to_expr(&node.id, prop, filter));
        }
    }

    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            for (prop, filter) in &rel.filters {
                conds.push(filter_to_expr(alias, prop, filter));
            }
        }
    }

    Expr::and_all(conds.into_iter().map(Some))
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

fn build_order_by(order_by: &Option<crate::input::InputOrderBy>) -> Vec<OrderExpr> {
    let Some(ob) = order_by else { return vec![] };
    vec![OrderExpr {
        expr: Expr::col(&ob.node, &ob.property),
        desc: ob.direction == OrderDirection::Desc,
    }]
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn node<'a>(nodes: &'a [InputNode], id: &str) -> Result<&'a InputNode> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .ok_or_else(|| QueryError::Lowering(format!("node '{id}' not found")))
}

fn entity(node: &InputNode) -> Result<&str> {
    node.entity
        .as_deref()
        .ok_or_else(|| QueryError::Lowering(format!("node '{}' has no entity", node.id)))
}

fn table_name(ontology: &Ontology, entity: &str) -> Result<String> {
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
        assert_eq!(q.select.len(), 2);
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

        let Node::RecursiveCte(cte) = lower(&input, &test_ontology()).unwrap() else {
            panic!("expected RecursiveCte");
        };
        println!("{:?}", cte);
        assert_eq!(cte.max_depth, 3);
        assert_eq!(cte.name, "path_cte");
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
    fn test_lower_multi_hop() {
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
            }
        }
        assert!(count_joins(&q.from) >= 4);
    }
}

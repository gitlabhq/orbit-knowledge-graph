//! Lower: Input → AST
//!
//! Converts the LLM's JSON input into a SQL-oriented AST.

use crate::ast::{Expr, JoinType, Node, Op, OrderExpr, Query, RecursiveCte, SelectExpr, TableRef};
use crate::error::{QueryError, Result};
use crate::input::{
    Direction, FilterOp, Input, InputAggregation, InputFilter, InputNode, InputRelationship,
    OrderDirection, QueryType,
};
use ontology::{Ontology, EDGE_TABLE};
use serde_json::Value;
use std::collections::HashMap;

/// Lower parsed input into an AST node
pub fn lower(input: &Input, ontology: &Ontology) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal | QueryType::Pattern => lower_traversal(input, ontology),
        QueryType::Aggregation => lower_aggregation(input, ontology),
        QueryType::PathFinding => lower_path_finding(input, ontology),
    }
}

/// Lower a traversal query: SELECT ... FROM nodes JOIN edges JOIN nodes ... WHERE ...
fn lower_traversal(input: &Input, ontology: &Ontology) -> Result<Node> {
    let (from, edge_aliases) = build_from(&input.nodes, &input.relationships, ontology)?;
    let where_clause = build_where(&input.nodes, &input.relationships, &edge_aliases, ontology)?;

    // Build SELECT - return node IDs
    let select: Vec<SelectExpr> = input
        .nodes
        .iter()
        .map(|n| SelectExpr {
            expr: Expr::col(&n.id, "id"),
            alias: Some(format!("{}_id", n.id)),
        })
        .collect();

    // Build ORDER BY
    let order_by = if let Some(ref ob) = input.order_by {
        let node_label = find_node_label(&input.nodes, &ob.node);
        if let Some(ref label) = node_label {
            ontology.validate_field(label, &ob.property)?;
        }
        vec![OrderExpr {
            expr: Expr::col(&ob.node, &ob.property),
            desc: ob.direction == OrderDirection::Desc,
        }]
    } else {
        vec![]
    };

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        group_by: vec![],
        order_by,
        limit: Some(input.limit),
    })))
}

/// Lower an aggregation query: SELECT agg(...) ... GROUP BY ...
fn lower_aggregation(input: &Input, ontology: &Ontology) -> Result<Node> {
    let (from, edge_aliases) = build_from(&input.nodes, &input.relationships, ontology)?;
    let where_clause = build_where(&input.nodes, &input.relationships, &edge_aliases, ontology)?;

    let mut select = Vec::with_capacity(input.aggregations.len() * 2);
    let mut group_by = Vec::new();
    let mut grouped = std::collections::HashSet::new();

    for agg in &input.aggregations {
        // Validate aggregation property if specified
        if let Some(ref prop) = agg.property {
            if let Some(ref target) = agg.target {
                if let Some(ref label) = find_node_label(&input.nodes, target) {
                    ontology.validate_field(label, prop)?;
                }
            }
        }

        // Add GROUP BY column
        if let Some(ref gb) = agg.group_by {
            if !grouped.contains(gb) {
                grouped.insert(gb.clone());
                group_by.push(Expr::col(gb, "id"));
                select.push(SelectExpr {
                    expr: Expr::col(gb, "id"),
                    alias: Some(format!("{gb}_id")),
                });
            }
        }

        // Add aggregate function
        let alias = agg
            .alias
            .clone()
            .unwrap_or_else(|| agg.function.as_sql().to_lowercase());
        select.push(SelectExpr {
            expr: build_agg_func(agg),
            alias: Some(alias),
        });
    }

    // ORDER BY aggregation result
    let order_by = if let Some(ref agg_sort) = input.aggregation_sort {
        if agg_sort.agg_index < input.aggregations.len() {
            vec![OrderExpr {
                expr: build_agg_func(&input.aggregations[agg_sort.agg_index]),
                desc: agg_sort.direction == OrderDirection::Desc,
            }]
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    Ok(Node::Query(Box::new(Query {
        select,
        from,
        where_clause,
        group_by,
        order_by,
        limit: Some(input.limit),
    })))
}

fn build_agg_func(agg: &InputAggregation) -> Expr {
    let arg = if let Some(ref prop) = agg.property {
        if let Some(ref target) = agg.target {
            Expr::col(target, prop)
        } else {
            Expr::lit(1)
        }
    } else if let Some(ref target) = agg.target {
        Expr::col(target, "id")
    } else {
        Expr::lit(1)
    };

    Expr::func(agg.function.as_sql(), vec![arg])
}

/// Lower a path finding query: WITH RECURSIVE ...
fn lower_path_finding(input: &Input, ontology: &Ontology) -> Result<Node> {
    let path = input.path.as_ref().ok_or_else(|| {
        QueryError::Lowering(
            "path_finding query requires a 'path' configuration with 'from' and 'to' nodes".into(),
        )
    })?;

    let start_node = input
        .nodes
        .iter()
        .find(|n| n.id == path.from)
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "path 'from' references node \"{}\" which is not defined in nodes",
                path.from
            ))
        })?;

    let end_node = input
        .nodes
        .iter()
        .find(|n| n.id == path.to)
        .ok_or_else(|| {
            QueryError::Lowering(format!(
                "path 'to' references node \"{}\" which is not defined in nodes",
                path.to
            ))
        })?;

    // Base case: start node - support multiple node IDs with IN clause
    let base_where = if start_node.node_ids.len() == 1 {
        Some(Expr::eq(
            Expr::col("n", "id"),
            Expr::lit(start_node.node_ids[0]),
        ))
    } else if start_node.node_ids.len() > 1 {
        Some(Expr::binary(
            Op::In,
            Expr::col("n", "id"),
            Expr::lit(Value::Array(
                start_node
                    .node_ids
                    .iter()
                    .map(|&id| Value::from(id))
                    .collect(),
            )),
        ))
    } else {
        None
    };

    let base = Query {
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
        from: {
            let label = start_node.label.as_ref().ok_or_else(|| {
                QueryError::Lowering("path finding requires node labels to determine table".into())
            })?;
            let table = ontology.table_name(label)?;
            TableRef::scan_with_filter(&table, "n", label)
        },
        where_clause: base_where,
        group_by: vec![],
        order_by: vec![],
        limit: None,
    };

    // Recursive case: extend path
    let recursive = Query {
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
        from: {
            let label = end_node.label.as_ref().ok_or_else(|| {
                QueryError::Lowering("path finding requires node labels to determine table".into())
            })?;
            let table = ontology.table_name(label)?;
            TableRef::join(
                JoinType::Inner,
                TableRef::join(
                    JoinType::Inner,
                    TableRef::scan("path_cte", "p"),
                    TableRef::scan(EDGE_TABLE, "e"),
                    Expr::eq(Expr::col("p", "node_id"), Expr::col("e", "from_id")),
                ),
                TableRef::scan_with_filter(&table, "n", label),
                Expr::eq(Expr::col("e", "to_id"), Expr::col("n", "id")),
            )
        },
        where_clause: Expr::and_all([
            Some(Expr::binary(
                Op::Lt,
                Expr::col("p", "depth"),
                Expr::lit(path.max_depth as i64),
            )),
            Some(Expr::unary(
                Op::Not,
                Expr::func("has", vec![Expr::col("p", "path"), Expr::col("n", "id")]),
            )),
        ]),
        group_by: vec![],
        order_by: vec![],
        limit: None,
    };

    // Final query - support multiple end node IDs with IN clause
    let final_where = if end_node.node_ids.len() == 1 {
        Some(Expr::eq(
            Expr::col("path_cte", "node_id"),
            Expr::lit(end_node.node_ids[0]),
        ))
    } else if end_node.node_ids.len() > 1 {
        Some(Expr::binary(
            Op::In,
            Expr::col("path_cte", "node_id"),
            Expr::lit(Value::Array(
                end_node
                    .node_ids
                    .iter()
                    .map(|&id| Value::from(id))
                    .collect(),
            )),
        ))
    } else {
        None
    };

    let final_query = Query {
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
        where_clause: final_where,
        group_by: vec![],
        order_by: vec![OrderExpr {
            expr: Expr::col("path_cte", "depth"),
            desc: false,
        }],
        limit: Some(input.limit),
    };

    Ok(Node::RecursiveCte(Box::new(RecursiveCte {
        name: "path_cte".into(),
        base,
        recursive,
        max_depth: path.max_depth,
        final_query,
    })))
}

/// Build the FROM clause with joins.
///
/// The starting node is determined by:
/// 1. If there are relationships, use the "from" node of the first relationship
/// 2. Otherwise, use the first node in the array
fn build_from(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    ontology: &Ontology,
) -> Result<(TableRef, HashMap<usize, String>)> {
    if nodes.is_empty() {
        return Err(QueryError::Lowering("at least one node required".into()));
    }

    let mut edge_aliases = HashMap::new();

    // Determine starting node: "from" of first relationship, or first node if no relationships
    let start_node = if let Some(first_rel) = rels.first() {
        nodes
            .iter()
            .find(|n| n.id == first_rel.from)
            .ok_or_else(|| {
                QueryError::Lowering(format!(
                    "relationship references node \"{}\" which is not defined",
                    first_rel.from
                ))
            })?
    } else {
        &nodes[0]
    };

    // Start with the starting node - label required to determine table
    let start_label = start_node.label.as_ref().ok_or_else(|| {
        QueryError::Lowering(format!(
            "node \"{}\" requires a label to determine which table to query",
            start_node.id
        ))
    })?;
    let start_table = ontology.table_name(start_label)?;
    let mut result = TableRef::scan_with_filter(&start_table, &start_node.id, start_label);

    // Join edges and nodes for each relationship
    for (i, rel) in rels.iter().enumerate() {
        let edge_alias = format!("e{i}");
        edge_aliases.insert(i, edge_alias.clone());

        // Validate and set relationship type filter
        let type_filter = if rel.types.len() == 1 && rel.types[0] != "*" {
            ontology.validate_type(&rel.types[0])?;
            Some(rel.types[0].clone())
        } else {
            // Validate all relationship types even if we don't filter by them
            for rel_type in &rel.types {
                if rel_type != "*" {
                    ontology.validate_type(rel_type)?;
                }
            }
            None
        };

        // Join edge table
        let edge_join_cond = match rel.direction {
            Direction::Incoming => {
                Expr::eq(Expr::col(&rel.from, "id"), Expr::col(&edge_alias, "to_id"))
            }
            Direction::Both => Expr::or_all([
                Some(Expr::eq(
                    Expr::col(&rel.from, "id"),
                    Expr::col(&edge_alias, "from_id"),
                )),
                Some(Expr::eq(
                    Expr::col(&rel.from, "id"),
                    Expr::col(&edge_alias, "to_id"),
                )),
            ])
            .unwrap(),
            Direction::Outgoing => Expr::eq(
                Expr::col(&rel.from, "id"),
                Expr::col(&edge_alias, "from_id"),
            ),
        };

        let edge_table = if let Some(ref tf) = type_filter {
            TableRef::scan_with_filter(EDGE_TABLE, &edge_alias, tf)
        } else {
            TableRef::scan(EDGE_TABLE, &edge_alias)
        };

        result = TableRef::join(JoinType::Inner, result, edge_table, edge_join_cond);

        // Join target node - label required to determine table
        let target_label = find_node_label(nodes, &rel.to).ok_or_else(|| {
            QueryError::Lowering(format!(
                "node \"{}\" requires a label to determine which table to query",
                rel.to
            ))
        })?;
        let target_table_name = ontology.table_name(&target_label)?;

        let target_join_cond = match rel.direction {
            Direction::Incoming => {
                Expr::eq(Expr::col(&edge_alias, "from_id"), Expr::col(&rel.to, "id"))
            }
            Direction::Both => Expr::or_all([
                Some(Expr::eq(
                    Expr::col(&edge_alias, "to_id"),
                    Expr::col(&rel.to, "id"),
                )),
                Some(Expr::eq(
                    Expr::col(&edge_alias, "from_id"),
                    Expr::col(&rel.to, "id"),
                )),
            ])
            .unwrap(),
            Direction::Outgoing => {
                Expr::eq(Expr::col(&edge_alias, "to_id"), Expr::col(&rel.to, "id"))
            }
        };

        let target_table = TableRef::scan_with_filter(&target_table_name, &rel.to, &target_label);

        result = TableRef::join(JoinType::Inner, result, target_table, target_join_cond);
    }

    Ok((result, edge_aliases))
}

fn find_node_label(nodes: &[InputNode], id: &str) -> Option<String> {
    nodes
        .iter()
        .find(|n| n.id == id)
        .and_then(|n| n.label.clone())
}

/// Build the WHERE clause from filters
fn build_where(
    nodes: &[InputNode],
    rels: &[InputRelationship],
    edge_aliases: &HashMap<usize, String>,
    ontology: &Ontology,
) -> Result<Option<Expr>> {
    let mut conds: Vec<Option<Expr>> = Vec::new();

    for node in nodes {
        // Node ID filter
        match node.node_ids.len() {
            0 => {}
            1 => {
                conds.push(Some(Expr::eq(
                    Expr::col(&node.id, "id"),
                    Expr::lit(node.node_ids[0]),
                )));
            }
            _ => {
                let ids: Vec<Value> = node.node_ids.iter().map(|&id| Value::from(id)).collect();
                conds.push(Some(Expr::binary(
                    Op::In,
                    Expr::col(&node.id, "id"),
                    Expr::lit(Value::Array(ids)),
                )));
            }
        }

        // ID range
        if let Some(ref range) = node.id_range {
            conds.push(Some(Expr::binary(
                Op::Ge,
                Expr::col(&node.id, "id"),
                Expr::lit(range.start),
            )));
            conds.push(Some(Expr::binary(
                Op::Le,
                Expr::col(&node.id, "id"),
                Expr::lit(range.end),
            )));
        }

        // Property filters
        for (prop, filter) in &node.filters {
            if let Some(ref label) = node.label {
                ontology.validate_field(label, prop)?;
            }
            conds.push(Some(filter_to_expr(&node.id, prop, filter)));
        }
    }

    for (i, rel) in rels.iter().enumerate() {
        if let Some(alias) = edge_aliases.get(&i) {
            for (prop, filter) in &rel.filters {
                conds.push(Some(filter_to_expr(alias, prop, filter)));
            }
        }
    }

    Ok(Expr::and_all(conds))
}

fn filter_to_expr(table: &str, column: &str, filter: &InputFilter) -> Expr {
    let col = Expr::col(table, column);

    match filter.op {
        None => {
            // Simple equality
            Expr::eq(
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            )
        }
        Some(op) => match op {
            FilterOp::Eq => Expr::eq(
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            ),
            FilterOp::Gt => Expr::binary(
                Op::Gt,
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            ),
            FilterOp::Lt => Expr::binary(
                Op::Lt,
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            ),
            FilterOp::Gte => Expr::binary(
                Op::Ge,
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            ),
            FilterOp::Lte => Expr::binary(
                Op::Le,
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            ),
            FilterOp::In => Expr::binary(
                Op::In,
                col,
                Expr::Literal(filter.value.clone().unwrap_or(Value::Null)),
            ),
            FilterOp::Contains => {
                let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                Expr::binary(Op::Like, col, Expr::lit(format!("%{val}%")))
            }
            FilterOp::StartsWith => {
                let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                Expr::binary(Op::Like, col, Expr::lit(format!("{val}%")))
            }
            FilterOp::EndsWith => {
                let val = filter.value.as_ref().and_then(|v| v.as_str()).unwrap_or("");
                Expr::binary(Op::Like, col, Expr::lit(format!("%{val}")))
            }
            FilterOp::IsNull => Expr::unary(Op::IsNull, col),
            FilterOp::IsNotNull => Expr::unary(Op::IsNotNull, col),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::parse_input;

    fn test_ontology() -> Ontology {
        use ontology::DataType::{DateTime, String};
        Ontology::new()
            .with_nodes(["User", "Project", "Note", "Group"])
            .with_edges(["AUTHORED", "CONTAINS", "MEMBER_OF"])
            .with_fields("User", [("username", String), ("state", String), ("created_at", DateTime)])
    }

    #[test]
    fn test_lower_simple_traversal() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "label": "Note"},
                {"id": "u", "label": "User"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"}
            ],
            "limit": 25
        }"#,
        )
        .unwrap();

        let ast = lower(&input, &test_ontology()).unwrap();
        if let Node::Query(q) = ast {
            assert_eq!(q.limit, Some(25));
            assert_eq!(q.select.len(), 2);
        } else {
            panic!("expected Query");
        }
    }

    #[test]
    fn test_lower_aggregation() {
        let input = parse_input(
            r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n", "label": "Note"}, {"id": "u", "label": "User"}],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "aggregations": [
                {"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}
            ],
            "limit": 10
        }"#,
        )
        .unwrap();

        let ast = lower(&input, &test_ontology()).unwrap();
        if let Node::Query(q) = ast {
            assert!(!q.group_by.is_empty());
            // Check for COUNT in select
            let has_count = q
                .select
                .iter()
                .any(|s| matches!(&s.expr, Expr::FuncCall { name, .. } if name == "COUNT"));
            assert!(has_count);
        } else {
            panic!("expected Query");
        }
    }

    #[test]
    fn test_lower_path_finding() {
        let input = parse_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "label": "Project", "node_ids": [100]},
                {"id": "end", "label": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        )
        .unwrap();

        let ast = lower(&input, &test_ontology()).unwrap();
        if let Node::RecursiveCte(cte) = ast {
            assert_eq!(cte.max_depth, 3);
            assert_eq!(cte.name, "path_cte");
        } else {
            panic!("expected RecursiveCte");
        }
    }

    #[test]
    fn test_lower_with_filters() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            }],
            "limit": 30
        }"#,
        )
        .unwrap();

        let ast = lower(&input, &test_ontology()).unwrap();
        if let Node::Query(q) = ast {
            assert!(q.where_clause.is_some());
        } else {
            panic!("expected Query");
        }
    }

    fn count_joins(t: &TableRef) -> usize {
        match t {
            TableRef::Join { left, right, .. } => 1 + count_joins(left) + count_joins(right),
            TableRef::Scan { .. } => 0,
        }
    }

    #[test]
    fn test_lower_multi_hop() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "label": "User"},
                {"id": "n", "label": "Note"},
                {"id": "p", "label": "Project"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"},
                {"type": "CONTAINS", "from": "p", "to": "n"}
            ],
            "limit": 20
        }"#,
        )
        .unwrap();

        let ast = lower(&input, &test_ontology()).unwrap();
        if let Node::Query(q) = ast {
            let joins = count_joins(&q.from);
            assert!(joins >= 4, "expected >= 4 joins, got {joins}");
        } else {
            panic!("expected Query");
        }
    }
}

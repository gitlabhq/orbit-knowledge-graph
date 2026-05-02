//! Traversal query lowering (v2).
//!
//! Single-node: direct node table scan.
//! Multi-node: skeleton edge chain + edge metadata SELECT + ORDER BY.

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::types::*;

pub fn lower_traversal(input: &mut Input) -> Result<Node> {
    if input.is_search() || input.relationships.is_empty() {
        return lower_single_node(input);
    }

    let skeleton = Skeleton::build(input)?;

    let mut select = Vec::new();
    for ea in &skeleton.edge_aliases {
        select.extend(edge_select_columns(ea));
    }

    let order_by = input
        .order_by
        .as_ref()
        .map(|ob| {
            vec![OrderExpr {
                expr: Expr::col(&ob.node, &ob.property),
                desc: matches!(ob.direction, OrderDirection::Desc),
            }]
        })
        .unwrap_or_default();

    let q = skeleton.into_query(select, vec![], order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

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

//! Traversal query lowering (v2).
//!
//! Single-node: direct node table scan.
//! Multi-node: skeleton edge chain + edge metadata SELECT + ORDER BY.

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;

use super::shared::*;
use super::types::*;

pub fn lower_traversal(input: &mut Input) -> Result<Node> {
    if input.is_search() || input.relationships.is_empty() {
        return lower_single_node(input);
    }

    let skeleton = Skeleton::plan(input);
    let output = skeleton.emit(input)?;

    let mut select = Vec::new();
    for ea in &output.edge_aliases {
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

    let q = output.into_query(select, vec![], order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

fn lower_single_node(input: &mut Input) -> Result<Node> {
    let skeleton = Skeleton::plan(input);
    let output = skeleton.emit(input)?;

    let node = input
        .nodes
        .first()
        .ok_or_else(|| QueryError::Lowering("no nodes in query".into()))?;

    let mut select = vec![SelectExpr::new(Expr::col(&node.id, "id"), "id")];
    for col in requested_columns(&node.columns) {
        if col != "id" {
            select.push(SelectExpr::new(Expr::col(&node.id, &col), col.clone()));
        }
    }

    let q = output.into_query(select, vec![], vec![], input.limit);
    Ok(Node::Query(Box::new(q)))
}

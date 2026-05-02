//! Traversal query lowering (v2).
//!
//! Single-node: direct node table scan.
//! Multi-node: skeleton edge chain + edge metadata SELECT + ORDER BY.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::shared::edge_select_columns;
use super::types::Skeleton;

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

    let q = output.into_query(vec![], vec![], order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

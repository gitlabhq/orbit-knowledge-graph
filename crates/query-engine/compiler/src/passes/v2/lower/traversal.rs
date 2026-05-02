//! Traversal query lowering (v2).
//!
//! Single-node: direct node table scan.
//! Multi-node: skeleton edge chain + edge metadata SELECT + ORDER BY.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use crate::constants::*;
use super::shared::{edge_select_columns, edge_select_columns_with_prefix};
use super::types::Skeleton;

pub fn lower_traversal(input: &mut Input) -> Result<Node> {
    if input.is_search() || input.relationships.is_empty() {
        return lower_single_node(input);
    }

    let skeleton = Skeleton::plan(input);
    let output = skeleton.emit(input)?;

    let mut select = Vec::new();
    for (i, ea) in output.edge_aliases.iter().enumerate() {
        let is_multi = input
            .relationships
            .get(i)
            .is_some_and(|r| r.max_hops > 1);
        if is_multi {
            // Multi-hop: use hop_ prefix and include path_nodes.
            let prefix = format!("hop_{ea}");
            select.extend(edge_select_columns_with_prefix(ea, &prefix));
            select.push(SelectExpr::new(
                Expr::col(ea, PATH_NODES_COLUMN),
                format!("{prefix}_{PATH_NODES_COLUMN}"),
            ));
        } else {
            select.extend(edge_select_columns(ea));
        }
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

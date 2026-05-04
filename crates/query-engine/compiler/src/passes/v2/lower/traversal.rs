//! Traversal query lowering.
//!
//! Single-node: direct node table scan.
//! Multi-node: edge chain plan + edge metadata SELECT + ORDER BY.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::super::plan::{EdgeChainPlan, Strategy};
use super::super::shared::edge_select_columns;
use super::super::shared::edge_select_columns_with_prefix;
use crate::constants::*;

pub fn emit_traversal(plan: &EdgeChainPlan, input: &mut Input) -> Result<Node> {
    if matches!(plan.strategy, Strategy::SingleNode) {
        return emit_single_node(plan, input);
    }

    let output = plan.emit(input)?;

    let mut select = Vec::new();
    let already_has_edge_cols = output.select.iter().any(|s| {
        s.alias
            .as_deref()
            .is_some_and(|a| a.ends_with(EDGE_TYPE_SUFFIX))
    });
    if !already_has_edge_cols {
        for (i, ea) in output.edge_aliases.iter().enumerate() {
            let is_multi = plan.hops.get(i).is_some_and(|h| h.max_hops > 1);
            if is_multi {
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
    }

    let order_by = plan
        .order_by
        .as_ref()
        .map(|ob| {
            vec![OrderExpr {
                expr: Expr::col(&ob.node, &ob.property),
                desc: ob.desc,
            }]
        })
        .unwrap_or_default();

    let q = output.into_query(select, vec![], order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}

fn emit_single_node(plan: &EdgeChainPlan, input: &mut Input) -> Result<Node> {
    let output = plan.emit(input)?;

    let order_by = plan
        .order_by
        .as_ref()
        .map(|ob| {
            vec![OrderExpr {
                expr: Expr::col(&ob.node, &ob.property),
                desc: ob.desc,
            }]
        })
        .unwrap_or_default();

    let q = output.into_query(vec![], vec![], order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}

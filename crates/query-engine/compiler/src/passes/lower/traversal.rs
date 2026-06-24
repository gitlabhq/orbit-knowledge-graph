//! Traversal query lowering.

use crate::ast::*;
use crate::error::Result;

use crate::constants::*;
use crate::passes::plan::{Plan, Strategy};
use crate::passes::shared::edge_select_columns;
use crate::passes::shared::edge_select_columns_with_prefix;

use super::pagination::{apply_keyset, traversal_keys};

pub fn emit_traversal(plan: &Plan) -> Result<Node> {
    if matches!(plan.strategy, Strategy::SingleNode) {
        return emit_single_node(plan);
    }

    let output = plan.emit_edge_chain()?;

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

    let mut q = output.into_query(select, vec![], vec![], plan.limit);
    let keys = traversal_keys(plan);
    apply_keyset(&mut q, &keys, plan.cursor.as_ref(), false)?;
    Ok(Node::Query(Box::new(q)))
}

fn emit_single_node(plan: &Plan) -> Result<Node> {
    let output = plan.emit_edge_chain()?;

    let mut q = output.into_query(vec![], vec![], vec![], plan.limit);
    let keys = traversal_keys(plan);
    apply_keyset(&mut q, &keys, plan.cursor.as_ref(), false)?;
    Ok(Node::Query(Box::new(q)))
}

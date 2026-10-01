use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::EmitOutput;
use crate::constants::*;
use crate::passes::plan::{Plan, Traversal};
use crate::passes::shared::edge_select_columns;
use crate::passes::shared::edge_select_columns_with_prefix;

pub fn emit_traversal(plan: &Plan<Traversal>, input: &Input, output: EmitOutput) -> Result<Node> {
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

    let order_by = input
        .order_by
        .as_ref()
        .map(|ob| {
            vec![if matches!(ob.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::col(&ob.node, &ob.property))
            } else {
                OrderExpr::asc(Expr::col(&ob.node, &ob.property))
            }]
        })
        .unwrap_or_default();
    let q = output.into_query(select, vec![], order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

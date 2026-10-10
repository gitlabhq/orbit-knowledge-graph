use crate::ast::*;
use crate::constants::EDGE_TYPE_SUFFIX;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::edge_chain::Hop;

use super::EmitOutput;
pub fn emit_traversal(input: &Input, output: EmitOutput, hops: &[Hop]) -> Result<Node> {
    let order_by = match (&input.order_by, &input.relationship_order) {
        (Some(ob), _) => vec![ordered(Expr::col(&ob.node, &ob.property), ob.direction)],
        (None, Some(order)) => {
            let (index, hop) = hops
                .iter()
                .enumerate()
                .find(|(_, hop)| hop.input_index == order.relationship)
                .ok_or_else(|| {
                    QueryError::Lowering("ordered relationship has no emitted hop".into())
                })?;
            let column = format!("{}{EDGE_TYPE_SUFFIX}", hop.column_prefix(index));
            let edge_type = output
                .select
                .iter()
                .find(|select| select.alias.as_deref() == Some(column.as_str()))
                .map(|select| select.expr.clone())
                .ok_or_else(|| {
                    QueryError::Lowering(format!("edge type column '{column}' is not selected"))
                })?;
            vec![ordered(edge_type, order.direction)]
        }
        (None, None) => Vec::new(),
    };
    let q = output.into_query(vec![], vec![], order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

fn ordered(expr: Expr, direction: OrderDirection) -> OrderExpr {
    if matches!(direction, OrderDirection::Desc) {
        OrderExpr::desc(expr)
    } else {
        OrderExpr::asc(expr)
    }
}

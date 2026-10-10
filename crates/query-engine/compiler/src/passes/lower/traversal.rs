use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::EmitOutput;
pub fn emit_traversal(input: &Input, output: EmitOutput) -> Result<Node> {
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
    let q = output.into_query(vec![], vec![], order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

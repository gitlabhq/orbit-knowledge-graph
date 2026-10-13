use crate::ast::*;
use crate::error::Result;
use crate::query::OrderDirection;

use super::EmitOutput;
pub fn emit_traversal(input: &crate::query::Query, output: EmitOutput) -> Result<Select> {
    let order_by = input
        .order
        .as_ref()
        .map(|ob| {
            vec![if matches!(ob.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::col(
                    &input.node(ob.node).alias,
                    ob.property.column.as_deref().unwrap_or(&ob.property.name),
                ))
            } else {
                OrderExpr::asc(Expr::col(
                    &input.node(ob.node).alias,
                    ob.property.column.as_deref().unwrap_or(&ob.property.name),
                ))
            }]
        })
        .unwrap_or_default();
    Ok(output.into_select(vec![], vec![], order_by, input.limit))
}

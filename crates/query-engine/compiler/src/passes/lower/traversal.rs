use crate::ast::*;
use crate::error::Result;
use crate::query::OrderDirection;

use super::EmitOutput;
pub fn emit_traversal(query: &crate::query::Query, output: EmitOutput) -> Result<Select> {
    let order_by = query
        .order()
        .map(|ob| {
            vec![if matches!(ob.direction, OrderDirection::Desc) {
                OrderExpr::desc(Expr::col(&query.node(ob.node).alias, ob.property.column()))
            } else {
                OrderExpr::asc(Expr::col(&query.node(ob.node).alias, ob.property.column()))
            }]
        })
        .unwrap_or_default();
    Ok(output.into_select(vec![], vec![], order_by, query.limit))
}

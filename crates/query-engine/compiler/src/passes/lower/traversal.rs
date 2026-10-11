use crate::ast::*;
use crate::constants::EDGE_TYPE_SUFFIX;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::edge_chain::Hop;

use super::EmitOutput;
pub fn emit_traversal(input: &Input, output: EmitOutput, hops: &[Hop]) -> Result<Node> {
    let mut q = output.into_query(vec![], vec![], vec![], input.limit);
    q.order_by = match (&input.order_by, &input.relationship_return.sort) {
        (Some(ob), _) => vec![ordered(Expr::col(&ob.node, &ob.property), ob.direction)],
        (None, Some(sort)) => {
            let edge_type = hops
                .iter()
                .enumerate()
                .find(|(_, hop)| hop.input_index == sort.relationship)
                .and_then(|(index, hop)| edge_type(&q, hop, index))
                .ok_or_else(|| {
                    QueryError::Lowering("sorted relationship has no edge type column".into())
                })?;
            vec![ordered(edge_type, sort.direction)]
        }
        (None, None) => Vec::new(),
    };
    Ok(Node::Query(Box::new(q)))
}

pub fn edge_type(query: &Query, hop: &Hop, index: usize) -> Option<Expr> {
    query
        .selected(&format!("{}{EDGE_TYPE_SUFFIX}", hop.column_prefix(index)))
        .cloned()
}

fn ordered(expr: Expr, direction: OrderDirection) -> OrderExpr {
    if matches!(direction, OrderDirection::Desc) {
        OrderExpr::desc(expr)
    } else {
        OrderExpr::asc(expr)
    }
}

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

pub fn emit_traversal(
    input: &Input,
    mut output: Query,
    nodes: &std::collections::HashMap<String, super::NodeBinding>,
    bindings: &query_data_model::bindings::QueryBindings,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Result<Node> {
    let order_by = input
        .order_by
        .as_ref()
        .map(|ob| -> Result<Vec<OrderExpr>> {
            let value = nodes
                .get(&ob.node)
                .ok_or_else(|| {
                    crate::error::QueryError::Lowering(format!(
                        "sort node '{}' has no binding",
                        ob.node
                    ))
                })?
                .property(output.scope, bindings, model, &ob.property)?;
            Ok(vec![if matches!(ob.direction, OrderDirection::Desc) {
                OrderExpr::desc(value)
            } else {
                OrderExpr::asc(value)
            }])
        })
        .transpose()?
        .unwrap_or_default();
    output.order_by = order_by;
    output.limit = Some(input.limit);
    Ok(Node::Query(Box::new(output)))
}

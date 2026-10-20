use super::Emitted;
use crate::ast::*;
use crate::constants::EDGE_TYPE_SUFFIX;
use crate::error::{QueryError, Result};
use crate::input::{Input, OrderDirection};
use crate::passes::lower::{graph, predicates};
use crate::passes::plan::{EdgeRelation, Plan, Traversal};

pub(crate) fn emit(plan: &Plan<Traversal>, input: &Input) -> Result<Emitted> {
    let execution = &plan.operation.execution;
    let mut read = super::super::redaction_columns(plan, execution);
    read.extend(
        input
            .order_by
            .iter()
            .map(|order| execution.property(&order.node, &order.property)),
    );
    let mut output = graph::execute(execution, &read);
    let nodes = output.take_bindings(plan, input)?;
    let mut query = output.into_query(vec![], vec![], vec![], input.limit);
    query.order_by = match (&input.order_by, &input.relationship_return.sort) {
        (Some(order), _) => vec![ordered(
            predicates::column(&execution.property(&order.node, &order.property)),
            order.direction,
        )],
        (None, Some(sort)) => {
            let edge_type = plan
                .relations
                .iter()
                .find(|hop| hop.input_index == sort.relationship)
                .and_then(|hop| edge_type(&query, hop))
                .ok_or_else(|| {
                    QueryError::Lowering("sorted relationship has no edge type column".into())
                })?;
            vec![ordered(edge_type, sort.direction)]
        }
        (None, None) => Vec::new(),
    };
    let identities = input
        .nodes
        .iter()
        .filter_map(|node| nodes.get(&node.id))
        .map(|binding| binding.identity().clone());
    let stable_order = if input.relationships.is_empty() {
        identities.map(OrderExpr::asc).collect()
    } else {
        identities
            .fold(Vec::new(), |mut order, identity| {
                if !order.contains(&identity) {
                    order.push(identity);
                }
                order
            })
            .into_iter()
            .chain(multi_type_edge_types(&query, &plan.relations))
            .map(OrderExpr::asc)
            .collect()
    };
    Ok(Emitted {
        ast: Node::Query(Box::new(query)),
        nodes,
        stable_order,
    })
}

fn ordered(expr: Expr, direction: OrderDirection) -> OrderExpr {
    if direction == OrderDirection::Desc {
        OrderExpr::desc(expr)
    } else {
        OrderExpr::asc(expr)
    }
}

fn multi_type_edge_types(query: &Query, relations: &[EdgeRelation]) -> Vec<Expr> {
    relations
        .iter()
        .filter(|hop| {
            hop.max_hops == 1
                && (hop.rel_types.len() > 1
                    || crate::passes::normalize::is_wildcard(&hop.rel_types))
        })
        .filter_map(|hop| edge_type(query, hop))
        .collect()
}

fn edge_type(query: &Query, relation: &EdgeRelation) -> Option<Expr> {
    query
        .selected(&format!("{}{}", relation.column_prefix(), EDGE_TYPE_SUFFIX))
        .cloned()
}

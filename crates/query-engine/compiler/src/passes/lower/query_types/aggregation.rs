use super::Emitted;
use crate::ast::*;
use crate::error::Result;
use crate::input::{Input, OrderDirection};
use crate::passes::lower::predicates::{column, predicate};
use crate::passes::lower::{EmitOutput, graph};
use crate::passes::plan::query_types::aggregation::{AggregationPlan, Group};
use crate::passes::plan::{Aggregation, Plan};

pub(crate) fn emit(plan: &Plan<Aggregation>, input: &Input) -> Result<Emitted> {
    let result = &plan.operation.result;
    let mut read = super::super::redaction_columns(plan, &plan.operation.execution);
    for predicate in &result.condition {
        predicate.columns(&mut read);
    }
    read.extend(result.groups.iter().map(|group| group.column.clone()));
    read.extend(
        result
            .measures
            .iter()
            .filter_map(|measure| measure.argument.clone()),
    );
    let mut output = graph::execute(&plan.operation.execution, &read);
    let nodes = output.take_bindings(plan, input)?;
    let query = aggregation(&plan.operation.result, output, input.limit);
    let stable_order = query.group_by.iter().cloned().map(OrderExpr::asc).collect();
    Ok(Emitted {
        ast: Node::Query(Box::new(query)),
        nodes,
        stable_order,
    })
}

fn group(value: &Group) -> Expr {
    let column = column(&value.column);
    match value.truncate {
        Some(unit) => Expr::TimeBucket {
            unit,
            value: Box::new(column),
        },
        None => column,
    }
}

fn aggregation(plan: &AggregationPlan, output: EmitOutput, limit: u32) -> Query {
    let condition = Expr::conjoin(plan.condition.iter().map(predicate).collect());
    let select = plan
        .group_outputs
        .iter()
        .map(|(value, name)| SelectExpr::new(group(value), name))
        .chain(plan.measures.iter().map(|measure| {
            SelectExpr::new(
                Expr::Aggregate {
                    function: measure.function,
                    argument: measure
                        .argument
                        .as_ref()
                        .map(|value| Box::new(column(value))),
                    distinct: false,
                    condition: condition.clone().map(Box::new),
                },
                &measure.name,
            )
        }))
        .collect();
    let order = plan
        .order
        .iter()
        .map(|order| {
            let value = Expr::ident(&order.column);
            if order.direction == OrderDirection::Desc {
                OrderExpr::desc(value)
            } else {
                OrderExpr::asc(value)
            }
        })
        .collect();
    output.into_query(
        select,
        plan.groups.iter().map(group).collect(),
        order,
        limit,
    )
}

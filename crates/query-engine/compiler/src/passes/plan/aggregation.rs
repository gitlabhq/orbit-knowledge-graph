use query_data_model::QueryDataModel;

use crate::ast::{Expr, OrderExpr, SelectExpr};
use crate::input::{AggExpr, InputGroupByKey, OrderDirection, TruncateUnit, group_by_output_names};
use crate::passes::shared::requested_columns;

use super::HydrationStrategy;
use super::context::PlanningContext;

pub struct AggregationPlan {
    pub select: Vec<SelectExpr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderExpr>,
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn aggregation(&self, condition: Option<&Expr>) -> AggregationPlan {
        let aggregation = &self.input.aggregation;
        let mut plan = AggregationPlan {
            select: Vec::new(),
            group_by: Vec::new(),
            order_by: Vec::new(),
        };
        for (group, alias) in aggregation
            .group_by
            .iter()
            .zip(group_by_output_names(&aggregation.group_by))
        {
            match group {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    ..
                } => {
                    let column = Expr::col(node, property);
                    let expression = match truncate {
                        Some(unit) => {
                            let truncated = Expr::func(unit.ch_function(), vec![column]);
                            match unit {
                                TruncateUnit::Minute | TruncateUnit::Hour => {
                                    Expr::func("toDateTime64", vec![truncated, Expr::ident("0")])
                                }
                                _ => Expr::func("toDate32", vec![truncated]),
                            }
                        }
                        None => column,
                    };
                    plan.select.push(SelectExpr::new(expression.clone(), alias));
                    if !plan.group_by.contains(&expression) {
                        plan.group_by.push(expression);
                    }
                }
                InputGroupByKey::Node { node, .. } => {
                    if let Some(metadata) = self.nodes.get(node.as_str()) {
                        for column in requested_columns(&metadata.columns) {
                            let expression = Expr::col(node, column);
                            if !plan.group_by.contains(&expression) {
                                plan.group_by.push(expression);
                            }
                        }
                    }
                }
            }
        }
        for metric in &aggregation.metrics {
            let argument = match &metric.expr {
                AggExpr::Count(target) => target
                    .property
                    .as_ref()
                    .filter(|_| {
                        !self
                            .nodes
                            .get(&target.node)
                            .is_some_and(|node| node.hydration == HydrationStrategy::Skip)
                    })
                    .map(|property| Expr::col(&target.node, property)),
                AggExpr::Sum(property)
                | AggExpr::Avg(property)
                | AggExpr::Min(property)
                | AggExpr::Max(property)
                | AggExpr::Collect(property) => Some(Expr::col(&property.node, &property.property)),
            };
            let mut arguments: Vec<_> = argument.into_iter().collect();
            let function = metric.expr.function();
            let name = if let Some(condition) = condition {
                arguments.push(condition.clone());
                function.as_sql_if()
            } else {
                function.as_sql()
            };
            plan.select.push(SelectExpr::new(
                Expr::func(name, arguments),
                metric.output_name(),
            ));
        }
        if let Some(sort) = &aggregation.sort {
            let column = Expr::ident(&sort.column);
            plan.order_by
                .push(if sort.direction == OrderDirection::Desc {
                    OrderExpr::desc(column)
                } else {
                    OrderExpr::asc(column)
                });
        }
        plan
    }
}

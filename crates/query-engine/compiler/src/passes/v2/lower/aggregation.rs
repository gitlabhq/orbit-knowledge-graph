//! Aggregation query lowering.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::super::plan::Plan;
use super::super::shared::requested_columns;

pub fn emit_aggregation(
    plan: &Plan,
    aggregations: &[InputAggregation],
    agg_sort: Option<&InputAggSort>,
) -> Result<Node> {
    let output = plan.emit_edge_chain()?;
    let (agg_select, group_by, order_by) = build_aggregation(plan, aggregations, agg_sort);
    let q = output.into_query(agg_select, group_by, order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}

fn build_aggregation(
    plan: &Plan,
    aggregations: &[InputAggregation],
    agg_sort: Option<&InputAggSort>,
) -> (Vec<SelectExpr>, Vec<Expr>, Vec<OrderExpr>) {
    let mut select = Vec::new();
    let mut group_by = Vec::new();

    for agg in aggregations {
        let alias = agg.alias.as_deref().unwrap_or("agg_result");

        let agg_expr = match agg.function {
            AggFunction::Count => {
                if let (Some(target), Some(prop)) = (&agg.target, &agg.property) {
                    Expr::func("COUNT", vec![Expr::col(target, prop)])
                } else {
                    Expr::func("COUNT", vec![])
                }
            }
            _ => {
                let target = agg.target.as_deref().unwrap_or("*");
                let prop = agg.property.as_deref().unwrap_or("id");
                Expr::func(agg.function.as_sql(), vec![Expr::col(target, prop)])
            }
        };

        select.push(SelectExpr::new(agg_expr, alias));

        if let Some(ref gb) = agg.group_by {
            let cols = plan
                .nodes
                .get(gb.as_str())
                .map(|np| requested_columns(&np.columns))
                .unwrap_or_default();
            for col in cols {
                let expr = Expr::col(gb, &col);
                if !group_by.contains(&expr) {
                    group_by.push(expr);
                }
            }
        }
    }

    let mut order_by = Vec::new();
    if let Some(sort) = agg_sort
        && let Some(agg) = aggregations.get(sort.agg_index)
    {
        let alias = agg.alias.as_deref().unwrap_or("agg_result");
        order_by.push(OrderExpr {
            expr: Expr::ident(alias),
            desc: matches!(sort.direction, OrderDirection::Desc),
        });
    }

    (select, group_by, order_by)
}

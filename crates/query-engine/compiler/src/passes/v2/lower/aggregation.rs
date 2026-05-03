//! Aggregation query lowering.
//!
//! No-edge: direct node table scan with GROUP BY.
//! With edges: skeleton edge chain + GROUP BY + aggregate functions.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::shared::requested_columns;
use super::types::*;

pub fn lower_aggregation(input: &mut Input) -> Result<Node> {
    let skeleton = Skeleton::plan(input);
    let output = skeleton.emit(input)?;
    let (agg_select, group_by, order_by) = build_aggregation(input)?;

    let q = output.into_query(agg_select, group_by, order_by, input.limit);
    Ok(Node::Query(Box::new(q)))
}

fn build_aggregation(input: &Input) -> Result<(Vec<SelectExpr>, Vec<Expr>, Vec<OrderExpr>)> {
    let mut select = Vec::new();
    let mut group_by = Vec::new();

    for agg in &input.aggregations {
        let agg_expr = match agg.function {
            AggFunction::Count => {
                if let (Some(target), Some(prop)) = (agg.target.as_deref(), agg.property.as_deref())
                {
                    Expr::func("COUNT", vec![Expr::col(target, prop)])
                } else {
                    Expr::func("COUNT", vec![])
                }
            }
            AggFunction::Sum | AggFunction::Avg | AggFunction::Min | AggFunction::Max => {
                let target = agg.target.as_deref().unwrap_or("*");
                let prop = agg.property.as_deref().unwrap_or("id");
                let fname = match agg.function {
                    AggFunction::Sum => "SUM",
                    AggFunction::Avg => "AVG",
                    AggFunction::Min => "MIN",
                    AggFunction::Max => "MAX",
                    _ => unreachable!(),
                };
                Expr::func(fname, vec![Expr::col(target, prop)])
            }
            AggFunction::Collect => {
                let target = agg.target.as_deref().unwrap_or("*");
                let prop = agg.property.as_deref().unwrap_or("id");
                Expr::func("groupArray", vec![Expr::col(target, prop)])
            }
        };

        let alias = agg.alias.as_deref().unwrap_or("agg_result");
        select.push(SelectExpr::new(agg_expr, alias));

        if let Some(ref gb) = agg.group_by
            && let Some(gb_node) = input.nodes.iter().find(|n| n.id == *gb)
        {
            for col in requested_columns(&gb_node.columns) {
                let expr = Expr::col(gb, &col);
                if !group_by.contains(&expr) {
                    group_by.push(expr);
                }
            }
        }
    }

    let mut order_by = Vec::new();
    if let Some(ref agg_sort) = input.aggregation_sort
        && let Some(agg) = input.aggregations.get(agg_sort.agg_index)
    {
        let alias = agg.alias.as_deref().unwrap_or("agg_result");
        order_by.push(OrderExpr {
            expr: Expr::ident(alias),
            desc: matches!(agg_sort.direction, OrderDirection::Desc),
        });
    }

    Ok((select, group_by, order_by))
}

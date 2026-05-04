//! Aggregation query lowering.
//!
//! No-edge: direct node table scan with GROUP BY.
//! With edges: edge chain plan + GROUP BY + aggregate functions.

use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use super::super::plan::*;

pub fn emit_aggregation(plan: &EdgeChainPlan, input: &mut Input) -> Result<Node> {
    let output = plan.emit(input)?;
    let agg = plan.agg.as_ref().expect("aggregation plan required");
    let (agg_select, group_by, order_by) = build_aggregation(agg);

    let q = output.into_query(agg_select, group_by, order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}

fn build_aggregation(agg: &AggPlan) -> (Vec<SelectExpr>, Vec<Expr>, Vec<OrderExpr>) {
    let mut select = Vec::new();
    let mut group_by = Vec::new();

    for spec in &agg.specs {
        let agg_expr = match spec.function {
            AggFunction::Count => {
                if let (Some(target), Some(prop)) = (&spec.target, &spec.property) {
                    Expr::func("COUNT", vec![Expr::col(target, prop)])
                } else {
                    Expr::func("COUNT", vec![])
                }
            }
            _ => {
                let target = spec.target.as_deref().unwrap_or("*");
                let prop = spec.property.as_deref().unwrap_or("id");
                Expr::func(spec.function.as_sql(), vec![Expr::col(target, prop)])
            }
        };

        select.push(SelectExpr::new(agg_expr, &spec.alias));

        if let Some(ref gb) = spec.group_by {
            for col in &gb.columns {
                let expr = Expr::col(&gb.node_alias, col);
                if !group_by.contains(&expr) {
                    group_by.push(expr);
                }
            }
        }
    }

    let mut order_by = Vec::new();
    if let Some(ref sort) = agg.sort {
        order_by.push(OrderExpr {
            expr: Expr::ident(&sort.alias),
            desc: sort.desc,
        });
    }

    (select, group_by, order_by)
}

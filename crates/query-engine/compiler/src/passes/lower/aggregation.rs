use crate::ast::*;
use crate::error::Result;
use crate::input::*;

use crate::passes::plan::{HydrationStrategy, Plan};
use crate::passes::shared::requested_columns;

pub fn emit_aggregation(
    plan: &Plan,
    aggregations: &[InputAggregationMetric],
    group_by_keys: &[InputGroupByKey],
    agg_sort: Option<&InputAggSort>,
) -> Result<Node> {
    let output = plan.emit_edge_chain()?;
    let if_cond = output.edge_if_predicates.clone();
    let (agg_select, group_by, order_by) = build_aggregation(
        plan,
        aggregations,
        group_by_keys,
        agg_sort,
        if_cond.as_ref(),
    );
    let q = output.into_query(agg_select, group_by, order_by, plan.limit);
    Ok(Node::Query(Box::new(q)))
}

fn default_alias(func: AggFunction) -> String {
    func.as_sql().to_lowercase()
}

fn build_aggregation(
    plan: &Plan,
    aggregations: &[InputAggregationMetric],
    group_by_keys: &[InputGroupByKey],
    agg_sort: Option<&InputAggSort>,
    if_cond: Option<&Expr>,
) -> (Vec<SelectExpr>, Vec<Expr>, Vec<OrderExpr>) {
    let mut select = Vec::new();
    let mut group_by = Vec::new();

    let group_by_names = group_by_output_names(group_by_keys);
    for (group, alias) in group_by_keys.iter().zip(group_by_names) {
        match group {
            InputGroupByKey::Property {
                node,
                property,
                transform,
                ..
            } => {
                let col = Expr::col(node, property);
                let expr = match transform {
                    Some(crate::input::PropertyTransform::Truncate { unit }) => {
                        Expr::func(unit.ch_function(), vec![col])
                    }
                    None => col,
                };
                select.push(SelectExpr::new(expr.clone(), alias));
                if !group_by.contains(&expr) {
                    group_by.push(expr);
                }
            }
            InputGroupByKey::Node { node, .. } => {
                let cols = plan
                    .nodes
                    .get(node.as_str())
                    .map(|np| requested_columns(&np.columns))
                    .unwrap_or_default();
                for col in cols {
                    let expr = Expr::col(node, &col);
                    if !group_by.contains(&expr) {
                        group_by.push(expr);
                    }
                }
            }
        }
    }

    for agg in aggregations {
        let owned_default = default_alias(agg.function);
        let alias = agg.alias.as_deref().unwrap_or(&owned_default);

        let agg_expr = build_agg_expr(plan, agg, if_cond);
        select.push(SelectExpr::new(agg_expr, alias));
    }

    let mut order_by = Vec::new();
    if let Some(sort) = agg_sort {
        let alias = sort.column.as_str();
        order_by.push(if matches!(sort.direction, OrderDirection::Desc) {
            OrderExpr::desc(Expr::ident(alias))
        } else {
            OrderExpr::asc(Expr::ident(alias))
        });
    }

    (select, group_by, order_by)
}

/// `if_cond` is present only on the LIMIT BY dedup path; it switches the
/// aggregates to `-If` combinators. `COUNT(col)` is kept as-is rather than
/// `countIf` because it must count non-null values of the column.
fn build_agg_expr(plan: &Plan, agg: &InputAggregationMetric, if_cond: Option<&Expr>) -> Expr {
    let count_drops_col = matches!(agg.function, AggFunction::Count)
        && agg
            .target
            .as_deref()
            .and_then(|t| plan.nodes.get(t))
            .is_some_and(|np| matches!(np.hydration, HydrationStrategy::Skip));

    match if_cond {
        Some(cond) => match agg.function {
            AggFunction::Count => {
                if let (Some(target), Some(prop)) = (&agg.target, &agg.property)
                    && !count_drops_col
                {
                    Expr::func(
                        agg.function.as_sql_if(),
                        vec![Expr::col(target, prop), cond.clone()],
                    )
                } else {
                    Expr::func(agg.function.as_sql_if(), vec![cond.clone()])
                }
            }
            _ => {
                let target = agg.target.as_deref().unwrap_or("*");
                let prop = agg.property.as_deref().unwrap_or("id");
                Expr::func(
                    agg.function.as_sql_if(),
                    vec![Expr::col(target, prop), cond.clone()],
                )
            }
        },
        None => match agg.function {
            AggFunction::Count => {
                if let (Some(target), Some(prop)) = (&agg.target, &agg.property)
                    && !count_drops_col
                {
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
        },
    }
}

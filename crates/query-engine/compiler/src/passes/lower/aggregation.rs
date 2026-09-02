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
                truncate,
                ..
            } => {
                let col = Expr::col(node, property);
                let expr = match truncate {
                    Some(unit) => {
                        // Without the cast, `Date`/`DateTime` keys cross Arrow
                        // as bare integers on some server versions.
                        let truncated = Expr::func(unit.ch_function(), vec![col]);
                        match unit {
                            TruncateUnit::Minute | TruncateUnit::Hour => {
                                // Ident, not lit: toDateTime64 rejects a
                                // parameterized scale argument.
                                Expr::func("toDateTime64", vec![truncated, Expr::ident("0")])
                            }
                            TruncateUnit::Day
                            | TruncateUnit::Week
                            | TruncateUnit::Month
                            | TruncateUnit::Quarter
                            | TruncateUnit::Year => Expr::func("toDate32", vec![truncated]),
                        }
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
        let agg_expr = build_agg_expr(plan, &agg.expr, if_cond);
        select.push(SelectExpr::new(agg_expr, agg.output_name()));
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
    if plan.cursor.is_some() {
        // The group-key tuple is unique per result row, so it completes the
        // sort into a total order the keyset seek can anchor on.
        order_by.extend(group_by.iter().map(|e| OrderExpr::asc(e.clone())));
    }

    (select, group_by, order_by)
}

/// `if_cond` is present only on the LIMIT BY dedup path; it switches the
/// aggregates to `-If` combinators. `COUNT(col)` is kept as-is rather than
/// `countIf` because it must count non-null values of the column.
fn build_agg_expr(plan: &Plan, expr: &AggExpr, if_cond: Option<&Expr>) -> Expr {
    match expr {
        AggExpr::Count(target) => match (counted_column(plan, target), if_cond) {
            (Some(col), Some(cond)) => {
                Expr::func(AggFunction::Count.as_sql_if(), vec![col, cond.clone()])
            }
            (None, Some(cond)) => Expr::func(AggFunction::Count.as_sql_if(), vec![cond.clone()]),
            (Some(col), None) => Expr::func("COUNT", vec![col]),
            (None, None) => Expr::func("COUNT", vec![]),
        },
        AggExpr::Sum(prop)
        | AggExpr::Avg(prop)
        | AggExpr::Min(prop)
        | AggExpr::Max(prop)
        | AggExpr::Collect(prop) => {
            let col = Expr::col(&prop.node, &prop.property);
            match if_cond {
                Some(cond) => Expr::func(expr.function().as_sql_if(), vec![col, cond.clone()]),
                None => Expr::func(expr.function().as_sql(), vec![col]),
            }
        }
    }
}

/// `COUNT(node.prop)` needs the column in FROM. A skipped node's table is not
/// scanned, so the count degrades to `COUNT()`, unless a denormalized scan
/// carries the node, in which case the rebind supplies the column.
fn counted_column(plan: &Plan, target: &TargetRef) -> Option<Expr> {
    let property = target.property.as_deref()?;
    let skipped = plan
        .nodes
        .get(target.node.as_str())
        .is_some_and(|np| matches!(np.hydration, HydrationStrategy::Skip));
    let denormalized = plan.hops.iter().any(|h| {
        h.denormalized.is_some() && (h.from_node == target.node || h.to_node == target.node)
    });
    (!skipped || denormalized).then(|| Expr::col(&target.node, property))
}

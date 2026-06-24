//! Keyset pagination helpers for top-level query rows.

use serde_json::Value;

use ontology::constants::DEFAULT_PRIMARY_KEY;

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::{
    InputAggSort, InputAggregationMetric, InputCursor, InputGroupByKey, InputOrderBy,
    OrderDirection, cursor_column, decode_cursor_values,
};
use crate::passes::plan::Plan;
use crate::passes::shared::data_type_to_ch;

#[derive(Debug, Clone)]
pub(super) struct CursorKey {
    pub(super) expr: Expr,
    alias: String,
    ch_type: ChType,
    desc: bool,
}

impl CursorKey {
    pub(super) fn new(expr: Expr, ch_type: ChType, desc: bool, index: usize) -> Self {
        Self {
            expr,
            alias: cursor_column(index),
            ch_type,
            desc,
        }
    }
}

pub(super) fn traversal_keys(plan: &Plan) -> Vec<CursorKey> {
    let mut keys = Vec::new();
    if let Some(ob) = &plan.order_by {
        keys.push(order_by_key(ob, keys.len()));
    }

    let mut aliases: Vec<&String> = plan.nodes.keys().collect();
    aliases.sort();
    for alias in aliases {
        if plan
            .order_by
            .as_ref()
            .is_some_and(|ob| ob.node == *alias && ob.property == DEFAULT_PRIMARY_KEY)
        {
            continue;
        }
        let expr = plan
            .node_edge_mappings
            .get(alias)
            .map(|(edge_alias, edge_column)| Expr::col(edge_alias, edge_column))
            .unwrap_or_else(|| Expr::col(alias, DEFAULT_PRIMARY_KEY));
        keys.push(CursorKey::new(expr, ChType::Int64, false, keys.len()));
    }
    keys
}

pub(super) fn aggregation_keys(
    aggregations: &[InputAggregationMetric],
    group_by_keys: &[InputGroupByKey],
    group_by_names: &[String],
    select: &[SelectExpr],
    agg_sort: Option<&InputAggSort>,
) -> Vec<CursorKey> {
    let mut keys = Vec::new();
    if let Some(sort) = agg_sort {
        let ch_type = aggregations
            .iter()
            .find(|agg| aggregation_alias(agg) == sort.column)
            .map(aggregation_type)
            .unwrap_or(ChType::String);
        let expr = select_expr(select, &sort.column).unwrap_or_else(|| Expr::ident(&sort.column));
        keys.push(CursorKey::new(
            expr,
            ch_type,
            sort.direction == OrderDirection::Desc,
            keys.len(),
        ));
    }

    for (group, name) in group_by_keys.iter().zip(group_by_names) {
        match group {
            InputGroupByKey::Node { node, .. } => {
                keys.push(CursorKey::new(
                    Expr::col(node, DEFAULT_PRIMARY_KEY),
                    ChType::Int64,
                    false,
                    keys.len(),
                ));
            }
            InputGroupByKey::Property { .. } => {
                let expr = select_expr(select, name).unwrap_or_else(|| Expr::ident(name));
                keys.push(CursorKey::new(expr, ChType::String, false, keys.len()));
            }
        }
    }

    keys
}

pub(super) fn apply_keyset(
    query: &mut Query,
    keys: &[CursorKey],
    cursor: Option<&InputCursor>,
    seek_in_having: bool,
) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }

    for key in keys {
        query
            .select
            .push(SelectExpr::new(key.expr.clone(), key.alias.clone()));
    }
    query.order_by = keys
        .iter()
        .map(|key| {
            if key.desc {
                OrderExpr::desc(Expr::ident(&key.alias))
            } else {
                OrderExpr::asc(Expr::ident(&key.alias))
            }
        })
        .collect();

    let Some(after) = cursor.and_then(|c| c.after.as_deref()) else {
        return Ok(());
    };
    let values = decode_cursor_values(after)
        .map_err(|e| QueryError::Validation(format!("invalid pagination cursor: {e}")))?;
    if values.len() != keys.len() {
        return Err(QueryError::Validation(format!(
            "invalid pagination cursor: expected {} values, got {}",
            keys.len(),
            values.len()
        )));
    }

    let seek = seek_predicate(keys, &values);
    if seek_in_having {
        query.having = append_condition(query.having.take(), seek);
    } else {
        query.where_clause = append_condition(query.where_clause.take(), seek);
    }

    Ok(())
}

fn order_by_key(order_by: &InputOrderBy, index: usize) -> CursorKey {
    CursorKey::new(
        Expr::col(&order_by.node, &order_by.property),
        data_type_to_ch(order_by.data_type.as_ref()),
        order_by.direction == OrderDirection::Desc,
        index,
    )
}

fn aggregation_alias(agg: &InputAggregationMetric) -> String {
    agg.alias
        .clone()
        .unwrap_or_else(|| agg.function.to_string())
}

fn aggregation_type(agg: &InputAggregationMetric) -> ChType {
    match agg.function {
        crate::input::AggFunction::Avg => ChType::Float64,
        crate::input::AggFunction::Count => ChType::Int64,
        crate::input::AggFunction::Sum
        | crate::input::AggFunction::Min
        | crate::input::AggFunction::Max => ChType::Int64,
        crate::input::AggFunction::Collect => ChType::String,
    }
}

fn select_expr(select: &[SelectExpr], alias: &str) -> Option<Expr> {
    select
        .iter()
        .find(|item| item.alias.as_deref() == Some(alias))
        .map(|item| item.expr.clone())
}

fn seek_predicate(keys: &[CursorKey], values: &[Value]) -> Expr {
    let disjuncts = (0..keys.len()).map(|index| {
        let mut conjuncts = Vec::with_capacity(index + 1);
        for previous in 0..index {
            conjuncts.push(compare_key(
                &keys[previous],
                values[previous].clone(),
                Op::Eq,
            ));
        }
        let op = if keys[index].desc { Op::Lt } else { Op::Gt };
        conjuncts.push(compare_key(&keys[index], values[index].clone(), op));
        Expr::conjoin(conjuncts).expect("seek disjunct is non-empty")
    });

    Expr::or_all(disjuncts.map(Some)).expect("seek predicate is non-empty")
}

fn compare_key(key: &CursorKey, value: Value, op: Op) -> Expr {
    Expr::binary(op, key.expr.clone(), Expr::param(key.ch_type, value))
}

fn append_condition(existing: Option<Expr>, condition: Expr) -> Option<Expr> {
    Some(match existing {
        Some(existing) => Expr::and(existing, condition),
        None => condition,
    })
}

use ontology::constants::{DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN};
use orbit_utils::query_types::SqlType;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Relation, TextMatch, UnaryOp,
};
use serde_json::Value;

use super::{Built, and, binary, call, col, conjoin, eq, param};
use crate::input::{FilterOp, InputFilter};
use crate::passes::lower::sql::parameter_type;
use crate::passes::plan::filters::NullAsEpoch;
use crate::passes::plan::predicates::Column;

pub(crate) struct Filter<'a> {
    pub column: &'a Column,
    pub filter: &'a InputFilter,
    pub data_type: Option<&'a ontology::DataType>,
    pub in_sort_key: bool,
    pub text_indexed: bool,
    pub null_as_epoch: NullAsEpoch,
    pub negated: bool,
}

fn epoch<B: RelationalBackend>(data_type: ontology::DataType) -> Expr {
    let value = match data_type {
        ontology::DataType::Date => "1970-01-01",
        _ => "1970-01-01 00:00:00",
    };
    param::<B>(parameter_type(Some(&data_type)), value.into())
}

pub(crate) fn null_if_epoch<B: RelationalBackend>(
    value: Expr,
    data_type: ontology::DataType,
) -> Expr {
    call(Function::NullIf, vec![value, epoch::<B>(data_type)])
}

fn read<B: RelationalBackend>(column: Expr, null_as_epoch: Option<ontology::DataType>) -> Expr {
    match null_as_epoch {
        Some(data_type) => null_if_epoch::<B>(column, data_type),
        None => column,
    }
}

fn unary(op: UnaryOp, value: Expr) -> Expr {
    Expr::Unary {
        op,
        value: Box::new(value),
    }
}

pub(crate) fn filter<B: RelationalBackend>(scope: &[&Relation], f: Filter<'_>) -> Built<Expr> {
    let raw = col(scope, &f.column.source, &f.column.name)?;
    let epochs = f.null_as_epoch;
    if let Some((alias, property)) = &f.filter.rhs_column {
        let rhs = read::<B>(col(scope, alias, property)?, epochs.rhs);
        let left = read::<B>(raw, epochs.column);
        return Ok(comparison(left, f.filter.op.unwrap_or(FilterOp::Eq), rhs)
            .expect("validated property comparison"));
    }
    let value = read::<B>(raw.clone(), epochs.column.filter(|_| f.negated));
    let given = || f.filter.value.clone().unwrap_or(Value::Null);
    let data_type = parameter_type(f.data_type);
    let op = f.filter.op.unwrap_or(FilterOp::Eq);
    let expression = match op {
        FilterOp::IsNull if let Some(epoch_type) = epochs.column => {
            return Ok(eq(raw, epoch::<B>(epoch_type)));
        }
        FilterOp::IsNotNull if let Some(epoch_type) = epochs.column => {
            return Ok(binary(BinaryOp::Ne, raw, epoch::<B>(epoch_type)));
        }
        op @ (FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte) => {
            comparison(value, op, param::<B>(data_type, given())).expect("comparison operator")
        }
        FilterOp::In => {
            let values = f
                .filter
                .value
                .as_ref()
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            in_list::<B>(value, data_type, values).expect("validated non-empty IN list")
        }
        FilterOp::IsNull => unary(UnaryOp::IsNull, value),
        FilterOp::IsNotNull => unary(UnaryOp::IsNotNull, value),
        op => {
            let fold = |expr: Expr| match f.in_sort_key {
                true => expr,
                false => call(Function::Lower, vec![expr]),
            };
            let text = f.filter.value_str().unwrap_or("").to_string();
            let needle = fold(param::<B>(SqlType::String, text.into()));
            let value = fold(value);
            let mode = match op {
                FilterOp::Contains => TextMatch::Contains,
                FilterOp::TokenMatch => TextMatch::Token,
                FilterOp::AllTokens => TextMatch::AllTokens,
                FilterOp::AnyTokens => TextMatch::AnyTokens,
                FilterOp::StartsWith => return Ok(call(Function::StartsWith, vec![value, needle])),
                FilterOp::EndsWith => return Ok(call(Function::EndsWith, vec![value, needle])),
                _ => unreachable!(),
            };
            Expr::TextSearch {
                mode,
                value: Box::new(value),
                query: Box::new(needle),
            }
        }
    };
    let expression = match epochs.column {
        Some(epoch_type) if !f.negated => and(
            expression,
            binary(BinaryOp::Ne, raw.clone(), epoch::<B>(epoch_type)),
        ),
        _ => expression,
    };
    if !(f.text_indexed && matches!(op, FilterOp::Eq | FilterOp::In)) {
        return Ok(expression);
    }
    let values = match given() {
        Value::Array(values) => values,
        value => vec![value],
    };
    let lower = |expr| call(Function::Lower, vec![expr]);
    let mut candidates: Vec<_> = values
        .into_iter()
        .map(|value| lower(param::<B>(data_type, value)))
        .collect();
    let matched = match candidates.len() {
        1 => eq(lower(raw), candidates.remove(0)),
        _ => binary(BinaryOp::In, lower(raw), call(Function::Tuple, candidates)),
    };
    Ok(and(expression, matched))
}

pub(crate) fn comparison(left: Expr, operator: FilterOp, right: Expr) -> Option<Expr> {
    let op = match operator {
        FilterOp::Eq => BinaryOp::Eq,
        FilterOp::Ne => BinaryOp::Ne,
        FilterOp::Gt => BinaryOp::Gt,
        FilterOp::Gte => BinaryOp::Ge,
        FilterOp::Lt => BinaryOp::Lt,
        FilterOp::Lte => BinaryOp::Le,
        _ => return None,
    };
    Some(binary(op, left, right))
}

pub(crate) fn in_list<B: RelationalBackend>(
    value: Expr,
    data_type: SqlType,
    values: Vec<Value>,
) -> Option<Expr> {
    match values.len() {
        0 => None,
        1 => Some(eq(value, param::<B>(data_type, values.into_iter().next()?))),
        _ => Some(binary(
            BinaryOp::In,
            value,
            param::<B>(data_type.to_array(), Value::Array(values)),
        )),
    }
}

pub(crate) fn ids<B: RelationalBackend>(value: Expr, ids: &[i64]) -> Expr {
    match ids {
        [id] => eq(value, super::int(*id)),
        _ => {
            let values = ids.iter().map(|id| Value::from(*id)).collect();
            in_list::<B>(value, SqlType::Int64, values)
                .unwrap_or_else(|| param::<B>(SqlType::Bool, false.into()))
        }
    }
}

pub(crate) fn id_range(value: Expr, start: i64, end: i64) -> Expr {
    and(
        binary(BinaryOp::Ge, value.clone(), super::int(start)),
        binary(BinaryOp::Le, value, super::int(end)),
    )
}

pub(crate) fn live<B: RelationalBackend>(scope: &[&Relation], alias: &str) -> Built<Expr> {
    if super::stored::<B>() {
        return Ok(Expr::Bool(true));
    }
    let deleted = col(scope, alias, DELETED_COLUMN)?;
    Ok(eq(deleted, param::<B>(SqlType::Bool, false.into())))
}

pub(crate) fn relationship_kinds<B: RelationalBackend>(
    scope: &[&Relation],
    alias: &str,
    kinds: &[String],
) -> Built<Option<Expr>> {
    if crate::passes::normalize::is_wildcard(kinds) {
        return Ok(None);
    }
    let kind = col(scope, alias, RELATIONSHIP_KIND_COLUMN)?;
    Ok(match kinds {
        [only] => Some(eq(kind, super::string(only))),
        _ => {
            let values = kinds.iter().cloned().map(Value::String).collect();
            in_list::<B>(kind, SqlType::String, values)
        }
    })
}

pub(crate) fn tags(column: Expr, values: &[String]) -> Expr {
    match values {
        [value] => call(Function::ArrayContains, vec![column, super::string(value)]),
        _ => {
            let values = values.iter().map(super::string).collect();
            call(
                Function::ArrayContainsAny,
                vec![column, call(Function::Array, values)],
            )
        }
    }
}

pub(crate) fn conjoined(parts: Vec<Expr>) -> Expr {
    conjoin(parts).expect("validated non-empty condition group")
}

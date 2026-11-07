use ontology::constants::{DELETED_COLUMN, RELATIONSHIP_KIND_COLUMN};
use orbit_utils::query_types::{SqlType, TimeZone};
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Relation, TextMatch, UnaryOp,
};
use serde_json::Value;

use query_data_model::plan::logical::{Comparison, Matching};

use super::{Built, and, binary, call, col, conjoin, eq, param};
use crate::passes::plan::filters::{NullAsEpoch, Test, json};
use crate::passes::plan::predicates::Column;

pub(crate) struct Filter<'a> {
    pub column: &'a Column,
    pub test: &'a Test,
    pub data_type: Option<&'a ontology::DataType>,
    pub in_sort_key: bool,
    pub text_indexed: bool,
    pub null_as_epoch: NullAsEpoch,
    pub negated: bool,
}

pub(crate) fn parameter_type(data_type: Option<&ontology::DataType>) -> SqlType {
    match data_type {
        Some(ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid) => {
            SqlType::String
        }
        Some(ontology::DataType::Int) => SqlType::Int64,
        Some(ontology::DataType::Float) => SqlType::Float64,
        Some(ontology::DataType::Bool) => SqlType::Bool,
        Some(ontology::DataType::Date) => SqlType::Date,
        Some(ontology::DataType::DateTime) => SqlType::Timestamp {
            precision: 6,
            timezone: Some(TimeZone::Utc),
        },
        None => SqlType::String,
    }
}

pub(crate) fn epoch<B: RelationalBackend>(data_type: ontology::DataType) -> Expr {
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
    let value = read::<B>(raw.clone(), epochs.column.filter(|_| f.negated));
    let data_type = parameter_type(f.data_type);
    let expression = match f.test {
        Test::Column(comparison, rhs) => {
            let rhs = read::<B>(col(scope, &rhs.source, &rhs.name)?, epochs.rhs);
            let left = read::<B>(raw, epochs.column);
            return Ok(binary(binary_op(*comparison), left, rhs));
        }
        Test::Null(null) => match (null, epochs.column) {
            (true, Some(epoch_type)) => return Ok(eq(raw, epoch::<B>(epoch_type))),
            (false, Some(epoch_type)) => {
                return Ok(binary(BinaryOp::Ne, raw, epoch::<B>(epoch_type)));
            }
            (true, None) => unary(UnaryOp::IsNull, value),
            (false, None) => unary(UnaryOp::IsNotNull, value),
        },
        Test::Compare(comparison, literal) => binary(
            binary_op(*comparison),
            value,
            param::<B>(data_type, json(literal)),
        ),
        Test::In(values) => {
            let values = values.iter().map(json).collect();
            in_list::<B>(value, data_type, values).expect("validated non-empty IN list")
        }
        Test::Text(matching, needle) => {
            let fold = |expr: Expr| match f.in_sort_key {
                true => expr,
                false => call(Function::Lower, vec![expr]),
            };
            let needle = fold(param::<B>(SqlType::String, needle.clone().into()));
            let value = fold(value);
            let mode = match matching {
                Matching::Contains => TextMatch::Contains,
                Matching::Token => TextMatch::Token,
                Matching::AllTokens => TextMatch::AllTokens,
                Matching::AnyTokens => TextMatch::AnyTokens,
                Matching::StartsWith => return Ok(call(Function::StartsWith, vec![value, needle])),
                Matching::EndsWith => return Ok(call(Function::EndsWith, vec![value, needle])),
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
    if !(f.text_indexed && f.test.matches_values()) {
        return Ok(expression);
    }
    let lower = |expr| call(Function::Lower, vec![expr]);
    let mut candidates: Vec<_> = f
        .test
        .literals()
        .iter()
        .map(|literal| lower(param::<B>(data_type, json(literal))))
        .collect();
    let matched = match candidates.len() {
        1 => eq(lower(raw), candidates.remove(0)),
        _ => binary(BinaryOp::In, lower(raw), call(Function::Tuple, candidates)),
    };
    Ok(and(expression, matched))
}

fn binary_op(comparison: Comparison) -> BinaryOp {
    match comparison {
        Comparison::Eq => BinaryOp::Eq,
        Comparison::Ne => BinaryOp::Ne,
        Comparison::Lt => BinaryOp::Lt,
        Comparison::Le => BinaryOp::Le,
        Comparison::Gt => BinaryOp::Gt,
        Comparison::Ge => BinaryOp::Ge,
    }
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
    if crate::input::is_wildcard(kinds) {
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

use orbit_authorization::Key;
use orbit_utils::query_types::{SqlType, TimeZone};
use query_data_model::Catalog;
use query_data_model::constants::DELETED_COLUMN;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Expr, Function, Relation, RelationalError, TextMatch, UnaryOp,
};
use serde_json::Value;

use query_data_model::plan::logical::{Comparison, Matching, Operand, Predicate};

use super::{Built, and, binary, call, col, conjoin, eq, param};

use crate::passes::plan::filters::BoundFilter;
use crate::passes::plan::predicates::Column;

pub(crate) fn parameter_type(data_type: Option<&query_data_model::DataType>) -> SqlType {
    match data_type {
        Some(
            query_data_model::DataType::String
            | query_data_model::DataType::Enum
            | query_data_model::DataType::Uuid,
        ) => SqlType::String,
        Some(query_data_model::DataType::Int) => SqlType::Int64,
        Some(query_data_model::DataType::Float) => SqlType::Float64,
        Some(query_data_model::DataType::Bool) => SqlType::Bool,
        Some(query_data_model::DataType::Date) => SqlType::Date,
        Some(query_data_model::DataType::DateTime) => SqlType::Timestamp {
            precision: 6,
            timezone: Some(TimeZone::Utc),
        },
        None => SqlType::String,
    }
}

pub(crate) fn epoch<B: RelationalBackend>(data_type: query_data_model::DataType) -> Expr {
    let value = match data_type {
        query_data_model::DataType::Date => "1970-01-01",
        _ => "1970-01-01 00:00:00",
    };
    param::<B>(parameter_type(Some(&data_type)), value.into())
}

pub(crate) fn null_if_epoch<B: RelationalBackend>(
    value: Expr,
    data_type: query_data_model::DataType,
) -> Expr {
    call(Function::NullIf, vec![value, epoch::<B>(data_type)])
}

fn read<B: RelationalBackend>(
    column: Expr,
    null_as_epoch: Option<query_data_model::DataType>,
) -> Expr {
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

/// The one lowering of a filter leaf, under `negated` NOTs that stay above it. Null-as-epoch
/// columns and the lower() text-index twin are applied here only.
pub(crate) fn filter<B: RelationalBackend>(
    catalog: &Catalog<B>,
    scope: &[&Relation],
    (column, rhs): (&Column, Option<&Column>),
    f: &BoundFilter,
    negated: bool,
) -> Built<Expr> {
    let raw = col(scope, &column.source, &column.name)?;
    let epochs = f.null_as_epoch;
    let value = read::<B>(raw.clone(), epochs.column.filter(|_| negated));
    let data_type = parameter_type(f.data_type.as_ref());
    let expression = match &f.leaf {
        Predicate::Compare {
            comparison,
            right: Operand::Expr(_),
            ..
        } => {
            let rhs = rhs.ok_or(RelationalError::Incomplete("a compared column"))?;
            let rhs = read::<B>(col(scope, &rhs.source, &rhs.name)?, epochs.rhs);
            let left = read::<B>(raw, epochs.column);
            return Ok(binary(binary_op(*comparison), left, rhs));
        }
        Predicate::Null { null, .. } => match (null, epochs.column) {
            (true, Some(epoch_type)) => return Ok(eq(raw, epoch::<B>(epoch_type))),
            (false, Some(epoch_type)) => {
                return Ok(binary(BinaryOp::Ne, raw, epoch::<B>(epoch_type)));
            }
            (true, None) => unary(UnaryOp::IsNull, value),
            (false, None) => unary(UnaryOp::IsNotNull, value),
        },
        Predicate::Compare {
            comparison,
            right: Operand::Literal(literal),
            ..
        } => binary(
            binary_op(*comparison),
            value,
            param::<B>(data_type, literal.into()),
        ),
        Predicate::In { values, .. } => {
            let values = values.iter().map(Value::from).collect();
            in_list::<B>(value, data_type, values).expect("validated non-empty IN list")
        }
        Predicate::InTuple { left, values } => {
            if f.tuple.len() != left.len() {
                return Err(RelationalError::Incomplete(
                    "a stored column for each tuple part",
                ));
            }
            let columns = f
                .tuple
                .iter()
                .map(|(stored, _)| col(scope, &column.source, catalog.column_name(*stored)))
                .collect::<Built<Vec<_>>>()?;
            let tuple = |literals: &Vec<query_data_model::plan::logical::Literal>| {
                Expr::List(
                    literals
                        .iter()
                        .zip(&f.tuple)
                        .map(|(literal, (_, data_type))| {
                            param::<B>(parameter_type(Some(data_type)), literal.into())
                        })
                        .collect(),
                )
            };
            let left = Expr::List(columns);
            match &values[..] {
                [one] => eq(left, tuple(one)),
                _ => binary(
                    BinaryOp::In,
                    left,
                    Expr::List(values.iter().map(tuple).collect()),
                ),
            }
        }
        Predicate::Text {
            matching, needle, ..
        } => {
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
        Predicate::And(_) | Predicate::Not(_) => {
            return Err(RelationalError::Incomplete("a filter leaf"));
        }
    };
    let expression = match epochs.column {
        Some(epoch_type) if !negated => and(
            expression,
            binary(BinaryOp::Ne, raw.clone(), epoch::<B>(epoch_type)),
        ),
        _ => expression,
    };
    if !(f.text_indexed && f.matches_values()) {
        return Ok(expression);
    }
    let lower = |expr| call(Function::Lower, vec![expr]);
    let mut candidates: Vec<_> = f
        .leaf
        .literals()
        .iter()
        .map(|literal| lower(param::<B>(data_type, literal.into())))
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

/// A key as the value of the one identity column it matches; a composite key fills no single
/// column.
pub(crate) fn key_value(key: &Key) -> Built<(SqlType, Value)> {
    match key {
        Key::Int(key) => Ok((SqlType::Int64, Value::from(*key))),
        Key::Text(key) | Key::Uuid(key) => Ok((SqlType::String, Value::from(key.as_str()))),
        Key::Composite(_) => Err(RelationalError::Unsupported("composite identity keys")),
    }
}

pub(crate) fn id_list<B: RelationalBackend>(value: Expr, ids: &[Key]) -> Built<Expr> {
    let values = ids.iter().map(key_value).collect::<Built<Vec<_>>>()?;
    let data_type = values
        .first()
        .map_or(SqlType::Int64, |(data_type, _)| *data_type);
    let values = values.into_iter().map(|(_, value)| value).collect();
    Ok(in_list::<B>(value, data_type, values)
        .unwrap_or_else(|| param::<B>(SqlType::Bool, false.into())))
}

/// The rows whose identity columns hold one of `ids`: the one column's IN list, or a tuple IN
/// over each part of a composite identity.
pub(crate) fn key_list<B: RelationalBackend>(columns: Vec<Expr>, ids: &[Key]) -> Built<Expr> {
    let [_, _, ..] = columns.as_slice() else {
        let column = columns
            .into_iter()
            .next()
            .ok_or(RelationalError::Incomplete("an identity column"))?;
        return id_list::<B>(column, ids);
    };
    let tuple = |key: &Key| -> Built<Expr> {
        let Key::Composite(parts) = key else {
            return Err(RelationalError::Unsupported(
                "a single key on a composite identity",
            ));
        };
        if parts.len() != columns.len() {
            return Err(RelationalError::Incomplete(
                "one key part per identity column",
            ));
        }
        let values = parts
            .iter()
            .map(|part| key_value(part).map(|(data_type, value)| param::<B>(data_type, value)))
            .collect::<Built<Vec<_>>>()?;
        Ok(Expr::List(values))
    };
    let mut tuples = ids.iter().map(tuple).collect::<Built<Vec<_>>>()?;
    let left = Expr::List(columns);
    Ok(match tuples.len() {
        0 => param::<B>(SqlType::Bool, false.into()),
        1 => eq(left, tuples.remove(0)),
        _ => binary(BinaryOp::In, left, Expr::List(tuples)),
    })
}

pub(crate) fn ids<B: RelationalBackend>(value: Expr, ids: &[Key]) -> Built<Expr> {
    match ids {
        [Key::Int(id)] => Ok(eq(value, super::int(*id))),
        _ => id_list::<B>(value, ids),
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
    column: &Column,
    kinds: &[String],
) -> Built<Option<Expr>> {
    if kinds.is_empty() {
        return Ok(None);
    }
    let kind = col(scope, &column.source, &column.name)?;
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

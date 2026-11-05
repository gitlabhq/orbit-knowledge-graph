use serde_json::{Map, Value};

use crate::plan::logical::{Comparison, Expr, Literal, Matching, Predicate};
use crate::surface::SurfaceError;

pub(super) fn conditions(
    filters: &Map<String, Value>,
    mut column: impl FnMut(&str) -> Result<Expr, SurfaceError>,
) -> Result<Vec<Predicate>, SurfaceError> {
    let mut predicates = Vec::new();
    for (property, entry) in filters {
        let left = column(property)?;
        for (op, value) in operators(entry) {
            predicates.push(condition(left, property, op, value)?);
        }
    }
    Ok(predicates)
}

fn object(map: &Map<String, Value>) -> Vec<(&str, &Value)> {
    map.iter().map(|(op, value)| (op.as_str(), value)).collect()
}

fn operators(entry: &Value) -> Vec<(&str, &Value)> {
    match entry {
        Value::Object(map) => object(map),
        Value::Array(items) if items.iter().any(Value::is_object) => items
            .iter()
            .filter_map(Value::as_object)
            .flat_map(object)
            .collect(),
        value => vec![("eq", value)],
    }
}

fn condition(
    left: Expr,
    property: &str,
    op: &str,
    value: &Value,
) -> Result<Predicate, SurfaceError> {
    let comparison = |comparison| Ok(left.compare(comparison, literal(property, value)?));
    let pinned =
        |value| literal(property, value).map(|value| crate::surface::pinned(property, value));
    let text = |matching| match value {
        Value::String(needle) => Ok(left.matches(matching, needle.clone())),
        _ => Err(not_text(property, value)),
    };
    match op {
        "eq" => Ok(left.eq(pinned(value)?)),
        "ne" => comparison(Comparison::Ne),
        "gt" => comparison(Comparison::Gt),
        "lt" => comparison(Comparison::Lt),
        "gte" => comparison(Comparison::Ge),
        "lte" => comparison(Comparison::Le),
        "in" => match value {
            Value::Array(values) => {
                Ok(left.one_of(values.iter().map(pinned).collect::<Result<Vec<_>, _>>()?))
            }
            _ => Err(SurfaceError::schema(format!(
                "filter on \"{property}\": \"in\" requires an array value"
            ))),
        },
        "contains" => text(Matching::Contains),
        "starts_with" => text(Matching::StartsWith),
        "ends_with" => text(Matching::EndsWith),
        "token_match" => text(Matching::Token),
        "all_tokens" => text(Matching::AllTokens),
        "any_tokens" => text(Matching::AnyTokens),
        "is_null" | "is_not_null" => match value {
            Value::Bool(apply) => Ok(left.is_null((op == "is_null") == *apply)),
            _ => Err(SurfaceError::schema(format!(
                "filter on \"{property}\": \"{op}\" takes a boolean value"
            ))),
        },
        unknown => Err(SurfaceError::schema(format!(
            "filter on \"{property}\": unknown operator \"{unknown}\""
        ))),
    }
}

pub(super) fn literal(property: &str, value: &Value) -> Result<Literal, SurfaceError> {
    match value {
        Value::String(text) => Ok(Literal::String(text.clone())),
        Value::Bool(flag) => Ok(Literal::Bool(*flag)),
        Value::Number(number) => number
            .as_i64()
            .map(Literal::Int)
            .or_else(|| (!number.is_u64()).then(|| number.as_f64().map(Literal::Float))?)
            .ok_or_else(|| {
                SurfaceError::schema(format!(
                    "filter on \"{property}\": {number} is out of range for a signed 64-bit integer"
                ))
            }),
        other => Err(not_scalar(property, other)),
    }
}

fn not_scalar(property: &str, value: &Value) -> SurfaceError {
    SurfaceError::schema(format!(
        "filter on \"{property}\": {value} is not a scalar value"
    ))
}

fn not_text(property: &str, value: &Value) -> SurfaceError {
    SurfaceError::schema(format!("filter on \"{property}\": {value} is not a string"))
}

pub(super) fn id(value: &Value) -> Result<i64, SurfaceError> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| SurfaceError::schema(format!("{value} is not a signed 64-bit node id")))
}

use crate::ast::*;
use crate::error::{QueryError, Result};
use crate::input::*;
use crate::passes::plan::BoundFilter;
use crate::passes::plan::helpers::denorm_tag_values;
use query_data_model::bindings::ColumnRef;

pub fn filter_to_expr(column: ColumnRef, rhs: Option<ColumnRef>, bound: &BoundFilter) -> Expr {
    filter_expression(column, rhs, &bound.filter, bound.data_type.as_ref())
}

pub(crate) fn filter_expression(
    column: ColumnRef,
    rhs: Option<ColumnRef>,
    filter: &InputFilter,
    data_type: Option<&ontology::DataType>,
) -> Expr {
    let col = Expr::Column(column);
    if let Some(rhs) = rhs {
        return comparison(col, filter.op.unwrap_or(FilterOp::Eq), Expr::Column(rhs))
            .expect("validated property comparison");
    }

    let val = || filter.value.clone().unwrap_or(serde_json::Value::Null);
    let typed = |v: serde_json::Value| -> Expr { Expr::param(parameter_type(data_type), v) };

    match filter.op.unwrap_or(FilterOp::Eq) {
        op @ (FilterOp::Eq
        | FilterOp::Ne
        | FilterOp::Gt
        | FilterOp::Gte
        | FilterOp::Lt
        | FilterOp::Lte) => comparison(col, op, typed(val())).expect("comparison operator"),
        FilterOp::In => {
            if let Some(arr) = filter.value.as_ref().and_then(|v| v.as_array()) {
                Expr::col_in(column, parameter_type(data_type), arr.clone())
                    .unwrap_or_else(|| Expr::param(SqlType::Bool, false))
            } else {
                Expr::param(SqlType::Bool, false)
            }
        }
        FilterOp::IsNull => Expr::unary(Op::IsNull, col),
        FilterOp::IsNotNull => Expr::unary(Op::IsNotNull, col),
        op @ (FilterOp::TokenMatch | FilterOp::AllTokens | FilterOp::AnyTokens) => {
            Expr::TokenSearch {
                mode: match op {
                    FilterOp::TokenMatch => TokenMatchMode::Single,
                    FilterOp::AllTokens => TokenMatchMode::All,
                    _ => TokenMatchMode::Any,
                },
                value: Box::new(col),
                query: Box::new(Expr::param(
                    SqlType::String,
                    filter
                        .value
                        .as_ref()
                        .and_then(|value| value.as_str())
                        .unwrap_or(""),
                )),
            }
        }
        op => {
            let function = match op {
                FilterOp::Contains => Function::ContainsInsensitive,
                FilterOp::StartsWith => Function::StartsWith,
                FilterOp::EndsWith => Function::EndsWith,
                _ => unreachable!(),
            };
            let value = filter
                .value
                .as_ref()
                .and_then(|value| value.as_str())
                .unwrap_or("");
            Expr::func(function, vec![col, Expr::param(SqlType::String, value)])
        }
    }
}

pub fn comparison(left: Expr, operator: FilterOp, right: Expr) -> Result<Expr> {
    let operator = match operator {
        FilterOp::Eq => Op::Eq,
        FilterOp::Ne => Op::Ne,
        FilterOp::Gt => Op::Gt,
        FilterOp::Gte => Op::Ge,
        FilterOp::Lt => Op::Lt,
        FilterOp::Lte => Op::Le,
        _ => {
            return Err(QueryError::Lowering("invalid property comparison".into()));
        }
    };
    Ok(Expr::binary(operator, left, right))
}

pub fn id_list_predicate(column: ColumnRef, ids: &[i64]) -> Expr {
    if ids.len() == 1 {
        Expr::eq(Expr::Column(column), Expr::int(ids[0]))
    } else {
        Expr::col_in(
            column,
            SqlType::Int64,
            ids.iter().map(|id| serde_json::Value::from(*id)).collect(),
        )
        .unwrap_or_else(|| Expr::param(SqlType::Bool, false))
    }
}

pub fn id_range_predicate(column: ColumnRef, range: &InputIdRange) -> Expr {
    Expr::and(
        Expr::binary(Op::Ge, Expr::Column(column), Expr::int(range.start)),
        Expr::binary(Op::Le, Expr::Column(column), Expr::int(range.end)),
    )
}

pub fn parameter_type(dt: Option<&ontology::DataType>) -> SqlType {
    match dt {
        Some(ontology::DataType::String | ontology::DataType::Enum | ontology::DataType::Uuid) => {
            SqlType::String
        }
        Some(ontology::DataType::Int) => SqlType::Int64,
        Some(ontology::DataType::Float) => SqlType::Float64,
        Some(ontology::DataType::Bool) => SqlType::Bool,
        Some(ontology::DataType::DateTime | ontology::DataType::Date) => SqlType::Timestamp {
            precision: 6,
            timezone: Some(TimeZone::Utc),
        },
        None => SqlType::String,
    }
}

pub fn deleted_false(column: ColumnRef) -> Expr {
    Expr::eq(Expr::Column(column), Expr::param(SqlType::Bool, false))
}

pub fn rel_kind_filter(column: ColumnRef, types: &[String]) -> Option<Expr> {
    if crate::passes::normalize::is_wildcard(types) {
        return None;
    }
    if types.len() == 1 {
        Some(Expr::eq(Expr::Column(column), Expr::string(&types[0])))
    } else {
        Expr::col_in(
            column,
            SqlType::String,
            types
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect(),
        )
    }
}

/// Returns `None` for unsupported filter ops.
pub fn denorm_tag_expr(column: ColumnRef, tag_key: &str, filter: &InputFilter) -> Option<Expr> {
    denorm_tag_values(tag_key, filter).map(|values| tag_membership(column, &values))
}

pub(crate) fn tag_membership(column: ColumnRef, values: &[String]) -> Expr {
    if let [value] = values {
        Expr::func(
            Function::ArrayContains,
            vec![Expr::Column(column), Expr::string(value)],
        )
    } else {
        Expr::func(
            Function::ArrayContainsAny,
            vec![
                Expr::Column(column),
                Expr::func(Function::Array, values.iter().map(Expr::string).collect()),
            ],
        )
    }
}

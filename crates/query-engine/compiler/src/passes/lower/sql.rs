use crate::ast::*;

fn epoch(data_type: ontology::DataType) -> Expr {
    let value = match data_type {
        ontology::DataType::Date => "1970-01-01",
        _ => "1970-01-01 00:00:00",
    };
    Expr::param(parameter_type(Some(&data_type)), value)
}

pub(super) fn null_if_epoch(value: Expr, data_type: ontology::DataType) -> Expr {
    Expr::func(Function::NullIf, vec![value, epoch(data_type)])
}

pub fn parameter_type(dt: Option<&ontology::DataType>) -> SqlType {
    match dt {
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

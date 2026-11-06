use super::sql;
use crate::ast::*;
use crate::passes::plan::predicates::{Column, OutputValue};

pub(super) fn column(value: &Column) -> Expr {
    Expr::col(&value.source, &value.name)
}

fn step(id: &Column, kind: &Column) -> Expr {
    Expr::func(
        Function::Array,
        vec![Expr::func(Function::Tuple, vec![column(id), column(kind)])],
    )
}

pub(super) fn output_value(value: &OutputValue) -> Expr {
    match value {
        OutputValue::Column(value) => column(value),
        OutputValue::EpochAsNull(value, data_type) => sql::null_if_epoch(column(value), *data_type),
        OutputValue::Text(value) => Expr::string(value),
        OutputValue::Depth(value) => Expr::int(i64::from(*value)),
        OutputValue::Path(steps) => Expr::func(
            Function::Array,
            steps
                .iter()
                .map(|(id, kind)| Expr::func(Function::Tuple, vec![column(id), column(kind)]))
                .collect(),
        ),
        OutputValue::PathAppend { path, id, kind } => {
            Expr::func(Function::ArrayConcat, vec![column(path), step(id, kind)])
        }
        OutputValue::PathPrepend { path, id, kind } => {
            Expr::func(Function::ArrayConcat, vec![step(id, kind), column(path)])
        }
    }
}

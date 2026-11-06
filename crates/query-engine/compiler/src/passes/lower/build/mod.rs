pub(crate) mod expr;
pub(crate) mod predicates;
pub(crate) mod sources;

use orbit_utils::query_types::SqlType;
use query_data_model::implementations::relational::plan::{
    BinaryOp, Column, Expr, Function, ReadMode, Relation, RelationalBuilder, RelationalError,
};
use query_data_model::implementations::relational::{CurrentRows, RelationalBackend};
use query_data_model::plan::tracking::Declare;
use serde_json::Value;

use crate::passes::plan::predicates::Column as Named;

pub(crate) type Built<T> = std::result::Result<T, RelationalError>;

pub(crate) fn stored<B: RelationalBackend>() -> bool {
    B::CAPABILITIES.current_rows == CurrentRows::Stored
}

pub(crate) fn scan<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    table: &str,
    alias: &str,
    final_: bool,
) -> Built<Relation> {
    let read = match final_ || stored::<B>() {
        true => ReadMode::Current,
        false => ReadMode::Raw,
    };
    let declared = q.catalog().table(table)?;
    q.scan(declared, read, alias)
}

pub(crate) fn lookup(scope: &[&Relation], source: &str, name: &str) -> Built<Column> {
    let mut last = Err(RelationalError::Incomplete("a column lookup"));
    for relation in scope {
        last = relation.qualified(source, name);
        if last.is_ok() {
            return last;
        }
    }
    last
}

pub(crate) fn col(scope: &[&Relation], source: &str, name: &str) -> Built<Expr> {
    lookup(scope, source, name).map(Column::expr)
}

pub(crate) fn column(scope: &[&Relation], column: &Named) -> Built<Expr> {
    col(scope, &column.source, &column.name)
}

pub(crate) fn named(scope: &[&Relation], name: &str) -> Built<Expr> {
    let mut last = Err(RelationalError::Incomplete("a column lookup"));
    for relation in scope {
        last = relation.column(name);
        if last.is_ok() {
            break;
        }
    }
    last.map(Column::expr)
}

pub(crate) fn param<B: RelationalBackend>(data_type: SqlType, value: Value) -> Expr {
    match value {
        Value::Null => Expr::Null,
        Value::Array(items) if !matches!(data_type, SqlType::Array(_)) && !stored::<B>() => {
            Expr::List(
                items
                    .into_iter()
                    .map(|value| Expr::Parameter { data_type, value })
                    .collect(),
            )
        }
        value => Expr::Parameter { data_type, value },
    }
}

pub(crate) fn literal(value: Value) -> Expr {
    match value {
        Value::Null => Expr::Null,
        Value::Array(items) => Expr::List(items.into_iter().map(literal).collect()),
        value => Expr::Parameter {
            data_type: SqlType::from_value(&value),
            value,
        },
    }
}

pub(crate) fn string(value: impl Into<String>) -> Expr {
    Expr::Parameter {
        data_type: SqlType::String,
        value: Value::String(value.into()),
    }
}

pub(crate) fn int(value: i64) -> Expr {
    Expr::Parameter {
        data_type: SqlType::Int64,
        value: value.into(),
    }
}

pub(crate) fn call(function: Function, args: Vec<Expr>) -> Expr {
    Expr::Call { function, args }
}

pub(crate) fn binary(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub(crate) fn eq(left: Expr, right: Expr) -> Expr {
    binary(BinaryOp::Eq, left, right)
}

pub(crate) fn and(left: Expr, right: Expr) -> Expr {
    binary(BinaryOp::And, left, right)
}

pub(crate) fn conjoin(exprs: Vec<Expr>) -> Option<Expr> {
    balanced(BinaryOp::And, exprs)
}

pub(crate) fn disjoin(exprs: Vec<Expr>) -> Option<Expr> {
    balanced(BinaryOp::Or, exprs)
}

fn balanced(op: BinaryOp, mut exprs: Vec<Expr>) -> Option<Expr> {
    match exprs.len() {
        0 => None,
        1 => exprs.pop(),
        length => {
            let right = exprs.split_off(length / 2);
            Some(binary(op, balanced(op, exprs)?, balanced(op, right)?))
        }
    }
}

pub(crate) fn present(mut parts: Vec<Option<Expr>>) -> Option<Expr> {
    match parts.len() {
        0 => None,
        1 => parts.pop().flatten(),
        length => {
            let right = parts.split_off(length / 2);
            match (present(parts), present(right)) {
                (Some(left), Some(right)) => Some(and(left, right)),
                (left, right) => left.or(right),
            }
        }
    }
}

pub(crate) fn versioning(column: &str) -> bool {
    column == ontology::constants::VERSION_COLUMN || column == ontology::constants::DELETED_COLUMN
}

pub(crate) fn filtered<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    rows: Relation,
    predicates: Vec<Option<Expr>>,
) -> Built<Relation> {
    match present(predicates) {
        Some(predicate) => q.filter(rows, predicate),
        None => Ok(rows),
    }
}

pub(crate) fn computed<B: RelationalBackend>(
    q: &mut RelationalBuilder<'_, B>,
    rows: Relation,
    items: Vec<(String, Expr)>,
) -> Built<(Relation, Vec<Column>)> {
    if items.is_empty() {
        return Ok((rows, Vec::new()));
    }
    let items = items
        .into_iter()
        .map(|(name, value)| Declare::named(name, value))
        .collect();
    q.compute(rows, items)
}

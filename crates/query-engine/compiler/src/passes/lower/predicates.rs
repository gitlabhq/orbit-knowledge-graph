use super::sql;
use crate::ast::*;
use crate::passes::plan::predicates::{Column, OutputValue, Predicate, Projection};

pub(super) fn column(value: &Column) -> Expr {
    Expr::col(&value.source, &value.name)
}

fn tuple(values: Vec<Expr>) -> Expr {
    match values.as_slice() {
        [value] => value.clone(),
        _ => Expr::func(Function::Tuple, values),
    }
}

fn step(id: &Column, kind: &Column) -> Expr {
    Expr::func(
        Function::Array,
        vec![Expr::func(Function::Tuple, vec![column(id), column(kind)])],
    )
}

pub(super) fn predicate(value: &Predicate) -> Expr {
    match value {
        Predicate::Boolean(expression) => expression.fold(
            &|leaf| predicate(leaf),
            &|children| Expr::conjoin(children).expect("validated non-empty condition group"),
            &|child| Expr::unary(Op::Not, child),
        ),
        Predicate::Property {
            column: value,
            filter,
            data_type,
            in_sort_key,
            text_indexed,
            ..
        } => sql::filter_expression(
            &value.source,
            &value.name,
            filter,
            data_type.as_ref(),
            *in_sort_key,
            *text_indexed,
        ),
        Predicate::Ids {
            column: value,
            values,
        } => sql::id_list_predicate(&value.source, &value.name, values),
        Predicate::IdRange {
            column: value,
            start,
            end,
        } => Expr::and(
            Expr::binary(Op::Ge, column(value), Expr::int(*start)),
            Expr::binary(Op::Le, column(value), Expr::int(*end)),
        ),
        Predicate::Live { alias } => sql::deleted_false(alias),
        Predicate::OrTombstone { predicates, alias } => Expr::binary(
            Op::Or,
            Expr::conjoin(predicates.iter().map(predicate).collect())
                .expect("tombstone guard wraps at least one predicate"),
            Expr::eq(
                Expr::col(alias, ontology::constants::DELETED_COLUMN),
                Expr::param(SqlType::Bool, true),
            ),
        ),
        Predicate::EntityKind {
            column: value,
            entity,
        } => Expr::eq(column(value), Expr::string(entity)),
        Predicate::ColumnEq { left, right } => Expr::eq(column(left), column(right)),
        Predicate::RelationshipKinds { alias, kinds } => {
            sql::rel_kind_filter(alias, kinds).unwrap_or_else(|| Expr::lit(true))
        }
        Predicate::Tags {
            column: value,
            values,
        } => sql::tag_membership(&value.source, &value.name, values),
        Predicate::Membership {
            columns,
            relation,
            keys,
        } => Expr::InSubquery {
            expr: Box::new(tuple(columns.iter().map(column).collect())),
            cte_name: super::graph::relation_name(*relation),
            columns: keys.clone(),
        },
        Predicate::Scope { alias, proof } => crate::scope::scope_predicate(proof, alias),
    }
}

pub(super) fn projections(values: &[Projection]) -> Vec<SelectExpr> {
    values
        .iter()
        .map(|value| {
            let expression = match &value.value {
                OutputValue::Column(value) => column(value),
                OutputValue::Text(value) => Expr::string(value),
                OutputValue::Depth(value) => Expr::int(i64::from(*value)),
                OutputValue::Path(steps) => Expr::func(
                    Function::Array,
                    steps
                        .iter()
                        .map(|(id, kind)| {
                            Expr::func(Function::Tuple, vec![column(id), column(kind)])
                        })
                        .collect(),
                ),
                OutputValue::PathAppend { path, id, kind } => {
                    Expr::func(Function::ArrayConcat, vec![column(path), step(id, kind)])
                }
                OutputValue::PathPrepend { path, id, kind } => {
                    Expr::func(Function::ArrayConcat, vec![step(id, kind), column(path)])
                }
            };
            SelectExpr::new(expression, &value.name)
        })
        .collect()
}

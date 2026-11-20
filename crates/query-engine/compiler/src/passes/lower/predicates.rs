use super::sql;
use crate::ast::*;
use crate::input::{AggFunction, BooleanExpression, OrderDirection};
use crate::passes::plan::graph::RelationId;
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
    lower_predicate(value, false)
}

fn boolean(expression: &BooleanExpression<Box<Predicate>>, negated: bool) -> Expr {
    match expression {
        BooleanExpression::Leaf(leaf) => lower_predicate(leaf, negated),
        BooleanExpression::And(children) => Expr::conjoin(
            children
                .iter()
                .map(|child| boolean(child, negated))
                .collect(),
        )
        .expect("validated non-empty condition group"),
        BooleanExpression::Not(child) => Expr::unary(Op::Not, boolean(child, true)),
    }
}

fn lower_predicate(value: &Predicate, negated: bool) -> Expr {
    match value {
        Predicate::Boolean(expression) => boolean(expression, negated),
        Predicate::Property {
            column: value,
            filter,
            data_type,
            in_sort_key,
            text_indexed,
            null_as_epoch,
            ..
        } => sql::filter_expression(
            value,
            filter,
            data_type.as_ref(),
            *in_sort_key,
            *text_indexed,
            *null_as_epoch,
            negated,
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
        Predicate::Threshold {
            column: value,
            direction,
            relation,
        } => threshold(value, *direction, *relation),
    }
}

fn threshold(value: &Column, direction: OrderDirection, relation: RelationId) -> Expr {
    let (op, last) = match direction {
        OrderDirection::Desc => (Op::Ge, AggFunction::Min),
        OrderDirection::Asc => (Op::Le, AggFunction::Max),
    };
    let name = super::graph::relation_name(relation);
    let last_value = Query {
        select: vec![SelectExpr::new(
            Expr::aggregate(last, Some(Expr::col(&name, &value.name))),
            &value.name,
        )],
        from: TableRef::cte(&name, &name),
        ..Default::default()
    };
    Expr::binary(op, column(value), Expr::Scalar(Box::new(last_value)))
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

pub(super) fn projections(values: &[Projection]) -> Vec<SelectExpr> {
    values
        .iter()
        .map(|value| SelectExpr::new(output_value(&value.value), &value.name))
        .collect()
}

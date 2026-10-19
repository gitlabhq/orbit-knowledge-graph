use super::sql;
use crate::ast::*;
use crate::input::OrderDirection;
use crate::passes::plan::aggregation::{AggregationPlan, Group};
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

fn group(value: &Group) -> Expr {
    let column = column(&value.column);
    match value.truncate {
        Some(unit) => Expr::TimeBucket {
            unit,
            value: Box::new(column),
        },
        None => column,
    }
}

pub(super) fn aggregation(plan: &AggregationPlan, output: super::EmitOutput, limit: u32) -> Node {
    let condition = Expr::conjoin(plan.condition.iter().map(predicate).collect());
    let select = plan
        .group_outputs
        .iter()
        .map(|(value, name)| SelectExpr::new(group(value), name))
        .chain(plan.measures.iter().map(|measure| {
            SelectExpr::new(
                Expr::Aggregate {
                    function: measure.function,
                    argument: measure
                        .argument
                        .as_ref()
                        .map(|value| Box::new(column(value))),
                    distinct: false,
                    condition: condition.clone().map(Box::new),
                },
                &measure.name,
            )
        }))
        .collect();
    let order = plan
        .order
        .iter()
        .map(|order| {
            let value = Expr::ident(&order.column);
            if order.direction == OrderDirection::Desc {
                OrderExpr::desc(value)
            } else {
                OrderExpr::asc(value)
            }
        })
        .collect();
    Node::Query(Box::new(output.into_query(
        select,
        plan.groups.iter().map(group).collect(),
        order,
        limit,
    )))
}

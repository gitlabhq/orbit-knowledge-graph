use super::sql;
use crate::ast::*;
use crate::input::{OrderDirection, TruncateUnit};
use crate::passes::plan::aggregation::{AggregationPlan, Group};
use crate::passes::plan::requirements::{Column, OutputValue, Predicate, Projection};

pub(super) fn column(value: &Column) -> Expr {
    Expr::col(&value.source, &value.name)
}

pub(super) fn predicate(value: &Predicate) -> Expr {
    match value {
        Predicate::Property {
            column: value,
            filter,
            data_type,
        } => sql::filter_expression(&value.source, &value.name, filter, data_type.as_ref()),
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
        Predicate::RelationshipKinds { alias, kinds } => {
            sql::rel_kind_filter(alias, kinds).expect("planned relationship kinds")
        }
        Predicate::Tags {
            column: value,
            values,
        } => sql::tag_membership(&value.source, &value.name, values),
        Predicate::Membership {
            column: value,
            definition,
            key,
        } => Expr::InSubquery {
            expr: Box::new(column(value)),
            cte_name: definition.clone(),
            column: key.clone(),
        },
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
                    "array",
                    steps
                        .iter()
                        .map(|(id, kind)| Expr::func("tuple", vec![column(id), column(kind)]))
                        .collect(),
                ),
            };
            SelectExpr::new(expression, &value.name)
        })
        .collect()
}

fn group(value: &Group) -> Expr {
    let column = column(&value.column);
    match value.truncate {
        Some(unit) => {
            let truncated = Expr::func(unit.ch_function(), vec![column]);
            match unit {
                TruncateUnit::Minute | TruncateUnit::Hour => {
                    Expr::func("toDateTime64", vec![truncated, Expr::ident("0")])
                }
                _ => Expr::func("toDate32", vec![truncated]),
            }
        }
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
            let mut arguments: Vec<_> = measure.argument.iter().map(column).collect();
            let name = if let Some(condition) = &condition {
                arguments.push(condition.clone());
                measure.function.as_sql_if()
            } else {
                measure.function.as_sql()
            };
            SelectExpr::new(Expr::func(name, arguments), &measure.name)
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

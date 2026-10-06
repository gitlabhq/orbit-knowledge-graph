use super::sql;
use crate::ast::*;
use crate::input::OrderDirection;
use crate::passes::plan::aggregation::{AggregationPlan, Group};
use crate::passes::plan::requirements::{Column, OutputValue, Predicate, PrefixPaths, Projection};

pub(super) fn column(value: &Column) -> Expr {
    Expr::col(&value.source, &value.name)
}

pub(super) fn predicate(value: &Predicate) -> Expr {
    match value {
        Predicate::PathPrefixes {
            column: value,
            paths,
        } => match paths {
            PrefixPaths::Union(paths) => prefix_union(&column(value), paths),
            PrefixPaths::Set(paths) => Expr::func(
                Function::ArrayExists,
                vec![
                    Expr::lambda(
                        "_gkg_path",
                        Expr::func(
                            Function::StartsWith,
                            vec![column(value), Expr::ident("_gkg_path")],
                        ),
                    ),
                    Expr::param(
                        SqlType::String.to_array(),
                        serde_json::Value::Array(
                            paths
                                .iter()
                                .map(|path| serde_json::Value::String(path.as_str().into()))
                                .collect(),
                        ),
                    ),
                ],
            ),
        },
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
            sql::rel_kind_filter(alias, kinds).unwrap_or_else(|| Expr::lit(true))
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
                OutputValue::Properties(columns) if columns.is_empty() => Expr::string("{}"),
                OutputValue::Properties(columns) => Expr::func(
                    Function::ToJson,
                    vec![Expr::func(
                        Function::Object,
                        columns
                            .iter()
                            .flat_map(|value| {
                                [
                                    Expr::string(&value.name),
                                    Expr::func(Function::ToString, vec![column(value)]),
                                ]
                            })
                            .collect(),
                    )],
                ),
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
            };
            SelectExpr {
                expr: expression,
                alias: Some(value.name.clone()),
            }
        })
        .collect()
}

fn prefix_union(column: &Expr, paths: &[orbit_utils::traversal_path::TraversalPath]) -> Expr {
    match paths {
        [] => Expr::lit(false),
        [path] => Expr::func(
            Function::StartsWith,
            vec![column.clone(), Expr::string(path.as_str())],
        ),
        paths => {
            let (left, right) = paths.split_at(paths.len() / 2);
            Expr::binary(
                Op::Or,
                prefix_union(column, left),
                prefix_union(column, right),
            )
        }
    }
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
        .map(|(value, name)| SelectExpr::exporting(group(value), name))
        .chain(plan.measures.iter().map(|measure| {
            SelectExpr::exporting(
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
        .map(|(export, direction)| {
            let value = Expr::Output(export.clone());
            if *direction == OrderDirection::Desc {
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

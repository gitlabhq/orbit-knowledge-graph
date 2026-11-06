use super::super::ast::{AggregateFunction, Expression, ProjectionItem, Sort, SortKey, Target};
use super::super::errors::invalid;
use super::Built;
use super::returns::{order, property_ids};
use super::scope::{Scope, Shape};
use crate::plan::logical::{Columns, Expr, LogicalBuilder, Measure, PlanError, Projection, Rows};
use crate::plan::tracking::Named;
use crate::surface::SurfaceError;

struct Group {
    named: Named<Expr>,
    key: Option<(String, String)>,
    node: Option<(String, Columns)>,
}

pub(super) fn project<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    scope: &Scope<'_>,
    items: Vec<ProjectionItem<'_>>,
    sort: Option<Sort<'_>>,
) -> Built<'m> {
    if matches!(scope.shape, Shape::Path(_)) {
        return Err(PlanError::shape("aggregation over shortest paths is not supported").into());
    }
    let mut groups: Vec<Group> = Vec::new();
    let mut measures = Vec::new();
    for item in items {
        let alias = item.alias.map(|alias| alias.value);
        match item.expression {
            Expression::Aggregate {
                function,
                distinct,
                target,
            } => {
                measures.push(measure(&rows, function, distinct, target, alias)?);
            }
            Expression::DateTrunc { unit, property, .. } => {
                let (node, name) = (property.node.value, property.property.value);
                let expr = rows.binding(&node)?.truncate(&name, unit)?;
                let unit: &str = unit.into();
                let derived = format!("{node}_{name}_{unit}");
                groups.push(Group {
                    named: expr.named(alias.unwrap_or(derived)),
                    key: None,
                    node: None,
                });
            }
            Expression::Property(property) => {
                let (node, name) = (property.node.value, property.property.value);
                let expr = rows.binding(&node)?.property(&name)?;
                let derived = format!("{node}_{name}");
                groups.push(Group {
                    named: expr.named(alias.unwrap_or(derived)),
                    key: Some((node, name)),
                    node: None,
                });
            }
            Expression::Node {
                span,
                variable,
                properties,
            } => {
                let columns: Vec<String> = properties.into_iter().map(|name| name.value).collect();
                node(
                    &mut groups,
                    &rows,
                    span,
                    variable.value,
                    Some(columns),
                    false,
                    alias,
                )?;
            }
            Expression::Variable(variable) => {
                graph(scope, variable.span, &variable.value)?;
                node(
                    &mut groups,
                    &rows,
                    variable.span,
                    variable.value,
                    None,
                    false,
                    alias,
                )?;
            }
            Expression::AllProperties { span, variable } => {
                graph(scope, span, &variable.value)?;
                node(&mut groups, &rows, span, variable.value, None, true, alias)?;
            }
            Expression::RelationshipType { span, .. } => {
                return Err(invalid(
                    span,
                    "grouping by relationship type is not supported yet",
                ));
            }
        }
    }
    let names: Vec<_> = groups
        .iter()
        .map(|group| (group.named.name.clone(), group.node.clone()))
        .chain(
            measures
                .iter()
                .map(|measure: &Named<Measure>| (measure.name.clone(), None)),
        )
        .collect();
    let keys: Vec<_> = groups
        .iter()
        .map(|group| (group.key.clone(), group.named.name.clone()))
        .collect();
    let mut rows = q.aggregate(rows, groups.into_iter().map(|group| group.named), measures)?;
    if let Some(sort) = sort {
        let column = match sort.key {
            SortKey::Property(key) => {
                let wanted = Some((key.node.value, key.property.value));
                keys.into_iter()
                    .find(|(key, _)| *key == wanted)
                    .map(|(_, name)| name)
                    .ok_or_else(|| {
                        invalid(
                            key.span,
                            "ORDER BY must name an aggregate alias or returned group key",
                        )
                    })?
            }
            SortKey::Variable(name) => name.value,
            SortKey::RelationshipType(name) => {
                return Err(invalid(
                    name.span,
                    &format!(
                        "ORDER BY type({}) is not supported in aggregations yet",
                        name.value
                    ),
                ));
            }
        };
        let key = rows.column(&column)?;
        rows = q.order(rows, [(key, order(sort.direction))])?;
    }
    let items = names
        .into_iter()
        .map(|(name, node)| {
            Ok(match node {
                Some((_, columns)) => {
                    let binding = rows.binding(&name)?.output();
                    Projection::Graph { binding, columns }.named(name)
                }
                None => Projection::Value(rows.column(&name)?).named(name),
            })
        })
        .collect::<Result<Vec<_>, SurfaceError>>()?;
    Ok(q.select(rows, items)?)
}

fn graph(scope: &Scope<'_>, span: pest::Span<'_>, variable: &str) -> Result<(), SurfaceError> {
    if scope.edge(variable).is_some() {
        return Err(invalid(
            span,
            "grouping by a relationship variable is not supported yet",
        ));
    }
    if scope.dynamic(variable) {
        return Err(invalid(span, "aggregation cannot return the path variable"));
    }
    Ok(())
}

fn node(
    groups: &mut Vec<Group>,
    rows: &Rows<'_>,
    span: pest::Span<'_>,
    alias: String,
    columns: Option<Vec<String>>,
    all: bool,
    name: Option<String>,
) -> Result<(), SurfaceError> {
    let columns = match (columns, all) {
        (Some(listed), _) => Columns::Listed(property_ids(rows, &alias, &listed)?),
        (None, true) => Columns::All,
        (None, false) => Columns::Default,
    };
    if groups.iter().any(|group| {
        matches!(&group.node, Some((other, previous)) if *other == alias && !same(previous, &columns))
    }) {
        return Err(invalid(span, "duplicate or overlapping node projection"));
    }
    let expr = rows.binding(&alias)?.node();
    let named = expr.named(name.unwrap_or_else(|| alias.clone()));
    groups.push(Group {
        named,
        key: None,
        node: Some((alias, columns)),
    });
    Ok(())
}

fn same(left: &Columns, right: &Columns) -> bool {
    match (left, right) {
        (Columns::Listed(left), Columns::Listed(right)) => {
            let set = |ids: &[crate::PropertyId]| {
                ids.iter()
                    .copied()
                    .collect::<std::collections::BTreeSet<_>>()
            };
            set(left) == set(right)
        }
        _ => left == right,
    }
}

fn measure(
    rows: &Rows<'_>,
    function: AggregateFunction,
    distinct: bool,
    target: Target<'_>,
    alias: Option<String>,
) -> Result<Named<Measure>, SurfaceError> {
    let (node, property, input) = match target {
        Target::Property(property) => {
            let (node, name) = (property.node.value, property.property.value);
            let input = rows.binding(&node)?.property(&name)?;
            (node, Some(name), input)
        }
        Target::Variable(variable) if function == AggregateFunction::Count => {
            let input = rows.binding(&variable.value)?.node();
            (variable.value, None, input)
        }
        Target::Variable(variable) => {
            return Err(invalid(
                variable.span,
                "this aggregate requires a node.property target",
            ));
        }
    };
    let (measure, function) = match (function, distinct) {
        (AggregateFunction::Count, false) => (Measure::Count(Some(input)), "count"),
        (AggregateFunction::Count, true) => (Measure::CountDistinct(input), "count_distinct"),
        (_, true) => {
            return Err(SurfaceError::schema(
                "\"distinct\" works only with \"count\"",
            ));
        }
        (AggregateFunction::Sum, _) => (Measure::Sum(input), "sum"),
        (AggregateFunction::Avg, _) => (Measure::Avg(input), "avg"),
        (AggregateFunction::Min, _) => (Measure::Min(input), "min"),
        (AggregateFunction::Max, _) => (Measure::Max(input), "max"),
    };
    let derived = match property {
        Some(property) => format!("{function}_{node}_{property}"),
        None => format!("{function}_{node}"),
    };
    Ok(measure.named(alias.unwrap_or(derived)))
}

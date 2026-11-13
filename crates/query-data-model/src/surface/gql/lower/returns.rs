use std::collections::HashMap;

use super::super::ast::{
    AggregateFunction, Expression, Name, OrderDirection, ProjectionItem, Projections, Sort,
    SortKey, Target,
};
use super::super::errors::invalid;
use super::Built;
use super::conditions::column;
use crate::plan::logical::{
    Columns, EdgeField, Expr, LogicalBuilder, LogicalType, Measure, Order, Projection, Rows,
};
use crate::plan::tracking::Named;
use crate::surface::SurfaceError;

/// What RETURN names: graph items in written order, with `a.x, a.y` folded into one node item.
#[derive(Default)]
struct Returned {
    /// Neighbors and path results carry every relationship in their path column, so a plain
    /// `RETURN r` or `RETURN type(r)` adds nothing and lowers to no item.
    paths: bool,
    items: Vec<(String, Item)>,
    aliases: HashMap<String, Option<(String, String)>>,
    dynamic: Option<(String, Columns)>,
}

enum Item {
    Node(Option<Vec<String>>, bool),
    Graph(String, Columns),
    Value(Expr),
}

pub(super) fn project<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    nodes: &[&str],
    dynamic: Option<&str>,
    projections: Projections<'_>,
    sort: Option<Sort<'_>>,
) -> Built<'m> {
    let items = match projections {
        Projections::Star(span) if dynamic.is_some() => {
            return Err(invalid(span, "RETURN * is only supported for traversal"));
        }
        Projections::Star(_) => Vec::new(),
        Projections::Items(items)
            if items
                .iter()
                .any(|item| matches!(item.expression, Expression::Aggregate { .. })) =>
        {
            return aggregate(q, rows, items, sort);
        }
        Projections::Items(items) => items,
    };
    let mut returned = Returned {
        paths: dynamic.is_some(),
        ..Returned::default()
    };
    if items.is_empty() {
        returned.items = nodes
            .iter()
            .map(|alias| (alias.to_string(), Item::Node(None, false)))
            .collect();
    }
    for item in items {
        returned.item(&rows, item)?;
    }
    let rows = match sort {
        Some(sort) => {
            let key = returned.sort_key(&rows, sort.key)?;
            q.order(rows, [(key, order(sort.direction))])?
        }
        None => rows,
    };
    let projections = returned.select(&rows, dynamic)?;
    Ok(q.select(rows, projections)?)
}

impl Returned {
    fn alias(
        &mut self,
        span: pest::Span<'_>,
        alias: Option<String>,
        target: Option<(String, String)>,
    ) -> Result<(), SurfaceError> {
        match alias.map(|alias| self.aliases.insert(alias, target)) {
            Some(Some(_)) => Err(invalid(span, "duplicate alias")),
            _ => Ok(()),
        }
    }

    fn item(&mut self, rows: &Rows<'_>, item: ProjectionItem<'_>) -> Result<(), SurfaceError> {
        let alias = item.alias.map(|alias| alias.value);
        match item.expression {
            Expression::Aggregate { .. } => unreachable!("aggregates lower through aggregate"),
            Expression::DateTrunc {
                span,
                unit,
                property,
            } => {
                let expr = rows
                    .binding(&property.node.value)?
                    .truncate_named(&property.property.value, unit)?;
                let name = alias.unwrap_or_else(|| span.as_str().to_string());
                self.items.push((name, Item::Value(expr)));
            }
            Expression::Node {
                span,
                variable,
                properties,
            } => {
                self.alias(span, alias, None)?;
                let columns = properties.into_iter().map(|name| name.value).collect();
                self.items
                    .push((variable.value, Item::Node(Some(columns), false)));
            }
            Expression::Property(property) => {
                let (node, name) = (property.node.value, property.property.value);
                self.alias(property.span, alias, Some((node.clone(), name.clone())))?;
                match self.items.iter_mut().find(|(other, item)| {
                    *other == node && matches!(item, Item::Node(Some(_), true))
                }) {
                    Some((_, Item::Node(Some(columns), _))) => columns.push(name),
                    _ => self.items.push((node, Item::Node(Some(vec![name]), true))),
                }
            }
            Expression::Variable(variable) => {
                self.graph(rows, variable, Columns::Default, alias)?
            }
            Expression::AllProperties(variable) => {
                self.graph(rows, variable, Columns::All, alias)?
            }
            Expression::RelationshipType { span, variable } => {
                let binding = rows.binding(&variable.value)?;
                let kind = binding.field(EdgeField::Kind)?;
                if self.paths && alias.is_none() && binding.hops().max == 1 {
                    return Ok(());
                }
                let name = alias.clone().unwrap_or_else(|| span.as_str().to_owned());
                self.alias(span, alias, None)?;
                self.items.push((name, Item::Value(kind)));
            }
            Expression::Match(field) => {
                let column = format!("match.{}", field.value);
                let name = alias.clone().unwrap_or_else(|| column.clone());
                self.alias(field.span, alias, None)?;
                self.items.push((name, Item::Value(rows.column(&column)?)));
            }
        }
        Ok(())
    }

    fn graph(
        &mut self,
        rows: &Rows<'_>,
        variable: Name<'_>,
        columns: Columns,
        alias: Option<String>,
    ) -> Result<(), SurfaceError> {
        let binding = rows.binding(&variable.value)?;
        let name = alias.clone().unwrap_or_else(|| variable.value.clone());
        match binding.kind() {
            LogicalType::Node | LogicalType::Path if self.dynamic.is_none() => {
                self.dynamic = Some((name, columns));
            }
            LogicalType::Edge if self.paths && alias.is_none() && binding.hops().max == 1 => {}
            LogicalType::Node | LogicalType::Path | LogicalType::Edge => {
                self.items
                    .push((name, Item::Graph(variable.value, columns)));
            }
            _ if self.paths && columns == Columns::All => {
                return Err(invalid(
                    variable.span,
                    "dynamic graph results cannot be renamed or projected as properties",
                ));
            }
            _ => {
                self.alias(variable.span, alias, None)?;
                let all = columns == Columns::All;
                self.items.push((variable.value, Item::Node(None, all)));
            }
        }
        Ok(())
    }

    fn sort_key(&self, rows: &Rows<'_>, key: SortKey<'_>) -> Result<Expr, SurfaceError> {
        let kind = |name: &str| Ok(rows.binding(name)?.field(EdgeField::Kind)?);
        match key {
            SortKey::Property(key) => column(rows, &key),
            SortKey::RelationshipType(name) => kind(&name.value),
            SortKey::Variable(name) => {
                if let Some((_, Item::Value(expr @ Expr::Edge { .. }))) =
                    self.items.iter().find(|(column, _)| *column == name.value)
                {
                    return Ok(*expr);
                }
                match self.aliases.get(&name.value) {
                    Some(Some((node, property))) => {
                        Ok(rows.binding(node)?.property_named(property)?)
                    }
                    Some(None) => Err(invalid(
                        name.span,
                        "ORDER BY a property alias, not a node alias",
                    )),
                    None => Err(invalid(
                        name.span,
                        "traversal ORDER BY requires node.property or a property alias",
                    )),
                }
            }
        }
    }

    fn select(
        self,
        rows: &Rows<'_>,
        dynamic: Option<&str>,
    ) -> Result<Vec<Named<Projection>>, SurfaceError> {
        let mut items = Vec::new();
        for (name, item) in self.items {
            let projection = match item {
                Item::Value(expr) => Projection::Value(expr),
                Item::Graph(variable, columns) => Projection::Graph {
                    binding: rows.binding(&variable)?.output(),
                    columns,
                },
                Item::Node(listed, all) => {
                    let binding = rows.binding(&name)?;
                    let columns = match (listed, all) {
                        (Some(names), _) => Columns::Listed(property_ids(rows, &name, &names)?),
                        (None, true) => Columns::All,
                        (None, false) => binding.catalog().default_columns(),
                    };
                    Projection::Graph {
                        binding: binding.output(),
                        columns,
                    }
                }
            };
            items.push(projection.named(name));
        }
        if let Some(variable) = dynamic {
            let (name, columns) = self
                .dynamic
                .unwrap_or_else(|| (variable.to_string(), Columns::Default));
            let binding = rows.binding(variable)?;
            let columns = match columns {
                Columns::Default => binding.catalog().default_columns(),
                columns => columns,
            };
            items.push(
                Projection::Graph {
                    binding: binding.output(),
                    columns,
                }
                .named(name),
            );
        }
        Ok(items)
    }
}

/// A group key: its expression, the node property it reads, and the columns of a node key.
type Group = (Named<Expr>, Option<(String, String)>, Option<Columns>);

fn aggregate<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    items: Vec<ProjectionItem<'_>>,
    sort: Option<Sort<'_>>,
) -> Built<'m> {
    let mut groups: Vec<Group> = Vec::new();
    let mut measures = Vec::new();
    for item in items {
        let alias = item.alias.map(|alias| alias.value);
        let node = |variable: Name<'_>, columns: Columns| -> Result<_, SurfaceError> {
            let expr = rows.binding(&variable.value)?.node();
            let name = alias.clone().unwrap_or(variable.value);
            Ok((expr.named(name), None, Some(columns)))
        };
        let group = match item.expression {
            Expression::Aggregate {
                function,
                distinct,
                target,
            } => {
                measures.push(measure(&rows, function, distinct, target, alias)?);
                continue;
            }
            Expression::DateTrunc { unit, property, .. } => {
                let (node, name) = (property.node.value, property.property.value);
                let expr = rows.binding(&node)?.truncate_named(&name, unit)?;
                let unit: &str = unit.into();
                (
                    expr.named(alias.unwrap_or(format!("{node}_{name}_{unit}"))),
                    None,
                    None,
                )
            }
            Expression::Property(property) => {
                let (node, name) = (property.node.value, property.property.value);
                let expr = rows.binding(&node)?.property_named(&name)?;
                let derived = format!("{node}_{name}");
                (
                    expr.named(alias.unwrap_or(derived)),
                    Some((node, name)),
                    None,
                )
            }
            Expression::Node {
                variable,
                properties,
                ..
            } => {
                let names: Vec<String> = properties.into_iter().map(|name| name.value).collect();
                let listed = Columns::Listed(property_ids(&rows, &variable.value, &names)?);
                node(variable, listed)?
            }
            Expression::Variable(variable) => {
                let columns = rows.binding(&variable.value)?.catalog().default_columns();
                node(variable, columns)?
            }
            Expression::AllProperties(variable) => node(variable, Columns::All)?,
            Expression::RelationshipType { span, variable } => {
                let expr = rows.binding(&variable.value)?.field(EdgeField::Kind)?;
                (
                    expr.named(alias.unwrap_or_else(|| span.as_str().to_owned())),
                    None,
                    None,
                )
            }
            Expression::Match(field) => {
                let column = format!("match.{}", field.value);
                let expr = rows.column(&column)?;
                (expr.named(alias.unwrap_or(column)), None, None)
            }
        };
        let overlaps = |(other, _, columns): &Group| {
            other.item == group.0.item
                && columns
                    .as_ref()
                    .zip(group.2.as_ref())
                    .is_some_and(|(left, right)| !same(left, right))
        };
        if groups.iter().any(overlaps) {
            return Err(SurfaceError::schema(
                "a node grouped twice must return the same properties both times",
            ));
        }
        groups.push(group);
    }
    let names: Vec<_> = groups
        .iter()
        .map(|(named, _, columns)| (named.name.clone(), columns.clone()))
        .chain(
            measures
                .iter()
                .map(|measure: &Named<Measure>| (measure.name.clone(), None)),
        )
        .collect();
    let keys: Vec<_> = groups
        .iter()
        .map(|(named, key, _)| (key.clone(), named.name.clone()))
        .collect();
    let mut rows = q.aggregate(rows, groups.into_iter().map(|(named, ..)| named), measures)?;
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
            SortKey::Variable(name) | SortKey::RelationshipType(name) => name.value,
        };
        let key = rows.column(&column)?;
        rows = q.order(rows, [(key, order(sort.direction))])?;
    }
    let items = names
        .into_iter()
        .map(|(name, columns)| {
            Ok(match columns {
                Some(columns) => {
                    let binding = rows.binding(&name)?.output();
                    Projection::Graph { binding, columns }.named(name)
                }
                None => Projection::Value(rows.column(&name)?).named(name),
            })
        })
        .collect::<Result<Vec<_>, SurfaceError>>()?;
    Ok(q.select(rows, items)?)
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
            let input = rows.binding(&node)?.property_named(&name)?;
            (node, Some(name), input)
        }
        Target::Variable(variable) => {
            let input = rows.binding(&variable.value)?.node();
            (variable.value, None, input)
        }
    };
    let (measure, function) = match (function, distinct) {
        (AggregateFunction::Count, false) => (Measure::Count(Some(input)), "count"),
        (AggregateFunction::Count, true) => (Measure::CountDistinct(input), "count_distinct"),
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

/// The Aggregate groups by the node alone, so one node cannot return two property sets.
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

fn order(direction: OrderDirection) -> Order {
    match direction {
        OrderDirection::Asc => Order::Ascending,
        OrderDirection::Desc => Order::Descending,
    }
}

fn property_ids(
    rows: &Rows<'_>,
    alias: &str,
    names: &[String],
) -> Result<Vec<crate::PropertyId>, SurfaceError> {
    let binding = rows.binding(alias)?;
    names
        .iter()
        .map(|name| match binding.property_named(name)? {
            Expr::Property { property, .. } => Ok(property),
            _ => Err(crate::plan::logical::PlanError::NoEntity.into()),
        })
        .collect()
}

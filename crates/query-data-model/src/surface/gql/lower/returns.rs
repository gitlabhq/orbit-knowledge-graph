use std::collections::HashMap;

use super::super::ast::{
    Expression, Name, OrderDirection, ProjectionItem, Projections, Sort, SortKey,
};
use super::super::errors::invalid;
use super::scope::{Scope, Shape};
use super::{Built, aggregate};
use crate::plan::logical::limits::MAX_COLUMNS;
use crate::plan::logical::{Columns, EdgeField, Expr, LogicalBuilder, Order, Projection, Rows};
use crate::surface::SurfaceError;

pub(super) enum Item {
    Node(String, Option<Vec<String>>, bool),
    Edge(String, String),
    Type(String, String),
}

#[derive(Default)]
struct Returned {
    items: Vec<Item>,
    aliases: HashMap<String, Option<(String, String)>>,
    dynamic: Option<Columns>,
}

pub(super) fn order(direction: OrderDirection) -> Order {
    match direction {
        OrderDirection::Asc => Order::Ascending,
        OrderDirection::Desc => Order::Descending,
    }
}

pub(super) fn project<'m>(
    q: &mut LogicalBuilder<'m>,
    rows: Rows<'m>,
    scope: &Scope<'_>,
    projections: Projections<'_>,
    sort: Option<Sort<'_>>,
) -> Built<'m> {
    let items = match projections {
        Projections::Star(span) if scope.shape != Shape::Match => {
            return Err(invalid(span, "RETURN * is only supported for traversal"));
        }
        Projections::Star(_) => Vec::new(),
        Projections::Items { span, items } => {
            if items
                .iter()
                .any(|item| matches!(item.expression, Expression::Aggregate { .. }))
            {
                if matches!(scope.shape, Shape::Neighbors(_)) {
                    return Err(invalid(
                        span,
                        "neighbors cannot be aggregated; use labeled node patterns",
                    ));
                }
                return aggregate::project(q, rows, scope, items, sort);
            }
            items
        }
    };
    let mut returned = Returned::default();
    if items.is_empty() {
        returned.items = scope
            .nodes
            .iter()
            .map(|node| Item::Node(node.alias.clone(), None, false))
            .collect();
    }
    for item in items {
        returned.item(scope, item)?;
    }
    let rows = match sort {
        Some(sort) if scope.shape != Shape::Match => {
            return Err(invalid(
                sort.span,
                "path finding and neighbors have fixed result ordering",
            ));
        }
        Some(sort) => {
            let key = returned.sort_key(&rows, scope, sort.key)?;
            q.order(rows, [(key, order(sort.direction))])?
        }
        None => rows,
    };
    let projections = returned.select(&rows, scope)?;
    Ok(q.select(rows, projections)?)
}

impl Returned {
    fn node(&mut self, alias: &str) -> Option<&mut Item> {
        self.items
            .iter_mut()
            .find(|item| matches!(item, Item::Node(name, ..) if name == alias))
    }

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

    fn item(&mut self, scope: &Scope<'_>, item: ProjectionItem<'_>) -> Result<(), SurfaceError> {
        let alias = item.alias.map(|alias| alias.value);
        let path = matches!(scope.shape, Shape::Path(_));
        match item.expression {
            Expression::Aggregate { .. } => {
                unreachable!("aggregates lower through aggregate::project")
            }
            Expression::DateTrunc { span, .. } => Err(invalid(
                span,
                "date_trunc is only supported as an aggregation group key",
            )),
            Expression::Node {
                span,
                variable,
                properties,
            } => {
                if path {
                    return Err(invalid(
                        span,
                        "node projections require traversal, neighbors, or aggregation",
                    ));
                }
                let columns: Vec<String> = properties.into_iter().map(|name| name.value).collect();
                let distinct: std::collections::HashSet<_> = columns.iter().collect();
                if distinct.len() != columns.len() || self.node(&variable.value).is_some() {
                    return Err(invalid(span, "duplicate or overlapping node projection"));
                }
                self.alias(span, alias, None)?;
                if scope.node(&variable.value).is_none() {
                    return Err(invalid(
                        span,
                        "node projection references an undefined variable",
                    ));
                }
                self.items
                    .push(Item::Node(variable.value, Some(columns), false));
                Ok(())
            }
            Expression::Property(property) => {
                let span = property.span;
                if path {
                    return Err(invalid(
                        span,
                        "property projections require traversal or neighbors",
                    ));
                }
                let (node, name) = (property.node.value, property.property.value);
                self.alias(span, alias, Some((node.clone(), name.clone())))?;
                if scope.node(&node).is_none() {
                    return Err(invalid(
                        span,
                        "property projection references an undefined node",
                    ));
                }
                match self.node(&node) {
                    None => self.items.push(Item::Node(node, Some(vec![name]), true)),
                    Some(Item::Node(_, Some(columns), true)) if columns.len() == MAX_COLUMNS => {
                        return Err(invalid(
                            span,
                            &format!("a node projects at most {MAX_COLUMNS} properties"),
                        ));
                    }
                    Some(Item::Node(_, Some(columns), true)) if !columns.contains(&name) => {
                        columns.push(name)
                    }
                    Some(_) => {
                        return Err(invalid(span, "duplicate or overlapping node projection"));
                    }
                }
                Ok(())
            }
            Expression::Variable(variable) => {
                self.graph(scope, variable.span, variable, false, alias)
            }
            Expression::AllProperties { span, variable } => {
                self.graph(scope, span, variable, true, alias)
            }
            Expression::RelationshipType { span, variable } => {
                let name = alias.clone().unwrap_or_else(|| span.as_str().to_owned());
                if self.relationship(scope, span, &variable, alias)? {
                    self.items.push(Item::Type(name, variable.value));
                }
                Ok(())
            }
        }
    }

    fn relationship(
        &mut self,
        scope: &Scope<'_>,
        span: pest::Span<'_>,
        variable: &Name<'_>,
        alias: Option<String>,
    ) -> Result<bool, SurfaceError> {
        let name = &variable.value;
        match scope.edge(name) {
            Some((_, edge)) if edge.hops.max > 1 => {
                return Err(invalid(
                    span,
                    &format!(
                        "{name} has a variable length and binds a list; type({name}) and RETURN {name} need a one-hop relationship, so list the types in the pattern instead, such as -[{name}:CLOSES|MENTIONS*1..3]->"
                    ),
                ));
            }
            Some(_) => {}
            None => return Err(invalid(span, &format!("undefined variable {name}"))),
        }
        if scope.shape != Shape::Match {
            return match alias {
                Some(alias) => Err(invalid(
                    span,
                    &format!(
                        "{alias}: neighbors and path finding return one path column that shows each edge type, so a relationship item cannot be renamed"
                    ),
                )),
                None => Ok(false),
            };
        }
        let written = alias.clone().unwrap_or_else(|| name.clone());
        if self.items.iter().any(|item| match item {
            Item::Node(other, ..) | Item::Edge(other, _) | Item::Type(other, _) => {
                *other == written
            }
        }) || scope.node(&written).is_some()
        {
            return Err(invalid(span, "duplicate RETURN column name"));
        }
        self.alias(span, alias, None)?;
        Ok(true)
    }

    fn graph(
        &mut self,
        scope: &Scope<'_>,
        span: pest::Span<'_>,
        variable: Name<'_>,
        all: bool,
        alias: Option<String>,
    ) -> Result<(), SurfaceError> {
        if scope.edge(&variable.value).is_some() {
            if all {
                return Err(invalid(
                    span,
                    &format!(
                        "properties({}) is not supported; relationships have no properties",
                        variable.value
                    ),
                ));
            }
            let name = alias.clone().unwrap_or_else(|| variable.value.clone());
            if self.relationship(scope, span, &variable, alias)? {
                self.items.push(Item::Edge(name, variable.value));
            }
            return Ok(());
        }
        let dynamic = scope.dynamic(&variable.value);
        if dynamic || (matches!(scope.shape, Shape::Neighbors(_)) && all) {
            if alias.is_some() || (all && !dynamic) {
                return Err(invalid(
                    span,
                    "dynamic graph results cannot be renamed or projected as properties",
                ));
            }
            if self.dynamic.is_some() {
                return Err(invalid(span, "duplicate graph projection"));
            }
            self.dynamic = Some(if all { Columns::All } else { Columns::Default });
            return Ok(());
        }
        if matches!(scope.shape, Shape::Path(_)) {
            return Err(invalid(
                span,
                "path finding requires RETURN of the path variable",
            ));
        }
        if scope.node(&variable.value).is_none() {
            let (line, column) = span.start_pos().line_col();
            return Err(crate::plan::logical::PlanError::UnknownBinding {
                name: format!("{} at line {line}, column {column}", variable.value),
                available: scope.nodes.iter().map(|node| node.alias.clone()).collect(),
            }
            .into());
        }
        if self.node(&variable.value).is_some() {
            return Err(invalid(span, "duplicate or overlapping node projection"));
        }
        self.alias(span, alias, None)?;
        self.items.push(Item::Node(variable.value, None, all));
        Ok(())
    }

    fn sort_key(
        &self,
        rows: &Rows<'_>,
        scope: &Scope<'_>,
        key: SortKey<'_>,
    ) -> Result<Expr, SurfaceError> {
        let kind = |name: &Name<'_>| -> Result<Expr, SurfaceError> {
            Ok(rows.binding(&name.value)?.field(EdgeField::Kind)?)
        };
        let (node, property) = match key {
            SortKey::Property(key) if scope.edge(&key.node.value).is_some() => {
                return Err(invalid(
                    key.span,
                    &format!(
                        "ORDER BY cannot sort by a relationship property; sort by a node property or by type({})",
                        key.node.value
                    ),
                ));
            }
            SortKey::Property(key) => (key.node.value, key.property.value),
            SortKey::RelationshipType(name) => match scope.edge(&name.value) {
                Some((_, edge)) if edge.hops.max == 1 => return kind(&name),
                _ => {
                    return Err(invalid(
                        name.span,
                        &format!("undefined variable {}", name.value),
                    ));
                }
            },
            SortKey::Variable(name) => {
                if let Some(Item::Type(_, edge)) = self
                    .items
                    .iter()
                    .find(|item| matches!(item, Item::Type(column, _) if *column == name.value))
                {
                    return kind(&Name {
                        span: name.span,
                        value: edge.clone(),
                    });
                }
                match self.aliases.get(&name.value) {
                    Some(Some(target)) => target.clone(),
                    Some(None) => {
                        return Err(invalid(
                            name.span,
                            "ORDER BY a property alias, not a node alias",
                        ));
                    }
                    None => {
                        return Err(invalid(
                            name.span,
                            "traversal ORDER BY requires node.property or a property alias",
                        ));
                    }
                }
            }
        };
        Ok(rows.binding(&node)?.property(&property)?)
    }

    fn select(
        self,
        rows: &Rows<'_>,
        scope: &Scope<'_>,
    ) -> Result<Vec<crate::plan::tracking::Named<Projection>>, SurfaceError> {
        let mut items = Vec::new();
        for item in &self.items {
            let (name, binding, projection) = match item {
                Item::Node(alias, columns, all) => {
                    let columns = match (columns, all) {
                        (Some(names), _) => Columns::Listed(property_ids(rows, alias, names)?),
                        (None, true) => Columns::All,
                        (None, false) => Columns::Default,
                    };
                    (alias, alias, Some(columns))
                }
                Item::Edge(name, edge) => (name, edge, Some(Columns::Default)),
                Item::Type(name, edge) => (name, edge, None),
            };
            let bound = rows.binding(binding)?;
            items.push(match projection {
                Some(columns) => Projection::Graph {
                    binding: bound.output(),
                    columns,
                }
                .named(name),
                None => Projection::Value(bound.field(EdgeField::Kind)?).named(name),
            });
        }
        if let Shape::Path(name) | Shape::Neighbors(name) = &scope.shape {
            let columns = self.dynamic.unwrap_or(Columns::Default);
            let binding = rows.binding(name)?.output();
            items.push(Projection::Graph { binding, columns }.named(name));
        }
        Ok(items)
    }
}

pub(super) fn property_ids(
    rows: &Rows<'_>,
    alias: &str,
    names: &[String],
) -> Result<Vec<crate::PropertyId>, SurfaceError> {
    let binding = rows.binding(alias)?;
    names
        .iter()
        .map(|name| match binding.property(name)? {
            Expr::Property { property, .. } => Ok(property),
            _ => Err(crate::plan::logical::PlanError::NoEntity.into()),
        })
        .collect()
}

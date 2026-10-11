use std::collections::HashSet;

use crate::Result;
use crate::input::{
    AggExpr, ColumnSelection, DynamicColumnMode, InputAggSort, InputAggregationMetric,
    InputGroupByKey, InputOrderBy, OrderDirection, PropertyRef, QueryType, RelationshipColumn,
    RelationshipColumnKind, RelationshipOrder, TargetRef,
};

use super::super::ast::{AggregateFunction, Expression, Name, Projections, Sort, SortKey, Target};
use super::super::errors::invalid;
use super::Lowering;

impl Lowering {
    pub(super) fn project(&mut self, projections: Projections<'_>) -> Result<()> {
        let (span, items) = match projections {
            Projections::Star(span) => {
                if self.input.query_type != QueryType::Traversal {
                    return Err(invalid(span, "RETURN * is only supported for traversal"));
                }
                return Ok(());
            }
            Projections::Items { span, items } => (span, items),
        };
        let aggregate = items
            .iter()
            .any(|item| matches!(item.expression, Expression::Aggregate { .. }));
        if aggregate {
            if self.input.query_type == QueryType::Neighbors {
                return Err(invalid(
                    span,
                    "neighbors cannot be aggregated; use labeled node patterns",
                ));
            }
            self.input.query_type = QueryType::Aggregation;
        }
        let mut selected = HashSet::new();
        let mut property_nodes = HashSet::new();
        for item in items {
            let alias = item.alias.map(|alias| alias.value);
            match item.expression {
                Expression::Aggregate { function, target } => {
                    let expr = match (function, target) {
                        (AggregateFunction::Count, Target::Property(p)) => {
                            AggExpr::Count(TargetRef {
                                node: p.node.value,
                                property: Some(p.property.value),
                            })
                        }
                        (AggregateFunction::Count, Target::Variable(v)) => {
                            AggExpr::Count(TargetRef {
                                node: v.value,
                                property: None,
                            })
                        }
                        (_, Target::Variable(v)) => {
                            return Err(invalid(
                                v.span,
                                "this aggregate requires a node.property target",
                            ));
                        }
                        (AggregateFunction::Sum, Target::Property(p)) => AggExpr::Sum(p.into()),
                        (AggregateFunction::Avg, Target::Property(p)) => AggExpr::Avg(p.into()),
                        (AggregateFunction::Min, Target::Property(p)) => AggExpr::Min(p.into()),
                        (AggregateFunction::Max, Target::Property(p)) => AggExpr::Max(p.into()),
                    };
                    self.input
                        .aggregation
                        .metrics
                        .push(InputAggregationMetric { expr, alias });
                }
                Expression::DateTrunc {
                    span,
                    unit,
                    property,
                } => {
                    if !aggregate {
                        return Err(invalid(
                            span,
                            "date_trunc is only supported as an aggregation group key",
                        ));
                    }
                    self.input
                        .aggregation
                        .group_by
                        .push(InputGroupByKey::Property {
                            node: property.node.value,
                            property: property.property.value,
                            truncate: Some(unit),
                            alias,
                        });
                }
                Expression::Node {
                    span,
                    variable,
                    properties,
                } => {
                    if self.input.query_type == QueryType::PathFinding {
                        return Err(invalid(
                            span,
                            "node projections require traversal, neighbors, or aggregation",
                        ));
                    }
                    let variable = variable.value;
                    let columns: Vec<String> = properties.into_iter().map(|p| p.value).collect();
                    let property_set: HashSet<_> = columns.iter().collect();
                    if property_set.len() != columns.len() {
                        return Err(invalid(span, "duplicate or overlapping node projection"));
                    }
                    if !aggregate {
                        self.alias(span, alias.clone(), None)?;
                    }
                    let node = self
                        .input
                        .nodes
                        .iter_mut()
                        .find(|n| n.id == variable)
                        .ok_or_else(|| {
                            invalid(span, "node projection references an undefined variable")
                        })?;
                    if selected.insert(variable.clone()) {
                        node.columns = Some(ColumnSelection::List(columns));
                    } else if !aggregate
                        || !matches!(&node.columns, Some(ColumnSelection::List(previous)) if previous.iter().collect::<HashSet<_>>() == property_set)
                    {
                        return Err(invalid(span, "duplicate or overlapping node projection"));
                    }
                    if aggregate {
                        self.input.aggregation.group_by.push(InputGroupByKey::Node {
                            node: variable,
                            alias,
                        });
                    } else {
                        self.order_column(variable);
                    }
                }
                Expression::Property(property) => {
                    let span = property.span;
                    let PropertyRef { node, property } = property.into();
                    if aggregate {
                        self.input
                            .aggregation
                            .group_by
                            .push(InputGroupByKey::Property {
                                node,
                                property,
                                truncate: None,
                                alias,
                            });
                    } else {
                        if self.input.query_type == QueryType::PathFinding {
                            return Err(invalid(
                                span,
                                "property projections require traversal or neighbors",
                            ));
                        }
                        let target = PropertyRef {
                            node: node.clone(),
                            property: property.clone(),
                        };
                        self.alias(span, alias, Some(target))?;
                        self.order_column(node.clone());
                        let input_node = self
                            .input
                            .nodes
                            .iter_mut()
                            .find(|n| n.id == node)
                            .ok_or_else(|| {
                                invalid(span, "property projection references an undefined node")
                            })?;
                        if selected.insert(node.clone()) {
                            property_nodes.insert(node.clone());
                            input_node.columns = Some(ColumnSelection::List(Vec::new()));
                        } else if !property_nodes.contains(&node) {
                            return Err(invalid(span, "duplicate or overlapping node projection"));
                        }
                        match &mut input_node.columns {
                            Some(ColumnSelection::List(columns))
                                if !columns.contains(&property) =>
                            {
                                columns.push(property)
                            }
                            _ => {
                                return Err(invalid(
                                    span,
                                    "duplicate or overlapping node projection",
                                ));
                            }
                        }
                    }
                }
                Expression::Variable(variable) => {
                    self.graph_projection(
                        variable.span,
                        variable,
                        false,
                        alias,
                        aggregate,
                        &mut selected,
                    )?;
                }
                Expression::AllProperties { span, variable } => {
                    self.graph_projection(span, variable, true, alias, aggregate, &mut selected)?;
                }
                Expression::RelationshipType { span, variable } => {
                    let relationship = self.relationship_index(span, &variable)?;
                    self.one_hop(span, &variable)?;
                    if aggregate {
                        return Err(invalid(
                            span,
                            "grouping by relationship type is not supported yet",
                        ));
                    }
                    self.relationship_item(
                        span,
                        alias,
                        span.as_str().to_owned(),
                        relationship,
                        RelationshipColumnKind::Type,
                    )?;
                }
            }
        }
        if self.input.relationship_return.columns.is_empty() {
            self.input.relationship_return.column_order.clear();
        }
        Ok(())
    }

    fn relationship_projection(
        &mut self,
        span: pest::Span<'_>,
        variable: Name<'_>,
        all: bool,
        alias: Option<String>,
        aggregate: bool,
        selected: &mut HashSet<String>,
    ) -> Result<()> {
        if aggregate {
            return Err(invalid(
                span,
                "grouping by a relationship variable is not supported yet",
            ));
        }
        if all {
            return Err(invalid(
                span,
                &format!(
                    "properties({}) is not supported; relationships have no properties",
                    variable.value
                ),
            ));
        }
        self.one_hop(span, &variable)?;
        if !selected.insert(variable.value.clone()) {
            return Err(invalid(span, "duplicate graph projection"));
        }
        let relationship = self.edges[&variable.value];
        self.relationship_item(
            span,
            alias,
            variable.value,
            relationship,
            RelationshipColumnKind::Relationship,
        )
    }

    fn graph_projection(
        &mut self,
        span: pest::Span<'_>,
        variable: Name<'_>,
        all: bool,
        alias: Option<String>,
        aggregate: bool,
        selected: &mut HashSet<String>,
    ) -> Result<()> {
        if self.edges.contains_key(&variable.value) {
            return self.relationship_projection(span, variable, all, alias, aggregate, selected);
        }
        let variable = variable.value;
        let dynamic =
            self.path.as_ref() == Some(&variable) || self.neighbor.as_ref() == Some(&variable);
        if dynamic || (self.input.query_type == QueryType::Neighbors && all) {
            if aggregate {
                return Err(invalid(span, "aggregation cannot return the path variable"));
            }
            if alias.is_some() || (all && !dynamic) {
                return Err(invalid(
                    span,
                    "dynamic graph results cannot be renamed or projected as properties",
                ));
            }
            if all {
                self.input.options.dynamic_columns = DynamicColumnMode::All;
            }
            if !selected.insert(variable) {
                return Err(invalid(span, "duplicate graph projection"));
            }
            return Ok(());
        }
        if self.input.query_type == QueryType::PathFinding {
            return Err(invalid(
                span,
                "path finding requires RETURN of the path variable",
            ));
        }
        let node = self
            .input
            .nodes
            .iter_mut()
            .find(|n| n.id == variable)
            .ok_or_else(|| {
                let (line, column) = span.start_pos().line_col();
                crate::QueryError::ReferenceError(format!(
                    "line {line}, column {column}: projection references undefined node \"{variable}\""
                ))
            })?;
        if !selected.insert(variable.clone())
            && (!aggregate
                || !matches!(
                    (all, &node.columns),
                    (false, None) | (true, Some(ColumnSelection::All))
                ))
        {
            return Err(invalid(span, "duplicate or overlapping node projection"));
        }
        if all {
            node.columns = Some(ColumnSelection::All);
        }
        if aggregate {
            self.input.aggregation.group_by.push(InputGroupByKey::Node {
                node: variable,
                alias,
            });
        } else {
            self.alias(span, alias, None)?;
            self.order_column(variable);
        }
        Ok(())
    }

    fn relationship_item(
        &mut self,
        span: pest::Span<'_>,
        alias: Option<String>,
        written: String,
        relationship: usize,
        kind: RelationshipColumnKind,
    ) -> Result<()> {
        if self.input.query_type != QueryType::Traversal {
            return match alias {
                Some(alias) => Err(invalid(
                    span,
                    &format!(
                        "{alias}: neighbors and path finding return one path column that shows each edge type, so a relationship item cannot be renamed"
                    ),
                )),
                None => Ok(()),
            };
        }
        let name = alias.clone().unwrap_or(written);
        let columns = &mut self.input.relationship_return.columns;
        if columns.iter().any(|column| column.name == name)
            || self.input.nodes.iter().any(|node| node.id == name)
        {
            return Err(invalid(span, "duplicate RETURN column name"));
        }
        columns.push(RelationshipColumn {
            name: name.clone(),
            relationship,
            kind,
        });
        self.order_column(name);
        self.alias(span, alias, None)
    }

    fn order_column(&mut self, name: String) {
        let order = &mut self.input.relationship_return.column_order;
        if !order.contains(&name) {
            order.push(name);
        }
    }

    fn alias(
        &mut self,
        span: pest::Span<'_>,
        alias: Option<String>,
        target: Option<PropertyRef>,
    ) -> Result<()> {
        if let Some(alias) = alias
            && self.aliases.insert(alias, target).is_some()
        {
            return Err(invalid(span, "duplicate alias"));
        }
        Ok(())
    }

    pub(super) fn order(&mut self, sort: Sort<'_>) -> Result<()> {
        match self.input.query_type {
            QueryType::Aggregation => {
                let column = match sort.key {
                    SortKey::Property(key) => {
                        let span = key.span;
                        let PropertyRef { node, property } = key.into();
                        self.input
                            .aggregation
                            .group_by
                            .iter()
                            .find(|g| {
                                g.node() == node
                                    && g.property() == Some(property.as_str())
                                    && g.truncate().is_none()
                            })
                            .map(InputGroupByKey::output_name)
                            .ok_or_else(|| {
                                invalid(
                                    span,
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
                self.input.aggregation.sort = Some(InputAggSort {
                    column,
                    direction: sort.direction,
                });
            }
            QueryType::Traversal => {
                let PropertyRef { node, property } = match sort.key {
                    SortKey::Property(key) => key.into(),
                    SortKey::RelationshipType(name) => {
                        let relationship = self.relationship_index(name.span, &name)?;
                        self.one_hop(name.span, &name)?;
                        self.order_by_relationship(relationship, sort.direction);
                        return Ok(());
                    }
                    SortKey::Variable(name) => {
                        if let Some(relationship) = self.type_column(&name.value) {
                            self.order_by_relationship(relationship, sort.direction);
                            return Ok(());
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
                self.input.order_by = Some(InputOrderBy {
                    node,
                    property,
                    direction: sort.direction,
                });
            }
            _ => {
                return Err(invalid(
                    sort.span,
                    "path finding and neighbors have fixed result ordering",
                ));
            }
        }
        Ok(())
    }

    fn type_column(&self, name: &str) -> Option<usize> {
        self.input
            .relationship_return
            .columns
            .iter()
            .find(|column| column.kind == RelationshipColumnKind::Type && column.name == name)
            .map(|column| column.relationship)
    }

    fn order_by_relationship(&mut self, relationship: usize, direction: OrderDirection) {
        self.input.relationship_return.sort = Some(RelationshipOrder {
            relationship,
            direction,
        });
    }
}

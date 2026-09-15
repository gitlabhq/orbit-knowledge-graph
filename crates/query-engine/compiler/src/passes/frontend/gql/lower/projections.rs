use std::collections::HashSet;

use crate::Result;
use crate::input::{
    AggExpr, ColumnSelection, DynamicColumnMode, InputAggSort, InputAggregationMetric,
    InputGroupByKey, InputOrderBy, PropertyRef, QueryType, TargetRef,
};

use super::super::ast::{AggregateFunction, Expression, Name, Projections, Sort, Target};
use super::super::invalid;
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
            if self.input.query_type != QueryType::Traversal {
                return Err(invalid(
                    span,
                    "path finding and neighbors cannot be aggregated; use labeled node patterns",
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
                    if self.input.query_type != QueryType::Traversal && !aggregate {
                        return Err(invalid(
                            span,
                            "node projections require traversal or aggregation",
                        ));
                    }
                    let variable = variable.value;
                    let columns: Vec<String> = properties.into_iter().map(|p| p.value).collect();
                    if !selected.insert(variable.clone())
                        || columns.iter().collect::<HashSet<_>>().len() != columns.len()
                    {
                        return Err(invalid(span, "duplicate or overlapping node projection"));
                    }
                    if aggregate {
                        if !columns.iter().any(|c| c == "id") {
                            return Err(invalid(
                                span,
                                "an aggregated node projection must include .id to preserve node identity; use a scalar property for property grouping",
                            ));
                        }
                    } else if alias.is_some() {
                        return Err(invalid(
                            span,
                            "traversal node projections cannot be renamed",
                        ));
                    }
                    let node = self
                        .input
                        .nodes
                        .iter_mut()
                        .find(|n| n.id == variable)
                        .ok_or_else(|| {
                            invalid(span, "node projection references an undefined variable")
                        })?;
                    node.columns = Some(ColumnSelection::List(columns));
                    if aggregate {
                        self.input.aggregation.group_by.push(InputGroupByKey::Node {
                            node: variable,
                            alias,
                        });
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
                        if alias.is_some() || self.input.query_type != QueryType::Traversal {
                            return Err(invalid(
                                span,
                                "property projections require traversal and cannot be renamed",
                            ));
                        }
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
            }
        }
        Ok(())
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
        let variable = variable.value;
        let dynamic =
            self.path.as_ref() == Some(&variable) || self.neighbor.as_ref() == Some(&variable);
        if dynamic
            || (self.input.query_type == QueryType::Neighbors
                && (all || self.edges.contains_key(&variable)))
        {
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
                "path finding requires RETURN of the shortestPath variable",
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
        if !selected.insert(variable.clone()) {
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
        } else if alias.is_some() {
            return Err(invalid(
                span,
                "traversal node projections cannot be renamed",
            ));
        }
        Ok(())
    }

    pub(super) fn order(&mut self, sort: Sort<'_>) -> Result<()> {
        match self.input.query_type {
            QueryType::Aggregation => {
                let column = match sort.key {
                    Target::Property(key) => {
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
                    Target::Variable(name) => name.value,
                };
                self.input.aggregation.sort = Some(InputAggSort {
                    column,
                    direction: sort.direction,
                });
            }
            QueryType::Traversal => {
                let key = match sort.key {
                    Target::Property(key) => key,
                    Target::Variable(name) => {
                        return Err(invalid(
                            name.span,
                            "traversal ORDER BY requires node.property",
                        ));
                    }
                };
                let PropertyRef { node, property } = key.into();
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
}

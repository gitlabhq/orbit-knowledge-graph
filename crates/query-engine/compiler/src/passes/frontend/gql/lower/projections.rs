use std::collections::HashSet;

use crate::Result;
use crate::input::{
    AggExpr, ColumnSelection, InputAggSort, InputAggregationMetric, InputGroupByKey, InputOrderBy,
    OrderDirection, PropertyRef, QueryType, TargetRef, TruncateUnit,
};
use pest::iterators::Pair;

use super::super::{Rule, invalid, name, property, unexpected, value::string};
use super::Lowering;

impl Lowering {
    pub(super) fn project(&mut self, clause: Pair<'_, Rule>) -> Result<()> {
        let items = clause.into_inner().next().expect("RETURN has projections");
        let aggregate = items.clone().into_inner().any(|item| {
            item.into_inner()
                .next()
                .is_some_and(|expression| expression.as_rule() == Rule::Aggregate)
        });
        if aggregate {
            if self.input.query_type != QueryType::Traversal {
                return Err(invalid(
                    &items,
                    "path finding and neighbors cannot be aggregated; use labeled node patterns",
                ));
            }
            self.input.query_type = QueryType::Aggregation;
        }
        let mut selected = HashSet::new();
        let mut property_nodes = HashSet::new();
        for item in items.into_inner() {
            if item.as_rule() == Rule::Star {
                if aggregate || self.input.query_type != QueryType::Traversal {
                    return Err(invalid(&item, "RETURN * is only supported for traversal"));
                }
                continue;
            }
            let mut parts = item.clone().into_inner();
            let expression = parts.next().expect("projection has an expression");
            let alias = parts.next().map(name).transpose()?;
            match expression.as_rule() {
                Rule::Aggregate => {
                    let mut parts = expression.into_inner();
                    let function = parts
                        .next()
                        .expect("aggregate has a function")
                        .as_str()
                        .to_ascii_lowercase();
                    let target = parts.next().expect("aggregate has a target");
                    let expr = if function == "count" {
                        let target = if target.as_rule() == Rule::PropertyExpression {
                            let p = property(target)?;
                            TargetRef {
                                node: p.node,
                                property: Some(p.property),
                            }
                        } else {
                            TargetRef {
                                node: name(target)?,
                                property: None,
                            }
                        };
                        AggExpr::Count(target)
                    } else {
                        if target.as_rule() != Rule::PropertyExpression {
                            return Err(invalid(
                                &target,
                                "this aggregate requires a node.property target",
                            ));
                        }
                        let p = property(target)?;
                        match function.as_str() {
                            "sum" => AggExpr::Sum(p),
                            "avg" => AggExpr::Avg(p),
                            "min" => AggExpr::Min(p),
                            "max" => AggExpr::Max(p),
                            _ => unreachable!("grammar restricts aggregate functions"),
                        }
                    };
                    self.input
                        .aggregation
                        .metrics
                        .push(InputAggregationMetric { expr, alias });
                }
                Rule::DateTrunc => {
                    if !aggregate {
                        return Err(invalid(
                            &expression,
                            "date_trunc is only supported as an aggregation group key",
                        ));
                    }
                    let mut parts = expression.clone().into_inner();
                    let unit = string(parts.next().expect("date_trunc has a unit"))?;
                    let truncate = match unit.as_str() {
                        "minute" => TruncateUnit::Minute,
                        "hour" => TruncateUnit::Hour,
                        "day" => TruncateUnit::Day,
                        "week" => TruncateUnit::Week,
                        "month" => TruncateUnit::Month,
                        "quarter" => TruncateUnit::Quarter,
                        "year" => TruncateUnit::Year,
                        _ => {
                            return Err(invalid(
                                &expression,
                                "date_trunc unit must be minute, hour, day, week, month, quarter, or year",
                            ));
                        }
                    };
                    let p = property(parts.next().expect("date_trunc has a property"))?;
                    self.input
                        .aggregation
                        .group_by
                        .push(InputGroupByKey::Property {
                            node: p.node,
                            property: p.property,
                            truncate: Some(truncate),
                            alias,
                        });
                }
                Rule::NodeProjection => {
                    if self.input.query_type != QueryType::Traversal && !aggregate {
                        return Err(invalid(
                            &expression,
                            "node projections require traversal or aggregation",
                        ));
                    }
                    let mut parts = expression.clone().into_inner();
                    let variable = name(parts.next().expect("node projection has a variable"))?;
                    let columns = parts
                        .map(|p| {
                            name(
                                p.into_inner()
                                    .next()
                                    .expect("projection property has a name"),
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    if !selected.insert(variable.clone())
                        || columns.iter().collect::<HashSet<_>>().len() != columns.len()
                    {
                        return Err(invalid(
                            &expression,
                            "duplicate or overlapping node projection",
                        ));
                    }
                    if aggregate {
                        if !columns.iter().any(|c| c == "id") {
                            return Err(invalid(
                                &expression,
                                "an aggregated node projection must include .id to preserve node identity; use a scalar property for property grouping",
                            ));
                        }
                    } else if alias.is_some() {
                        return Err(invalid(
                            &expression,
                            "traversal node projections cannot be renamed",
                        ));
                    }
                    let node = self
                        .input
                        .nodes
                        .iter_mut()
                        .find(|n| n.id == variable)
                        .ok_or_else(|| {
                            invalid(
                                &expression,
                                "node projection references an undefined variable",
                            )
                        })?;
                    node.columns = Some(ColumnSelection::List(columns));
                    if aggregate {
                        self.input.aggregation.group_by.push(InputGroupByKey::Node {
                            node: variable,
                            alias,
                        });
                    }
                }
                Rule::PropertyExpression => {
                    let p = property(expression.clone())?;
                    if aggregate {
                        self.input
                            .aggregation
                            .group_by
                            .push(InputGroupByKey::Property {
                                node: p.node,
                                property: p.property,
                                truncate: None,
                                alias,
                            });
                    } else {
                        if alias.is_some() || self.input.query_type != QueryType::Traversal {
                            return Err(invalid(
                                &expression,
                                "property projections require traversal and cannot be renamed",
                            ));
                        }
                        let node = self
                            .input
                            .nodes
                            .iter_mut()
                            .find(|n| n.id == p.node)
                            .ok_or_else(|| {
                                invalid(
                                    &expression,
                                    "property projection references an undefined node",
                                )
                            })?;
                        if selected.insert(p.node.clone()) {
                            property_nodes.insert(p.node.clone());
                            node.columns = Some(ColumnSelection::List(Vec::new()));
                        } else if !property_nodes.contains(&p.node) {
                            return Err(invalid(
                                &expression,
                                "duplicate or overlapping node projection",
                            ));
                        }
                        match &mut node.columns {
                            Some(ColumnSelection::List(columns))
                                if !columns.contains(&p.property) =>
                            {
                                columns.push(p.property)
                            }
                            _ => {
                                return Err(invalid(
                                    &expression,
                                    "duplicate or overlapping node projection",
                                ));
                            }
                        }
                    }
                }
                Rule::Variable | Rule::AllProperties => {
                    let all = expression.as_rule() == Rule::AllProperties;
                    let variable = if all {
                        name(
                            expression
                                .clone()
                                .into_inner()
                                .next()
                                .expect("properties has a node"),
                        )?
                    } else {
                        name(expression.clone())?
                    };
                    if self.path.as_ref() == Some(&variable)
                        || self.neighbor.as_ref() == Some(&variable)
                        || (self.input.query_type == QueryType::Neighbors
                            && (all || self.edges.contains_key(&variable)))
                    {
                        if alias.is_some() || all {
                            return Err(invalid(
                                &expression,
                                "dynamic graph results cannot be renamed or projected as properties",
                            ));
                        }
                        if !selected.insert(variable) {
                            return Err(invalid(&expression, "duplicate graph projection"));
                        }
                        continue;
                    }
                    if self.input.query_type == QueryType::PathFinding {
                        return Err(invalid(
                            &expression,
                            "path finding requires RETURN of the shortestPath variable",
                        ));
                    }
                    let node = self.input.nodes.iter_mut().find(|n| n.id == variable)
                        .ok_or_else(|| {
                            let (line, column) = expression.line_col();
                            crate::QueryError::ReferenceError(format!(
                                "line {line}, column {column}: projection references undefined node \"{variable}\""
                            ))
                        })?;
                    if !selected.insert(variable.clone()) {
                        return Err(invalid(
                            &expression,
                            "duplicate or overlapping node projection",
                        ));
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
                            &expression,
                            "traversal node projections cannot be renamed",
                        ));
                    }
                }
                _ => return Err(unexpected(&expression)),
            }
        }
        Ok(())
    }

    pub(super) fn order(&mut self, clause: Pair<'_, Rule>) -> Result<()> {
        let sort = clause.into_inner().next().expect("ORDER BY has a sort key");
        let mut parts = sort.clone().into_inner();
        let key = parts.next().expect("sort key has an expression");
        let direction = if parts
            .next()
            .is_some_and(|p| p.as_str().to_ascii_lowercase().starts_with("desc"))
        {
            OrderDirection::Desc
        } else {
            OrderDirection::Asc
        };
        match self.input.query_type {
            QueryType::Aggregation => {
                let column = if key.as_rule() == Rule::PropertyExpression {
                    let PropertyRef { node, property } = property(key.clone())?;
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
                                &key,
                                "ORDER BY must name an aggregate alias or returned group key",
                            )
                        })?
                } else {
                    name(key)?
                };
                self.input.aggregation.sort = Some(InputAggSort { column, direction });
            }
            QueryType::Traversal => {
                if key.as_rule() != Rule::PropertyExpression {
                    return Err(invalid(&key, "traversal ORDER BY requires node.property"));
                }
                let PropertyRef { node, property } = property(key)?;
                self.input.order_by = Some(InputOrderBy {
                    node,
                    property,
                    direction,
                });
            }
            _ => {
                return Err(invalid(
                    &sort,
                    "path finding and neighbors have fixed result ordering",
                ));
            }
        }
        Ok(())
    }
}

use query_data_model::plan::logical::{
    Columns, EdgeField, Expr, LogicalType, Measure, Order, Projection,
};
use query_data_model::plan::tracking::{NodeId, Ref};

use super::Bridge;
use crate::input::{
    AggExpr, ColumnSelection, DynamicColumnMode, InputAggSort, InputAggregationMetric,
    InputGroupByKey, InputOrderBy, OrderDirection, PropertyRef, QueryType, RelationshipColumn,
    RelationshipColumnKind, RelationshipOrder, TargetRef,
};
use crate::{QueryError, Result};

fn invariant(what: &str) -> QueryError {
    QueryError::PipelineInvariant(format!("the bridge cannot express {what}"))
}

impl Bridge<'_> {
    fn name(&self, output: Ref) -> String {
        self.plan.name(output).unwrap_or_default().to_string()
    }

    fn property(
        &self,
        binding: Ref,
        property: query_data_model::PropertyId,
    ) -> Result<(String, String)> {
        Ok((
            self.alias(binding)?,
            self.model.graph().property(property).name.clone(),
        ))
    }

    pub(super) fn aggregate(
        &mut self,
        node: NodeId,
        groups: &[Expr],
        measures: &[Measure],
    ) -> Result<()> {
        self.input.query_type = QueryType::Aggregation;
        let outputs: Vec<Ref> = self.plan.outputs(node).collect();
        for (group, output) in groups.iter().zip(&outputs) {
            let name = self.name(*output);
            let key = match group {
                Expr::Node(binding) => {
                    let node = self.alias(*binding)?;
                    self.nodes.insert(*output, node.clone());
                    let alias = (name != node).then(|| name.clone());
                    InputGroupByKey::Node { node, alias }
                }
                Expr::Property { binding, property }
                | Expr::Truncate {
                    binding, property, ..
                } => {
                    let (node, property) = self.property(*binding, *property)?;
                    let truncate = match group {
                        Expr::Truncate { unit, .. } => {
                            let unit: &str = unit.into();
                            Some(serde_json::from_value(serde_json::Value::from(unit))?)
                        }
                        _ => None,
                    };
                    let mut key = InputGroupByKey::Property {
                        node,
                        property,
                        truncate,
                        alias: None,
                    };
                    if key.output_name() != name
                        && let InputGroupByKey::Property { alias, .. } = &mut key
                    {
                        *alias = Some(name.clone());
                    }
                    key
                }
                _ => return Err(invariant("this group key")),
            };
            self.outputs.insert(*output, name);
            self.input.aggregation.group_by.push(key);
        }
        for (measure, output) in measures.iter().zip(&outputs[groups.len()..]) {
            let name = self.name(*output);
            let target = |expr: &Expr| -> Result<(String, Option<String>)> {
                match expr {
                    Expr::Node(binding) => Ok((self.alias(*binding)?, None)),
                    Expr::Property { binding, property } => {
                        let (node, property) = self.property(*binding, *property)?;
                        Ok((node, Some(property)))
                    }
                    _ => Err(invariant("this aggregate input")),
                }
            };
            let property = |expr: &Expr| -> Result<PropertyRef> {
                match target(expr)? {
                    (node, Some(property)) => Ok(PropertyRef { node, property }),
                    _ => Err(invariant("an aggregate over a node")),
                }
            };
            let (expr, distinct) = match measure {
                Measure::Count(Some(input)) | Measure::CountDistinct(input) => {
                    let (node, property) = target(input)?;
                    let distinct = matches!(measure, Measure::CountDistinct(_));
                    (AggExpr::Count(TargetRef { node, property }), distinct)
                }
                Measure::Sum(input) => (AggExpr::Sum(property(input)?), false),
                Measure::Avg(input) => (AggExpr::Avg(property(input)?), false),
                Measure::Min(input) => (AggExpr::Min(property(input)?), false),
                Measure::Max(input) => (AggExpr::Max(property(input)?), false),
                Measure::Count(None) => return Err(invariant("a count without a target")),
            };
            let mut metric = InputAggregationMetric {
                expr,
                alias: None,
                distinct,
            };
            if metric.output_name() != name {
                metric.alias = Some(name.clone());
            }
            self.outputs.insert(*output, name);
            self.input.aggregation.metrics.push(metric);
        }
        Ok(())
    }

    pub(super) fn order(&mut self, keys: &[(Expr, Order)]) -> Result<()> {
        for (key, order) in keys {
            let direction = match order {
                Order::Ascending => OrderDirection::Asc,
                Order::Descending => OrderDirection::Desc,
            };
            match key {
                Expr::Output(output) => {
                    let column = self
                        .outputs
                        .get(output)
                        .cloned()
                        .ok_or_else(|| invariant("this sort column"))?;
                    self.input.aggregation.sort = Some(InputAggSort { column, direction });
                }
                Expr::Property { binding, property } => {
                    let (node, property) = self.property(*binding, *property)?;
                    self.input.order_by = Some(InputOrderBy {
                        node,
                        property,
                        direction,
                    });
                }
                Expr::Edge {
                    binding,
                    field: EdgeField::Kind,
                } => {
                    let relationship = self.edges[binding];
                    self.input.relationship_return.sort = Some(RelationshipOrder {
                        relationship,
                        direction,
                    });
                }
                _ => return Err(invariant("this sort key")),
            }
        }
        Ok(())
    }

    pub(super) fn select(&mut self, node: NodeId, items: &[Projection]) -> Result<()> {
        let outputs: Vec<Ref> = self.plan.outputs(node).collect();
        let mut order = Vec::new();
        for (item, output) in items.iter().zip(outputs) {
            let name = self.name(output);
            let column = |kind, relationship| RelationshipColumn {
                name: name.clone(),
                relationship,
                kind,
            };
            match item {
                Projection::Graph { binding, columns } => match self.plan.value(*binding) {
                    LogicalType::Entity(_) => {
                        let alias = self.alias(*binding)?;
                        let selection = match columns {
                            Columns::Default => None,
                            Columns::All => Some(ColumnSelection::All),
                            Columns::Listed(listed) => Some(ColumnSelection::List(
                                listed
                                    .iter()
                                    .map(|id| self.model.graph().property(*id).name.clone())
                                    .collect(),
                            )),
                        };
                        let node = self
                            .input
                            .node_mut(&alias)
                            .ok_or_else(|| invariant("this node"))?;
                        node.columns = selection;
                        order.push(alias);
                    }
                    LogicalType::Node | LogicalType::Path => {
                        self.input.options.dynamic_columns = match columns {
                            Columns::All => DynamicColumnMode::All,
                            _ => DynamicColumnMode::Default,
                        };
                    }
                    LogicalType::Edge => {
                        let relationship = self.edges[binding];
                        self.input
                            .relationship_return
                            .columns
                            .push(column(RelationshipColumnKind::Relationship, relationship));
                        order.push(name.clone());
                    }
                    LogicalType::Scalar(_) => {}
                },
                Projection::Value(Expr::Edge {
                    binding,
                    field: EdgeField::Kind,
                }) => {
                    let relationship = self.edges[binding];
                    self.input
                        .relationship_return
                        .columns
                        .push(column(RelationshipColumnKind::Type, relationship));
                    order.push(name.clone());
                }
                Projection::Value(_) => {}
            }
        }
        if !self.input.relationship_return.columns.is_empty() {
            self.input.relationship_return.column_order = order;
        }
        Ok(())
    }
}

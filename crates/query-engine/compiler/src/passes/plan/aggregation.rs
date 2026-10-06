use crate::error::{QueryError, Result};
use query_data_model::QueryDataModel;

use super::helpers::requested_columns;
use crate::input::{
    AggExpr, AggFunction, InputGroupByKey, OrderDirection, TruncateUnit, group_by_output_names,
};

use super::HydrationStrategy;
use super::context::PlanningContext;
use super::physical::{ExecutionPlan, PhysicalSource};
use super::requirements::{Column, Predicate};

#[derive(Clone, PartialEq)]
pub struct Group {
    pub column: Column,
    pub truncate: Option<TruncateUnit>,
}

pub struct Measure {
    pub function: AggFunction,
    pub argument: Option<Column>,
    pub name: query_data_model::bindings::ExportId,
}

pub struct AggregationPlan {
    pub groups: Vec<Group>,
    pub group_outputs: Vec<(Group, query_data_model::bindings::ExportId)>,
    pub measures: Vec<Measure>,
    pub condition: Vec<Predicate>,
    pub order: Option<(query_data_model::bindings::ExportId, OrderDirection)>,
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn aggregation(&mut self, execution: &ExecutionPlan) -> Result<AggregationPlan> {
        let column = |context: &Self, node: &str, property: &str| {
            let relation = context.node_relations.get(node).ok_or_else(|| {
                QueryError::Lowering(format!("node '{node}' has no visible relation"))
            })?;
            context.column(*relation, property)
        };
        let aggregation = &self.input.aggregation;
        let mut source = &execution.source;
        let condition = loop {
            match source {
                PhysicalSource::Latest {
                    aggregate_condition,
                    relation,
                    ..
                } => {
                    break aggregate_condition
                        .iter()
                        .map(|predicate| {
                            predicate.map_columns(|column| {
                                let query_data_model::bindings::ExportOrigin::Stored(stored) = self
                                    .bindings
                                    .origin(column.export())
                                    .map_err(|error| QueryError::Lowering(error.to_string()))?
                                else {
                                    return Err(QueryError::Lowering(
                                        "latest-row condition requires stored columns".into(),
                                    ));
                                };
                                self.stored_column(*relation, stored)
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                }
                PhysicalSource::Join { left, .. } => source = left,
                PhysicalSource::Scope { input, .. } => source = &input.source,
                PhysicalSource::Filter { input, .. } | PhysicalSource::KeyFilter { input, .. } => {
                    source = input
                }
                PhysicalSource::Scan { .. } | PhysicalSource::Union { .. } => break vec![],
            }
        };
        let mut plan = AggregationPlan {
            groups: vec![],
            group_outputs: vec![],
            measures: vec![],
            condition,
            order: None,
        };
        for (group, alias) in aggregation
            .group_by
            .iter()
            .zip(group_by_output_names(&aggregation.group_by))
        {
            match group {
                InputGroupByKey::Property {
                    node,
                    property,
                    truncate,
                    ..
                } => {
                    let group = Group {
                        column: column(self, node, property)?,
                        truncate: *truncate,
                    };
                    let export = self
                        .bindings
                        .project(self.bindings.root())
                        .map_err(|error| QueryError::Lowering(error.to_string()))?;
                    self.names.exports.insert(export, alias);
                    plan.group_outputs.push((group.clone(), export));
                    if !plan.groups.contains(&group) {
                        plan.groups.push(group);
                    }
                }
                InputGroupByKey::Node { node, .. } => {
                    if let Some(metadata) = self.nodes.get(node.as_str()) {
                        for property in requested_columns(&metadata.columns) {
                            let group = Group {
                                column: column(self, node, &property)?,
                                truncate: None,
                            };
                            if !plan.groups.contains(&group) {
                                plan.groups.push(group);
                            }
                        }
                    }
                }
            }
        }
        for metric in &aggregation.metrics {
            let argument = match &metric.expr {
                AggExpr::Count(target) => target
                    .property
                    .as_ref()
                    .filter(|_| {
                        !self
                            .nodes
                            .get(&target.node)
                            .is_some_and(|node| node.hydration == HydrationStrategy::Skip)
                    })
                    .map(|property| column(self, &target.node, property))
                    .transpose()?,
                AggExpr::Sum(property)
                | AggExpr::Avg(property)
                | AggExpr::Min(property)
                | AggExpr::Max(property)
                | AggExpr::Collect(property) => {
                    Some(column(self, &property.node, &property.property)?)
                }
            };
            let export = self
                .bindings
                .project(self.bindings.root())
                .map_err(|error| QueryError::Lowering(error.to_string()))?;
            self.names.exports.insert(export, metric.output_name());
            plan.measures.push(Measure {
                function: metric.expr.function(),
                argument,
                name: export,
            });
        }
        plan.order = aggregation.sort.as_ref().map(|sort| {
            let export = plan
                .group_outputs
                .iter()
                .map(|(_, export)| export)
                .chain(plan.measures.iter().map(|measure| &measure.name))
                .find(|export| self.names.exports[export] == sort.column)
                .expect("validated aggregate output");
            (*export, sort.direction)
        });
        Ok(plan)
    }
}

use query_data_model::QueryDataModel;

use super::helpers::requested_columns;
use crate::input::{
    AggExpr, AggFunction, InputAggSort, InputGroupByKey, TruncateUnit, group_by_output_names,
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
    pub name: String,
}

pub struct AggregationPlan {
    pub groups: Vec<Group>,
    pub group_outputs: Vec<(Group, String)>,
    pub measures: Vec<Measure>,
    pub condition: Vec<Predicate>,
    pub order: Option<InputAggSort>,
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn aggregation(&self, execution: &ExecutionPlan) -> AggregationPlan {
        let aggregation = &self.input.aggregation;
        let mut source = &execution.source;
        let condition = loop {
            match source {
                PhysicalSource::Latest {
                    aggregate_condition,
                    ..
                } => break aggregate_condition.clone(),
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
            order: aggregation.sort.clone(),
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
                        column: Column::new(node, property),
                        truncate: *truncate,
                    };
                    plan.group_outputs.push((group.clone(), alias));
                    if !plan.groups.contains(&group) {
                        plan.groups.push(group);
                    }
                }
                InputGroupByKey::Node { node, .. } => {
                    if let Some(metadata) = self.nodes.get(node.as_str()) {
                        for column in requested_columns(&metadata.columns) {
                            let group = Group {
                                column: Column::new(node, column),
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
                    .map(|property| Column::new(&target.node, property)),
                AggExpr::Sum(property)
                | AggExpr::Avg(property)
                | AggExpr::Min(property)
                | AggExpr::Max(property)
                | AggExpr::Collect(property) => {
                    Some(Column::new(&property.node, &property.property))
                }
            };
            plan.measures.push(Measure {
                function: metric.expr.function(),
                argument,
                name: metric.output_name(),
            });
        }
        plan
    }
}

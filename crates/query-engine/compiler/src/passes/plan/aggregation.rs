use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::OrbitQueryModel;

use super::helpers::requested_columns;
use crate::input::{
    AggExpr, AggFunction, InputAggSort, InputGroupByKey, TruncateUnit, group_by_output_names,
};

use super::context::PlanningContext;
use super::physical::{ExecutionPlan, RelationNode};
use super::requirements::{Column, Predicate};
use super::{Aggregation, QueryPlan, edge_relation};

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

pub(super) fn plan<M>(context: PlanningContext<'_, M>) -> crate::error::Result<QueryPlan>
where
    M: OrbitQueryModel + ?Sized,
{
    let planned = edge_relation::plan(context)?;
    let result = planned.context.aggregation(&planned.execution);
    Ok(QueryPlan::Aggregation(planned.context.finish(
        Aggregation {
            execution: planned.execution,
            result,
        },
    )))
}

impl<M: OrbitQueryModel + ?Sized> PlanningContext<'_, M> {
    pub fn aggregation(&self, execution: &ExecutionPlan) -> AggregationPlan {
        let aggregation = &self.input.aggregation;
        let mut source = execution.root;
        let condition = loop {
            match execution.graph.get(source) {
                RelationNode::Latest { condition, .. } => break condition.clone(),
                RelationNode::Join { left, .. } => source = left.id,
                RelationNode::Filter { input, .. }
                | RelationNode::Project { input, .. }
                | RelationNode::Distinct { input, .. }
                | RelationNode::Bind { input, .. }
                | RelationNode::Sort { input, .. }
                | RelationNode::Limit { input, .. } => source = input.id,
                RelationNode::Owned { input, .. } => source = input.id,
                RelationNode::Read { .. } | RelationNode::UnionAll { .. } => break vec![],
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
                        column: execution.property(node, property),
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
                                column: execution.property(node, &column),
                                truncate: None,
                            };
                            if !plan.groups.contains(&group) {
                                plan.groups.push(group);
                            }
                        }
                        for column in [DEFAULT_PRIMARY_KEY, metadata.redaction_id_column.as_str()] {
                            let column = if column == DEFAULT_PRIMARY_KEY {
                                execution
                                    .bindings
                                    .iter()
                                    .find(|binding| binding.node == *node)
                                    .and_then(|binding| binding.property_source.as_ref())
                                    .map(|source| Column::new(source, column))
                                    .unwrap_or_else(|| execution.property(node, column))
                            } else {
                                execution.property(node, column)
                            };
                            let group = Group {
                                column,
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
                    .filter(|_| self.nodes.get(&target.node).is_some_and(|node| node.joined))
                    .map(|property| execution.property(&target.node, property)),
                AggExpr::Sum(property)
                | AggExpr::Avg(property)
                | AggExpr::Min(property)
                | AggExpr::Max(property)
                | AggExpr::Collect(property) => {
                    Some(execution.property(&property.node, &property.property))
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

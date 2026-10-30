//! Aggregation: plan the pattern like a traversal, then derive groups, measures, and the deferred
//! latest-row condition from the relation graph.

use std::collections::HashMap;

use ontology::constants::DEFAULT_PRIMARY_KEY;

use query_data_model::OrbitQueryModel;

use crate::error::Result;
use crate::input::{
    AggExpr, AggFunction, Input, InputAggSort, InputGroupByKey, TruncateUnit, group_by_output_names,
};
use crate::passes::plan::graph::{ExecutionPlan, RelationNode};
use crate::passes::plan::pattern::Pattern;
use crate::passes::plan::predicates::{Column, OutputValue, Predicate};
use crate::passes::plan::{NodePlan, Plan, property_value};

#[derive(Clone, PartialEq)]
pub struct Group {
    pub value: OutputValue,
    pub truncate: Option<TruncateUnit>,
}

pub struct Measure {
    pub function: AggFunction,
    pub argument: Option<OutputValue>,
    pub distinct: bool,
    pub name: String,
}

pub struct Aggregation {
    pub execution: ExecutionPlan,
    pub result: AggregationPlan,
}

pub struct AggregationPlan {
    pub groups: Vec<Group>,
    pub group_outputs: Vec<(Group, String)>,
    pub measures: Vec<Measure>,
    pub condition: Vec<Predicate>,
    pub order: Option<InputAggSort>,
}

pub(crate) fn plan<M: OrbitQueryModel + ?Sized>(
    input: &Input,
    model: &M,
) -> Result<Plan<Aggregation>> {
    let pattern = Pattern::new(input, model)?;
    let execution = pattern.execution()?;
    let result = aggregation_plan(input, &pattern.nodes, &execution);
    Ok(pattern.into_plan(execution, |execution| Aggregation { execution, result }))
}

fn aggregation_plan(
    input: &Input,
    nodes: &HashMap<String, NodePlan>,
    execution: &ExecutionPlan,
) -> AggregationPlan {
    let aggregation = &input.aggregation;
    let mut plan = AggregationPlan {
        groups: vec![],
        group_outputs: vec![],
        measures: vec![],
        condition: latest_row_condition(execution),
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
                    value: property_value(nodes, node, execution.property(node, property)),
                    truncate: *truncate,
                };
                plan.group_outputs.push((group.clone(), alias));
                add_group(&mut plan.groups, group);
            }
            InputGroupByKey::Relationship {
                relationship,
                property,
                ..
            } => {
                let group = Group {
                    value: OutputValue::Column(relationship_column(*relationship, property)),
                    truncate: None,
                };
                plan.group_outputs.push((group.clone(), alias));
                add_group(&mut plan.groups, group);
            }
            InputGroupByKey::Node { node, .. } => {
                let Some(metadata) = nodes.get(node.as_str()) else {
                    continue;
                };
                for column in metadata.requested_columns() {
                    add_group(
                        &mut plan.groups,
                        Group {
                            value: property_value(nodes, node, execution.property(node, &column)),
                            truncate: None,
                        },
                    );
                }
                let primary_key = execution
                    .bindings
                    .iter()
                    .find(|binding| binding.node == *node)
                    .and_then(|binding| binding.property_source.as_ref())
                    .map(|source| Column::new(source, DEFAULT_PRIMARY_KEY))
                    .unwrap_or_else(|| execution.property(node, DEFAULT_PRIMARY_KEY));
                add_group(
                    &mut plan.groups,
                    Group {
                        value: OutputValue::Column(primary_key),
                        truncate: None,
                    },
                );
                add_group(
                    &mut plan.groups,
                    Group {
                        value: OutputValue::Column(
                            execution.property(node, &metadata.redaction_id_column),
                        ),
                        truncate: None,
                    },
                );
            }
        }
    }
    for metric in &aggregation.metrics {
        if let Some(relationship) = metric.relationship {
            plan.measures.push(Measure {
                function: metric.expr.function(),
                argument: metric.expr.property().map(|property| {
                    OutputValue::Column(relationship_column(relationship, property))
                }),
                distinct: metric.distinct,
                name: metric.output_name(),
            });
            continue;
        }
        let argument = match &metric.expr {
            AggExpr::Count(target) if metric.distinct => Some(property_value(
                nodes,
                &target.node,
                execution.property(
                    &target.node,
                    target.property.as_deref().unwrap_or(DEFAULT_PRIMARY_KEY),
                ),
            )),
            AggExpr::Count(target) => target
                .property
                .as_ref()
                .filter(|_| nodes.get(&target.node).is_some_and(|node| node.joined))
                .map(|property| {
                    property_value(
                        nodes,
                        &target.node,
                        execution.property(&target.node, property),
                    )
                }),
            AggExpr::Sum(property)
            | AggExpr::Avg(property)
            | AggExpr::Min(property)
            | AggExpr::Max(property)
            | AggExpr::Collect(property) => Some(property_value(
                nodes,
                &property.node,
                execution.property(&property.node, &property.property),
            )),
        };
        plan.measures.push(Measure {
            function: metric.expr.function(),
            argument,
            distinct: metric.distinct,
            name: metric.output_name(),
        });
    }
    plan
}

fn relationship_column(relationship: usize, property: &str) -> Column {
    Column::new(
        crate::constants::edge_relation_alias(relationship),
        property,
    )
}

fn add_group(groups: &mut Vec<Group>, group: Group) {
    if !groups.contains(&group) {
        groups.push(group);
    }
}

/// The condition a single edge relation defers past its latest-row pick, so the measures apply it instead of the scan.
fn latest_row_condition(execution: &ExecutionPlan) -> Vec<Predicate> {
    let mut source = execution.root;
    loop {
        match execution.graph.get(source) {
            RelationNode::Latest { condition, .. } => break condition.clone(),
            RelationNode::Join { left, .. } => source = left.id,
            RelationNode::Owned { input, .. }
            | RelationNode::Filter { input, .. }
            | RelationNode::Project { input, .. }
            | RelationNode::Bind { input, .. } => source = input.id,
            RelationNode::Read { .. } | RelationNode::UnionAll { .. } => break vec![],
        }
    }
}

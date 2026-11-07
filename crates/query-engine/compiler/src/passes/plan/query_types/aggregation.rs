//! Aggregation: plan the pattern like a traversal, then derive groups, measures, and the deferred
//! latest-row condition from the relation graph.

use std::collections::HashMap;

use ontology::constants::DEFAULT_PRIMARY_KEY;
use query_data_model::OrbitQueryModel;
use query_data_model::implementations::relational::plan::AggregateFunction;
use query_data_model::plan::logical::{
    Expr, LogicalOperation, Measure as Logical, Order, TimeUnit,
};
use query_data_model::plan::tracking::Ref;

use crate::error::Result;
use crate::logical::{Query, binding};
use crate::passes::plan::graph::{ExecutionPlan, RelationNode};
use crate::passes::plan::pattern::Pattern;
use crate::passes::plan::predicates::{Column, OutputValue, Predicate};
use crate::passes::plan::{NodePlan, Plan, property_value};

#[derive(Clone, PartialEq)]
pub struct Group {
    pub value: OutputValue,
    pub truncate: Option<TimeUnit>,
}

pub struct Measure {
    pub function: AggregateFunction,
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
    /// The output column the rows sort by, and whether descending.
    pub order: Option<(String, bool)>,
}

pub(crate) fn plan<'a, M: OrbitQueryModel + ?Sized>(
    query: Query<'a>,
    model: &'a M,
) -> Result<Plan<Aggregation>> {
    let pattern = Pattern::new(query, model)?;
    let execution = pattern.execution()?;
    let result = aggregation_plan(query, &pattern.nodes, &execution);
    Ok(pattern.into_plan(execution, |execution| Aggregation { execution, result }))
}

fn aggregation_plan(
    query: Query<'_>,
    nodes: &HashMap<String, NodePlan>,
    execution: &ExecutionPlan,
) -> AggregationPlan {
    let mut plan = AggregationPlan {
        groups: vec![],
        group_outputs: vec![],
        measures: vec![],
        condition: latest_row_condition(execution),
        order: query
            .sort_keys()
            .into_iter()
            .rev()
            .find_map(|(key, order)| match key {
                Expr::Output(output) => {
                    Some((query.name(output).to_string(), order == Order::Descending))
                }
                _ => None,
            }),
    };
    let Some((groups, measures, outputs)) = aggregate(query) else {
        return plan;
    };
    let graph = query.model.graph();
    let value = |node: &str, property: &str| {
        property_value(nodes, node, execution.property(node, property))
    };
    for (group, output) in groups.iter().zip(&outputs) {
        let node = query.name(binding(*group));
        match group {
            Expr::Property { property, .. } | Expr::Truncate { property, .. } => {
                let group = Group {
                    value: value(node, &graph.property(*property).name),
                    truncate: match group {
                        Expr::Truncate { unit, .. } => Some(*unit),
                        _ => None,
                    },
                };
                plan.group_outputs
                    .push((group.clone(), query.name(*output).to_string()));
                add_group(&mut plan.groups, group);
            }
            Expr::Node(_) => {
                let Some(metadata) = nodes.get(node) else {
                    continue;
                };
                for column in &metadata.columns {
                    add_group(
                        &mut plan.groups,
                        Group {
                            value: value(node, column),
                            truncate: None,
                        },
                    );
                }
                let primary_key = execution
                    .bindings
                    .iter()
                    .find(|binding| binding.node == node)
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
            _ => {}
        }
    }
    for (measure, output) in measures.iter().zip(&outputs[groups.len()..]) {
        let (function, input, distinct) = match measure {
            Logical::Count(input) => (AggregateFunction::Count, input.as_ref(), false),
            Logical::CountDistinct(input) => (AggregateFunction::Count, Some(input), true),
            Logical::Sum(input) => (AggregateFunction::Sum, Some(input), false),
            Logical::Avg(input) => (AggregateFunction::Avg, Some(input), false),
            Logical::Min(input) => (AggregateFunction::Min, Some(input), false),
            Logical::Max(input) => (AggregateFunction::Max, Some(input), false),
        };
        let argument = input.and_then(|input| {
            let node = query.name(binding(*input));
            let property = match input {
                Expr::Property { property, .. } => Some(graph.property(*property).name.as_str()),
                _ => None,
            };
            match (function, distinct) {
                (AggregateFunction::Count, true) => {
                    Some(value(node, property.unwrap_or(DEFAULT_PRIMARY_KEY)))
                }
                (AggregateFunction::Count, false) => property
                    .filter(|_| nodes.get(node).is_some_and(|node| node.joined))
                    .map(|property| value(node, property)),
                _ => property.map(|property| value(node, property)),
            }
        });
        plan.measures.push(Measure {
            function,
            argument,
            distinct,
            name: query.name(*output).to_string(),
        });
    }
    plan
}

fn aggregate<'p>(query: Query<'p>) -> Option<(&'p [Expr], &'p [Logical], Vec<Ref>)> {
    query
        .operations()
        .find_map(|(node, operation)| match operation {
            LogicalOperation::Aggregate { groups, measures } => Some((
                &groups[..],
                &measures[..],
                query.plan.outputs(node).collect(),
            )),
            _ => None,
        })
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

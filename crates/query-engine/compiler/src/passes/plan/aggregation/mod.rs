//! Aggregation: groups, measures, and the order of the result rows, read from the matched pattern.

pub(super) mod emit;

use std::collections::HashMap;

use query_data_model::implementations::relational::plan::AggregateFunction;
use query_data_model::plan::logical::{
    Expr, LogicalOperation, Measure as Logical, Order, TimeUnit,
};
use query_data_model::plan::tracking::Ref;

use crate::logical::{Query, binding};
use crate::passes::plan::pattern::Matched;
use crate::passes::plan::predicates::{Column, OutputValue, Predicate};
use crate::passes::plan::{NodePlan, property_value};

#[derive(Clone, PartialEq)]
pub struct Group {
    pub value: OutputValue,
    pub truncate: Option<TimeUnit>,
}

pub struct Measure {
    pub function: AggregateFunction,
    /// The measured values; a distinct count of a composite node counts every identity part.
    pub arguments: Vec<OutputValue>,
    pub distinct: bool,
    pub name: String,
}

pub struct AggregationPlan {
    pub groups: Vec<Group>,
    pub group_outputs: Vec<(Group, String)>,
    pub measures: Vec<Measure>,
    pub condition: Vec<Predicate>,
    /// The output column the rows sort by, and whether descending.
    pub order: Option<(String, bool)>,
}

pub(crate) fn aggregation_plan(
    query: Query<'_>,
    nodes: &HashMap<String, NodePlan>,
    execution: &Matched,
) -> crate::error::Result<AggregationPlan> {
    let mut plan = AggregationPlan {
        groups: vec![],
        group_outputs: vec![],
        measures: vec![],
        condition: execution.condition.clone(),
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
        return Ok(plan);
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
                let source = execution
                    .bindings
                    .iter()
                    .find(|binding| binding.node == node)
                    .and_then(|binding| binding.property_source.as_ref());
                for part in &metadata.identity {
                    let primary_key = source
                        .map(|source| Column::new(source, part))
                        .unwrap_or_else(|| execution.property(node, part));
                    add_group(
                        &mut plan.groups,
                        Group {
                            value: OutputValue::Column(primary_key),
                            truncate: None,
                        },
                    );
                }
                if let Some(redaction) = &metadata.redaction_id_column {
                    add_group(
                        &mut plan.groups,
                        Group {
                            value: OutputValue::Column(execution.property(node, redaction)),
                            truncate: None,
                        },
                    );
                }
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
        let arguments: Vec<OutputValue> = match input {
            None => Vec::new(),
            Some(input) => {
                let node = query.name(binding(*input));
                let property = match input {
                    Expr::Property { property, .. } => {
                        Some(graph.property(*property).name.as_str())
                    }
                    _ => None,
                };
                let measured: Vec<&str> = match (function, distinct) {
                    (AggregateFunction::Count, true) => match (property, nodes.get(node)) {
                        (Some(property), _) => vec![property],
                        (None, Some(node)) => node.identity.iter().map(String::as_str).collect(),
                        (None, None) => Vec::new(),
                    },
                    (AggregateFunction::Count, false) => property
                        .filter(|_| nodes.get(node).is_some_and(|node| node.joined))
                        .into_iter()
                        .collect(),
                    _ => property.into_iter().collect(),
                };
                measured
                    .into_iter()
                    .map(|property| value(node, property))
                    .collect()
            }
        };
        plan.measures.push(Measure {
            function,
            arguments,
            distinct,
            name: query.name(*output).to_string(),
        });
    }
    Ok(plan)
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

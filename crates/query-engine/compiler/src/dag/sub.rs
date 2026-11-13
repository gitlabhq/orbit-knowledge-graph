//! The logical plan of one stage: the bindings its connection holds, the steps and conditions
//! among them, and the columns later stages read, rebuilt through the logical builder.

use std::collections::HashMap;

use orbit_server_config::QueryLimits;
use query_data_model::PropertyId;
use query_data_model::plan::logical::{
    Columns, Logical, LogicalOperation, LogicalPlan, Projection, Step,
};
use query_data_model::plan::tracking::{Language, Ref};

use super::cut::Group;
use crate::error::{QueryError, Result};
use crate::logical::Query;

/// A stage reads at most the key cap of rows, plus one that proves the cap was passed; the runner
/// fails a stage that returns that extra row.
pub(super) fn plan(
    query: Query<'_>,
    group: &Group,
    needed: &HashMap<Ref, Vec<PropertyId>>,
    limits: &QueryLimits,
    connections: &[String],
) -> Result<LogicalPlan> {
    let rows = limits.stage_keys as u64 + 1;
    let limits = QueryLimits {
        max_rows: limits.max_rows.max(rows),
        ..*limits
    };
    let catalog = query.model.logical(&limits).as_admin().across(connections);
    let graph = query.model.graph();
    let entity_name = |binding: Ref| -> Result<&str> {
        query
            .entity_name(binding)
            .ok_or_else(|| QueryError::PipelineInvariant("a binding has no entity".into()))
    };
    let mine = |reference: Ref| group.bindings.contains(&reference);
    catalog.build(|q| {
        let first = group.bindings[0];
        let mut rows = q.entities_as(catalog.entity(entity_name(first)?)?, query.name(first))?;
        let mut map: HashMap<Ref, Ref> =
            HashMap::from([(first, rows.binding(query.name(first))?.output())]);
        let mut matched: Vec<(Ref, String)> = Vec::new();
        for (node, operation) in query.operations() {
            let outputs: Vec<Ref> = query.plan.outputs(node).collect();
            match operation {
                LogicalOperation::Follow {
                    from,
                    relationships,
                    direction,
                    range,
                    ..
                } if mine(*from) && outputs.last().is_some_and(|to| mine(*to)) => {
                    let [.., edge, to] = outputs[..] else {
                        continue;
                    };
                    let from_binding = rows.binding(query.name(*from))?;
                    let mut step = Step::new(*direction)
                        .hops(*range)
                        .named(query.name(to))
                        .to(catalog.entity(entity_name(to)?)?);
                    for relationship in relationships {
                        step = step.via([
                            catalog.relationship(&graph.relationship(*relationship).name)?
                        ]);
                    }
                    if let Some(name) = query.plan.name(edge) {
                        step = step.edge(name);
                    }
                    rows = q.traverse(rows, from_binding, step)?;
                    map.insert(to, rows.binding(query.name(to))?.output());
                    if let Ok(bound) = rows.binding(query.name(edge)) {
                        map.insert(edge, bound.output());
                    }
                }
                LogicalOperation::Filter(predicate) => {
                    let mut operation = LogicalOperation::Filter(predicate.clone());
                    let mut foreign = false;
                    let mut touched = false;
                    Logical::references(
                        &mut operation,
                        &mut |reference| match map.get(reference) {
                            Some(mapped) => {
                                touched = true;
                                *reference = *mapped;
                            }
                            None => foreign = true,
                        },
                    );
                    match (touched, foreign) {
                        (true, true) => {
                            return Err(QueryError::Validation(
                                "a condition compares nodes on different connections".into(),
                            ));
                        }
                        (true, false) => {
                            let LogicalOperation::Filter(predicate) = operation else {
                                unreachable!("a filter stays a filter");
                            };
                            rows = q.filter(rows, predicate)?;
                        }
                        _ => {}
                    }
                }
                LogicalOperation::Structural(structural) => {
                    let Some(binding) = super::cut::referenced(structural.target) else {
                        continue;
                    };
                    let Some(mapped) = map.get(&binding) else {
                        continue;
                    };
                    let mut target = structural.target;
                    let mut operation = LogicalOperation::Structural(structural.clone());
                    Logical::references(&mut operation, &mut |reference| *reference = *mapped);
                    if let LogicalOperation::Structural(rebound) = operation {
                        target = rebound.target;
                    }
                    let language: &str = structural.language.into();
                    rows = q.structural(rows, target, structural.pattern.text(), language)?;
                    for output in &outputs {
                        matched.push((*output, query.name(*output).to_string()));
                    }
                }
                _ => {}
            }
        }
        let mut items = Vec::new();
        for binding in &group.bindings {
            let properties = needed.get(binding).cloned().unwrap_or_default();
            let bound = rows.binding(query.name(*binding))?.output();
            let projection = Projection::Graph {
                binding: bound,
                columns: Columns::Listed(properties),
            };
            items.push(projection.named(query.name(*binding)));
        }
        for (_, name) in matched
            .iter()
            .filter(|(output, _)| returned(query, *output))
        {
            items.push(Projection::Value(rows.column(name)?).named(name.clone()));
        }
        let rows = q.select(rows, items)?;
        Ok(q.limit(rows, limits.stage_keys as u64 + 1)?)
    })
}

/// Whether the query returns a match column.
fn returned(query: Query<'_>, output: Ref) -> bool {
    query.selected().iter().any(|item| {
        matches!(item, Projection::Value(query_data_model::plan::logical::Expr::Output(selected)) if *selected == output)
    })
}

use std::collections::HashMap;

use orbit_authorization::Key;
use orbit_server_config::QueryLimits;
use query_data_model::Model;
use query_data_model::plan::logical::LogicalOperation;
use query_data_model::plan::tracking::Ref;
use query_data_model::surface::Statement;
use serde::Serialize;

use super::KeyInput;
use crate::error::{QueryError, Result};

/// The keys a producer stage returned for one key input.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct KeySet {
    pub input: KeyInput,
    pub keys: Vec<Key>,
}

/// Binds each key set into the stage's logical plan as an `IN` filter on its binding, right
/// where the plan binds it, so the stage's planner reads the keys before it plans.
pub fn bind(
    statement: Statement,
    sets: &[KeySet],
    model: &Model,
    limits: &QueryLimits,
) -> Result<Statement> {
    for set in sets {
        if set.keys.len() > limits.stage_keys {
            return Err(QueryError::Validation(format!(
                "stage {} returned {} keys for {}; a stage hands at most {} to the next",
                set.input.from,
                set.keys.len(),
                set.input.binding,
                limits.stage_keys
            )));
        }
    }
    let limits = QueryLimits {
        in_values: limits.in_values.max(limits.stage_keys),
        pinned_ids: limits.pinned_ids.max(limits.stage_keys),
        ..*limits
    };
    let names: HashMap<Ref, String> = statement
        .plan
        .nodes()
        .flat_map(|node| statement.plan.outputs(node))
        .filter_map(|output| Some((output, statement.plan.name(output)?.to_string())))
        .collect();
    let catalog = model.logical(&limits).as_admin();
    let plan = catalog.rebuild(statement.plan, |q, step| {
        let binds = matches!(
            step.operation,
            LogicalOperation::Entities(_) | LogicalOperation::Follow { .. }
        );
        let bound = step
            .outputs()
            .last()
            .and_then(|output| names.get(output))
            .cloned();
        let mut rows = step.keep(q)?;
        for set in sets
            .iter()
            .filter(|set| binds && bound.as_ref() == Some(&set.input.binding))
        {
            let binding = rows.binding(&set.input.binding)?;
            let [property] = &set.input.properties[..] else {
                return Err(query_data_model::plan::logical::PlanError::shape(
                    "a composite key set pins one binding's key columns together",
                ));
            };
            let values = set
                .keys
                .iter()
                .map(crate::logical::anchors::literal)
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    query_data_model::plan::logical::PlanError::shape(
                        "a single key column reads single keys",
                    )
                })?;
            rows = q.filter(rows, binding.property(property)?.one_of(values))?;
        }
        Ok(rows)
    })?;
    Ok(Statement { plan, ..statement })
}

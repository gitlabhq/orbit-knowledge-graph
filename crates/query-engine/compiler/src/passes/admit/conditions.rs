use crate::error::{QueryError, Result};
use crate::input::{Condition, Input, QueryType};

pub(super) fn conditions(
    input: &Input,
    model: &impl query_data_model::OrbitQueryModel,
) -> Result<()> {
    let compares_nodes = input.conditions().any(Condition::compares_aliases);
    if compares_nodes && input.query_type != QueryType::Traversal {
        return Err(QueryError::Validation(
            "cross-node property comparisons are only supported in traversal queries".into(),
        ));
    }
    let shape = match input.query_type {
        QueryType::PathFinding => Some("shortest paths"),
        QueryType::Neighbors => Some("neighbors queries"),
        _ => None,
    };
    if let Some(shape) = shape
        && input.has_negation()
    {
        return Err(QueryError::Validation(format!(
            "{shape} support NOT only on =, <>, IS NULL, and IS NOT NULL conditions; use a traversal for other negated conditions"
        )));
    }
    let negated = input
        .predicates
        .iter()
        .flat_map(|root| root.negated_leaves());
    for (alias, property) in negated.flat_map(Condition::node_properties) {
        let Some(entity) = input.entity_of(alias) else {
            continue;
        };
        if model.virtual_source(entity, property).is_some() {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: virtual columns cannot be used under NOT"
            )));
        }
    }
    Ok(())
}

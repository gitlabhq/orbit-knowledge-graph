use crate::error::{QueryError, Result};
use crate::input::{Condition, Input};

pub(super) fn conditions(
    input: &Input,
    model: &impl query_data_model::OrbitQueryModel,
) -> Result<()> {
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

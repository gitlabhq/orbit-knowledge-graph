//! Rejects queries the engine will not run: shapes the planner does not
//! support, properties the backend does not store, conditions past the
//! engine limits, and scans with no selective node.

mod conditions;
mod limits;
mod rules;
mod selectivity;

use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{FilterOp, Input, InputFilter, InputNode, PredicateTarget, QueryType};

pub fn admit(input: &Input, model: &impl OrbitQueryModel, selectivity: bool) -> Result<()> {
    limits::predicates(input)?;
    limits::filters(input)?;
    conditions::conditions(input, model)?;
    filters(input, model)?;
    rules::epoch(input, model)?;
    rules::aggregation_text(input, model)?;
    rules::order(input, model)?;
    buckets(input)?;
    if selectivity {
        selectivity::selectivity(input)?;
    }
    Ok(())
}

fn filters(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    let conditions = input
        .conditions()
        .filter_map(|condition| match &condition.target {
            PredicateTarget::Node(alias) => Some((
                input.node(alias)?,
                condition.property.as_str(),
                std::slice::from_ref(&condition.filter),
            )),
            PredicateTarget::Relationship(_) => None,
        });
    let attached = input.nodes.iter().flat_map(|node| {
        node.filters
            .iter()
            .map(move |(property, filters)| (node, property.as_str(), filters.as_slice()))
    });
    for (node, property, filters) in attached.chain(conditions) {
        let Some(entity) = node.entity.as_deref() else {
            continue;
        };
        filter(input, model, node, entity, property, filters)?;
    }
    Ok(())
}

fn filter(
    input: &Input,
    model: &impl OrbitQueryModel,
    node: &InputNode,
    entity: &str,
    property: &str,
    filters: &[InputFilter],
) -> Result<()> {
    if model.virtual_source(entity, property).is_some() {
        if matches!(
            input.query_type,
            QueryType::Aggregation | QueryType::Neighbors | QueryType::PathFinding
        ) {
            let query_type: &str = input.query_type.into();
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: virtual columns cannot be \
                 filtered in {query_type} queries"
            )));
        }
        if node.node_ids.is_empty() {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: virtual columns can only be \
                 filtered on nodes with explicit node_ids; searching by virtual \
                 column content is not yet supported"
            )));
        }
    }
    let tokens = filters.iter().any(|filter| {
        matches!(
            filter.op,
            Some(FilterOp::TokenMatch | FilterOp::AllTokens | FilterOp::AnyTokens)
        )
    });
    let indexed = model
        .property(entity, property)
        .is_some_and(|property| model.has_text_index(property.id));
    if tokens && !indexed {
        return Err(QueryError::Validation(format!(
            "filter on \"{property}\" for {entity}: \
             token operators (token_match/all_tokens/any_tokens) require a text index on the field"
        )));
    }
    Ok(())
}

fn buckets(input: &Input) -> Result<()> {
    for (index, group) in input.aggregation.group_by.iter().enumerate() {
        let (Some(unit), Some(property)) = (group.truncate(), group.property()) else {
            continue;
        };
        let Some(node) = input.node(group.node()) else {
            continue;
        };
        if unit.requires_selectivity_guard()
            && node.node_ids.is_empty()
            && !node.filters.contains_key(property)
        {
            return Err(QueryError::Validation(format!(
                "group_by[{index}]: truncate \"{}\" on \"{property}\" requires either node_ids on \"{}\" \
                 or at least one filter on \"{property}\" to bound bucket cardinality",
                unit.name(),
                node.id,
            )));
        }
    }
    Ok(())
}

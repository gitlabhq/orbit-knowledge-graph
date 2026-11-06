use query_data_model::OrbitQueryModel;

use crate::error::{QueryError, Result};
use crate::input::{Input, PredicateTarget, QueryType};

pub(super) fn aggregation_text(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    if input.query_type != QueryType::Aggregation {
        return Ok(());
    }
    let rows = input.aggregation_row_nodes(model);
    let attached = input.nodes.iter().flat_map(|node| {
        node.filters.iter().flat_map(move |(property, filters)| {
            filters
                .iter()
                .map(move |filter| (node.id.as_str(), property.as_str(), filter.op))
        })
    });
    let conditions = input.conditions().flat_map(|condition| {
        condition
            .node_properties()
            .map(move |(alias, property)| (alias, property, condition.filter.op))
    });
    for (alias, property, op) in attached.chain(conditions) {
        if !op.is_some_and(|op| op.is_like() || op.is_token() || op.is_ordering()) {
            continue;
        }
        let Some(entity) = input.entity_of(alias) else {
            continue;
        };
        if rows.contains(alias)
            || model.entity_authorized_by_namespace(entity)
            || !model.property_is_free_text(entity, property)
        {
            continue;
        }
        return Err(QueryError::Validation(format!(
            "aggregation filter on {entity}.{property}: text matching and ordering require grouping by \"{alias}\""
        )));
    }
    Ok(())
}

pub(super) fn epoch(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    let attached = input.nodes.iter().flat_map(|node| {
        node.filters
            .iter()
            .flat_map(move |(property, filters)| filters.iter().map(move |f| (node, property, f)))
    });
    let conditions = input
        .conditions()
        .filter_map(|condition| match &condition.target {
            PredicateTarget::Node(alias) => {
                Some((input.node(alias)?, &condition.property, &condition.filter))
            }
            PredicateTarget::Relationship(_) => None,
        });
    for (node, property, filter) in attached.chain(conditions) {
        let (Some(entity), Some(value)) = (node.entity.as_deref(), filter.value.as_ref()) else {
            continue;
        };
        let values = value
            .as_array()
            .map_or(std::slice::from_ref(value), Vec::as_slice);
        let epoch_null = model
            .property(entity, property)
            .is_some_and(|property| model.null_as_epoch(property.id));
        if epoch_null
            && values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .any(at_or_before_epoch)
        {
            return Err(QueryError::Validation(format!(
                "filter on \"{property}\" for {entity}: \
                 values on or before 1970-01-01 stand for null; use is_null or is_not_null"
            )));
        }
    }
    Ok(())
}

fn at_or_before_epoch(value: &str) -> bool {
    let date = value.get(..10).filter(|date| {
        date.bytes().enumerate().all(|(i, byte)| match i {
            4 | 7 => byte == b'-',
            _ => byte.is_ascii_digit(),
        })
    });
    date.is_some_and(|date| date <= "1970-01-01")
}

pub(super) fn order(input: &Input, model: &impl OrbitQueryModel) -> Result<()> {
    let Some(order) = &input.order_by else {
        return Ok(());
    };
    let Some(entity) = input.entity_of(&order.node) else {
        return Ok(());
    };
    if model.virtual_source(entity, &order.property).is_some() {
        return Err(QueryError::Validation(format!(
            "order_by property {}.{} is virtual and cannot be read in SQL",
            order.node, order.property
        )));
    }
    Ok(())
}

use std::collections::HashMap;

use super::{BoundFilter, DenormalizedDirection, DenormalizedKey, DenormalizedProperty};
use crate::input::{ColumnSelection, FilterOp, InputFilter};
use query_data_model::RelationalMapping;

pub enum FilterOwner<'a> {
    Entity(query_data_model::EntityId),
    Tables(&'a [String]),
}

pub fn ordered_filters(
    filters: &HashMap<String, Vec<InputFilter>>,
    owner: FilterOwner<'_>,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> Vec<(String, BoundFilter)> {
    let mut properties: Vec<_> = filters.iter().collect();
    properties.sort_unstable_by_key(|(property, _)| *property);
    let mut bound: Vec<_> = properties
        .into_iter()
        .flat_map(|(property, filters)| {
            filters.iter().map(|filter| {
                (
                    property.clone(),
                    bind_filter(property, filter.clone(), &owner, model),
                )
            })
        })
        .collect();
    let key_narrowed = bound.iter().any(|(_, filter)| {
        filter.in_sort_key
            && matches!(
                filter.filter.op.unwrap_or(FilterOp::Eq),
                FilterOp::Eq | FilterOp::In
            )
    });
    if key_narrowed {
        for (_, filter) in &mut bound {
            filter.text_indexed = false;
        }
    }
    bound
}

pub fn bind_filter(
    property: &str,
    filter: InputFilter,
    owner: &FilterOwner<'_>,
    model: &(impl query_data_model::OrbitQueryModel + ?Sized),
) -> BoundFilter {
    let (metadata, in_sort_key) = match owner {
        FilterOwner::Entity(entity) => {
            let table = model.query_backend().entity_table(*entity);
            (
                model
                    .property_for_entity_id(*entity, property)
                    .map(|property| (Some(property.id), Some(property.data_type))),
                table.is_some_and(|table| model.in_sort_key(table, property)),
            )
        }
        FilterOwner::Tables(tables) => {
            let data_type = tables.first().and_then(|first| {
                let data_type = model.table_column_type(first, property)?;
                tables[1..]
                    .iter()
                    .all(|table| model.table_column_type(table, property) == Some(data_type))
                    .then_some(data_type)
            });
            (
                Some((None, data_type)),
                tables
                    .iter()
                    .all(|table| model.in_sort_key(table, property)),
            )
        }
    };
    let (property_id, data_type) = metadata.unwrap_or_default();
    let column = property_id
        .and_then(|property| model.property_column(property))
        .unwrap_or(property);
    BoundFilter {
        filter,
        property: property_id,
        data_type,
        selectivity: property_id
            .map(|property| model.property_selectivity(property))
            .or_else(|| data_type.map(ontology::FieldSelectivity::from_data_type))
            .unwrap_or_default(),
        namespace_anchor: model.anchor_foreign_keys().contains_key(column),
        in_sort_key,
        text_indexed: property_id.is_some_and(|property| model.has_text_index(property)),
    }
}

pub fn requested_columns(columns: &Option<ColumnSelection>) -> Vec<String> {
    match columns {
        Some(ColumnSelection::List(cols)) => cols.clone(),
        Some(ColumnSelection::All) => vec!["*".to_string()],
        None => vec![],
    }
}

pub fn rel_kind_filter_values(types: &[String]) -> Option<Vec<String>> {
    (!crate::passes::normalize::is_wildcard(types)).then(|| types.to_vec())
}

fn tag_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        value => Some(value.to_string()),
    }
}

pub fn denorm_tag_values(key: &str, filter: &InputFilter) -> Option<Vec<String>> {
    if filter.rhs_column.is_some() {
        return None;
    }
    match filter.op {
        None | Some(FilterOp::Eq) => Some(vec![format!(
            "{key}:{}",
            filter
                .value
                .as_ref()
                .and_then(tag_value)
                .unwrap_or_default()
        )]),
        Some(FilterOp::In) => {
            let values = filter.value.as_ref()?.as_array()?;
            let tags: Vec<_> = values
                .iter()
                .filter_map(|value| tag_value(value).map(|value| format!("{key}:{value}")))
                .collect();
            (!tags.is_empty()).then_some(tags)
        }
        _ => None,
    }
}

pub fn lowerable_denorm<'a>(
    filter: &BoundFilter,
    direction: DenormalizedDirection,
    denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<&'a DenormalizedProperty> {
    denormalized
        .get(&DenormalizedKey {
            property: filter.property?,
            direction,
        })
        .filter(|facts| denorm_tag_values(&facts.tag_key, &filter.filter).is_some())
}

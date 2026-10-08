use std::collections::HashMap;

use super::{BoundFilter, DenormalizedDirection, DenormalizedKey, DenormalizedProperty};
use crate::input::{ColumnSelection, FilterOp, InputFilter};
use query_data_model::QueryBackendCatalog;

pub enum FilterOwner<'a> {
    Entity(query_data_model::EntityId),
    Table(&'a str),
}

pub fn ordered_filters(
    filters: &HashMap<String, Vec<InputFilter>>,
    owner: FilterOwner<'_>,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> Vec<(String, BoundFilter)> {
    let mut properties: Vec<_> = filters.iter().collect();
    properties.sort_unstable_by_key(|(property, _)| *property);
    properties
        .into_iter()
        .flat_map(|(property, filters)| {
            filters.iter().map(|filter| {
                (
                    property.clone(),
                    bind_filter(property, filter.clone(), &owner, model),
                )
            })
        })
        .collect()
}

pub fn bind_filter(
    property: &str,
    filter: InputFilter,
    owner: &FilterOwner<'_>,
    model: &(impl query_data_model::QueryDataModel + ?Sized),
) -> BoundFilter {
    let (metadata, table) = match owner {
        FilterOwner::Entity(entity) => (
            model
                .property_for_entity_id(*entity, property)
                .map(|property| (Some(property.id), Some(property.data_type))),
            model.query_backend().entity_table(*entity),
        ),
        FilterOwner::Table(table) => (
            Some((None, model.table_column_type(table, property))),
            Some(*table),
        ),
    };
    let (property_id, data_type) = metadata.unwrap_or_default();
    BoundFilter {
        filter,
        property: property_id,
        data_type,
        selectivity: property_id
            .map(|property| model.property_selectivity(property))
            .unwrap_or_default(),
        in_sort_key: table.is_some_and(|table| model.in_sort_key(table, property)),
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

pub fn has_non_denorm_filters(
    filters: &[(String, BoundFilter)],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> bool {
    filters.iter().any(|(_, filter)| {
        [DenormalizedDirection::Source, DenormalizedDirection::Target]
            .into_iter()
            .all(|direction| lowerable_denorm(filter, direction, denormalized).is_none())
    })
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

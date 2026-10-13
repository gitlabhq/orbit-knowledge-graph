use std::collections::HashMap;

use super::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty, PlannedFilter};
use crate::query::{FilterOp, Predicate};

pub fn rel_kind_filter_values(types: &[String]) -> Option<Vec<String>> {
    (!crate::query::is_wildcard(types)).then(|| types.to_vec())
}

fn tag_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(value) => Some(value.clone()),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
        value => Some(value.to_string()),
    }
}

pub fn denorm_tag_values(key: &str, filter: &Predicate) -> Option<Vec<String>> {
    match filter.op {
        FilterOp::Eq => Some(vec![format!(
            "{key}:{}",
            filter
                .value
                .as_ref()
                .and_then(tag_value)
                .unwrap_or_default()
        )]),
        FilterOp::In => {
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
    filters: &[(String, PlannedFilter)],
    denormalized: &HashMap<DenormalizedKey, DenormalizedProperty>,
) -> bool {
    filters.iter().any(|(_, filter)| {
        [DenormalizedDirection::Source, DenormalizedDirection::Target]
            .into_iter()
            .all(|direction| lowerable_denorm(filter, direction, denormalized).is_none())
    })
}

pub fn lowerable_denorm<'a>(
    filter: &PlannedFilter,
    direction: DenormalizedDirection,
    denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<&'a DenormalizedProperty> {
    denormalized
        .get(&DenormalizedKey {
            property: filter.property?,
            direction,
        })
        .filter(|facts| denorm_tag_values(&facts.tag_key, &filter.predicate).is_some())
}

//! Binds an input filter to the backend facts about its property (sort key, text index, selectivity,
//! uniqueness, mutability). Those facts decide where the planner may apply the filter.

use std::collections::HashMap;

use query_data_model::{OrbitQueryModel, RelationalMapping};

use super::predicates::{Column, Predicate};
use super::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};
use crate::input::{FilterOp, Input, InputFilter, PredicateTarget};

#[derive(Clone, Copy, PartialEq)]
pub struct NullAsEpoch {
    pub column: bool,
    pub rhs: bool,
}

#[derive(Clone)]
pub struct BoundFilter {
    pub filter: InputFilter,
    pub property: Option<query_data_model::PropertyId>,
    pub data_type: Option<ontology::DataType>,
    pub selectivity: ontology::FieldSelectivity,
    pub namespace_anchor: bool,
    pub foreign_key: bool,
    pub in_sort_key: bool,
    pub text_indexed: bool,
    pub unique: bool,
    pub immutable: bool,
    pub null_as_epoch: NullAsEpoch,
}

impl BoundFilter {
    pub fn is_selective(&self) -> bool {
        if self.namespace_anchor || self.filter.rhs_column.is_some() {
            return false;
        }
        let high = self.selectivity == ontology::FieldSelectivity::High;
        match self.filter.op {
            Some(op) if op.is_text() => self.text_indexed,
            Some(FilterOp::Ne | FilterOp::IsNotNull) => false,
            None | Some(FilterOp::Eq | FilterOp::In) => high || self.text_indexed,
            Some(_) => high,
        }
    }

    pub fn at(&self, column: Column) -> Predicate {
        Predicate::Property {
            column,
            filter: self.filter.clone(),
            data_type: self.data_type,
            selective: self.is_selective(),
            in_sort_key: self.in_sort_key,
            text_indexed: self.text_indexed,
            unique: self.unique,
            immutable: self.immutable,
            foreign_key: self.foreign_key,
            null_as_epoch: self.null_as_epoch,
        }
    }
}

pub enum FilterOwner<'a> {
    Entity(query_data_model::EntityId),
    Tables(&'a [String]),
}

pub fn ordered_filters(
    filters: &HashMap<String, Vec<InputFilter>>,
    owner: FilterOwner<'_>,
    model: &(impl OrbitQueryModel + ?Sized),
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
    model: &(impl OrbitQueryModel + ?Sized),
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
        foreign_key: property_id
            .is_some_and(|property| model.query_backend().property_is_foreign_key(property)),
        in_sort_key,
        text_indexed: property_id.is_some_and(|property| model.has_text_index(property)),
        unique: property_id.is_some_and(|property| model.property_unique(property)),
        immutable: property_id.is_some_and(|property| !model.property_mutable(property)),
        null_as_epoch: NullAsEpoch {
            column: property_id.is_some_and(|property| model.null_as_epoch(property)),
            rhs: false,
        },
    }
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

pub fn denormalized_facts(
    input: &Input,
    model: &(impl OrbitQueryModel + ?Sized),
) -> HashMap<DenormalizedKey, DenormalizedProperty> {
    input
        .nodes
        .iter()
        .filter_map(|node| Some((model.graph().entity_id(node.entity.as_deref()?)?, node)))
        .flat_map(|(entity, node)| {
            [DenormalizedDirection::Source, DenormalizedDirection::Target]
                .into_iter()
                .flat_map(move |direction| {
                    node.filters
                        .keys()
                        .map(String::as_str)
                        .chain(
                            input
                                .conditions()
                                .filter(|condition| {
                                    condition.target == PredicateTarget::Node(node.id.clone())
                                })
                                .map(|condition| condition.property.as_str()),
                        )
                        .filter_map(move |property| {
                            let property = model.graph().property_id(entity, property)?;
                            let key = DenormalizedKey {
                                property,
                                direction,
                            };
                            model
                                .denormalized()
                                .property(key)
                                .cloned()
                                .map(|facts| (key, facts))
                        })
                })
        })
        .collect()
}

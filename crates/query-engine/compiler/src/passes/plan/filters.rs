//! Binds a filter leaf to the backend facts about its property (sort key, text index, selectivity,
//! uniqueness, mutability). Those facts decide where the planner may apply the filter.

use crate::passes::codegen::SqlBackend;
use std::collections::HashMap;

use crate::tp_bounds::anchor_keys;
use query_data_model::Catalog;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::plan::logical::{
    Comparison, Expr as Logical, Literal, Operand, Predicate as Leaf,
};
use serde_json::Value as Json;

use super::predicates::{Column, Predicate};
use super::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};
use crate::logical::Query;
use crate::logical::anchors::Anchor;

pub type NullAsEpochColumns = HashMap<String, query_data_model::DataType>;

/// The type of each compared column that stores null as the epoch, so the epoch literal matches it.
#[derive(Clone, Copy, PartialEq)]
pub struct NullAsEpoch {
    pub column: Option<query_data_model::DataType>,
    pub rhs: Option<query_data_model::DataType>,
}

/// A filter leaf bound to the storage facts of the column it tests. The leaf is positive: a NOT
/// either folded into it or stays in the tree above it.
#[derive(Clone, PartialEq)]
pub struct BoundFilter {
    pub leaf: Leaf,
    /// The column a comparison across columns reads on its right.
    pub rhs: Option<Column>,
    pub property: Option<query_data_model::PropertyId>,
    pub data_type: Option<query_data_model::DataType>,
    pub selectivity: query_data_model::FieldSelectivity,
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
        if self.namespace_anchor {
            return false;
        }
        let high = self.selectivity == query_data_model::FieldSelectivity::High;
        match &self.leaf {
            Leaf::Compare {
                right: Operand::Expr(_),
                ..
            }
            | Leaf::Compare {
                comparison: Comparison::Ne,
                ..
            }
            | Leaf::Null { null: false, .. } => false,
            Leaf::Text { .. } => self.text_indexed,
            Leaf::Compare {
                comparison: Comparison::Eq,
                ..
            }
            | Leaf::In { .. } => high || self.text_indexed,
            Leaf::Compare { .. } | Leaf::Null { .. } => high,
            Leaf::And(_) | Leaf::Not(_) => false,
        }
    }

    pub fn matches_values(&self) -> bool {
        matches!(
            self.leaf,
            Leaf::Compare {
                comparison: Comparison::Eq,
                right: Operand::Literal(_),
                ..
            } | Leaf::In { .. }
        )
    }

    pub fn tags(&self, key: &str) -> Option<Vec<String>> {
        let tag = |literal: &Literal| match Json::from(literal) {
            Json::String(text) => format!("{key}:{text}"),
            value => format!("{key}:{value}"),
        };
        let values = self.leaf.literals();
        (self.matches_values() && !values.is_empty()).then(|| values.iter().map(tag).collect())
    }

    pub fn at(&self, column: Column) -> Predicate {
        Predicate::Filter {
            column,
            filter: self.clone(),
        }
    }
}

pub enum FilterOwner<'a> {
    Entity(query_data_model::EntityId),
    Tables(&'a [String]),
}

/// A sort-key match already narrows the read, so no filter in the set also asks for the text index.
pub fn unindex_if_key_narrowed(filters: &mut [(String, BoundFilter)]) {
    if filters
        .iter()
        .any(|(_, filter)| filter.in_sort_key && filter.matches_values())
    {
        for (_, filter) in filters {
            filter.text_indexed = false;
        }
    }
}

pub fn bind_filter(
    property: &str,
    leaf: Leaf,
    rhs: Option<Column>,
    owner: &FilterOwner<'_>,
    model: &Catalog<impl SqlBackend>,
) -> BoundFilter {
    let graph = model.graph();
    let (metadata, in_sort_key) = match owner {
        FilterOwner::Entity(entity) => {
            let table = model.table_of(*entity);
            (
                graph
                    .property_id(*entity, property)
                    .map(|property| graph.property(property))
                    .map(|property| (Some(property.id), Some(property.data_type))),
                table.is_some_and(|table| in_sort_key(model, table, property)),
            )
        }
        FilterOwner::Tables(tables) => {
            let data_type = tables.first().and_then(|first| {
                let data_type = column_type(model, first, property)?;
                tables[1..]
                    .iter()
                    .all(|table| column_type(model, table, property) == Some(data_type))
                    .then_some(data_type)
            });
            (
                Some((None, data_type)),
                tables
                    .iter()
                    .all(|table| in_sort_key(model, table, property)),
            )
        }
    };
    let (property_id, data_type) = metadata.unwrap_or_default();
    BoundFilter {
        leaf,
        rhs,
        property: property_id,
        data_type,
        selectivity: property_id
            .map(|property| graph.property(property).selectivity)
            .or_else(|| data_type.map(query_data_model::FieldSelectivity::from_data_type))
            .unwrap_or_default(),
        namespace_anchor: property_id
            .is_some_and(|property| anchor_keys(model).iter().any(|(key, _)| *key == property)),
        foreign_key: property_id.is_some_and(|property| model.model().holds_foreign_key(property)),
        in_sort_key,
        text_indexed: property_id.is_some_and(|property| model.text_index(property)),
        unique: property_id.is_some_and(|property| graph.property(property).unique),
        immutable: property_id.is_some_and(|property| model.model().immutable(property)),
        null_as_epoch: NullAsEpoch {
            column: data_type
                .filter(|_| property_id.is_some_and(|property| model.null_as_epoch(property))),
            rhs: None,
        },
    }
}

fn in_sort_key<B: RelationalBackend>(catalog: &Catalog<B>, table: &str, column: &str) -> bool {
    catalog
        .physical_table(table)
        .is_some_and(|table| table.sort_key.iter().any(|key| key == column))
}

fn column_type<B: RelationalBackend>(
    catalog: &Catalog<B>,
    table: &str,
    column: &str,
) -> Option<query_data_model::DataType> {
    catalog.physical_table(table)?.column(column)?.data_type
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
        .filter(|facts| filter.tags(&facts.tag_key).is_some())
}

/// The denormalized facts of every property a filter or condition tests on a binding.
pub fn denormalized_facts(
    query: Query<'_>,
    model: &Catalog<impl SqlBackend>,
) -> HashMap<DenormalizedKey, DenormalizedProperty> {
    query
        .leaves()
        .into_iter()
        .filter_map(|leaf| match leaf.left() {
            Logical::Property { property, .. } => Some(property),
            _ => None,
        })
        .flat_map(|property| {
            [DenormalizedDirection::Source, DenormalizedDirection::Target]
                .into_iter()
                .filter_map(move |direction| {
                    let key = DenormalizedKey {
                        property,
                        direction,
                    };
                    model.denormalized(key).cloned().map(|facts| (key, facts))
                })
        })
        .collect()
}

/// The anchor's attached filters, each bound to its property on the binding's entity.
pub fn bind_facts<B: SqlBackend>(
    query: Query<'_>,
    anchor: &Anchor<'_>,
    alias: &str,
    model: &Catalog<B>,
) -> Vec<(String, BoundFilter)> {
    let graph = query.model.graph();
    let owner = FilterOwner::Entity(anchor.binding.entity);
    anchor
        .filters
        .iter()
        .filter_map(|fact| {
            let Logical::Property { property, .. } = fact.left() else {
                return None;
            };
            let name = &graph.property(property).name;
            let rhs = match fact.right() {
                Some(Logical::Property { property, .. }) => Some(property),
                Some(_) => return None,
                None => None,
            };
            let column = rhs.map(|rhs| Column::new(alias, &graph.property(rhs).name));
            let mut bound = bind_filter(name, (*fact.predicate).clone(), column, &owner, model);
            bound.null_as_epoch.rhs = rhs
                .filter(|rhs| model.null_as_epoch(*rhs))
                .map(|rhs| graph.property(rhs).data_type);
            Some((name.clone(), bound))
        })
        .collect()
}

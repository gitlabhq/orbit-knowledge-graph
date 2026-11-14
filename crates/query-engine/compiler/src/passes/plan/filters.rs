//! Binds a filter leaf to the backend facts about its property (sort key, text index, selectivity,
//! uniqueness, mutability). Those facts decide where the planner may apply the filter.

use crate::passes::codegen::SqlBackend;
use std::collections::HashMap;

use crate::tp_bounds::anchor_keys;
use query_data_model::implementations::relational::{ColumnId, TableId, Target};
use query_data_model::plan::logical::{
    Comparison, Expr as Logical, Literal, Operand, Predicate as Leaf,
};
use query_data_model::{Catalog, EntityId, PropertyId};
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
    pub target: Target,
    /// What a comparison across columns reads on its right.
    pub rhs: Option<Target>,
    pub data_type: Option<query_data_model::DataType>,
    pub selectivity: query_data_model::FieldSelectivity,
    pub namespace_anchor: bool,
    pub foreign_key: bool,
    pub in_sort_key: bool,
    pub text_indexed: bool,
    pub unique: bool,
    pub immutable: bool,
    pub null_as_epoch: NullAsEpoch,
    /// Every stored column and type a tuple IN reads, in tuple order; empty for any other leaf.
    pub tuple: Vec<(ColumnId, query_data_model::DataType)>,
}

impl BoundFilter {
    pub fn property(&self) -> Option<PropertyId> {
        match self.target {
            Target::Property(property) => Some(property),
            Target::Attribute(_) | Target::Edge(_) => None,
        }
    }

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
            Leaf::InTuple { .. } => high,
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

    pub fn at(&self, column: Column, rhs: Option<Column>) -> Predicate {
        Predicate::Filter {
            column,
            rhs,
            filter: self.clone(),
        }
    }
}

pub enum FilterOwner<'a> {
    Entity(EntityId),
    Tables(&'a [TableId]),
}

/// A sort-key match already narrows the read, so no filter in the set also asks for the text index.
pub fn unindex_if_key_narrowed(filters: &mut [(Target, BoundFilter)]) {
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
    target: Target,
    leaf: Leaf,
    rhs: Option<Target>,
    owner: &FilterOwner<'_>,
    model: &Catalog<impl SqlBackend>,
) -> BoundFilter {
    let graph = model.graph();
    let columns = |tables: &[TableId]| -> Vec<Option<ColumnId>> {
        tables
            .iter()
            .map(|table| model.column_of(target, *table))
            .collect()
    };
    let (property, data_type, columns) = match owner {
        FilterOwner::Entity(entity) => {
            let property = match target {
                Target::Property(property) if graph.property(property).entity == *entity => {
                    Some(property)
                }
                _ => None,
            };
            let table = model.entity_table(*entity);
            (
                property,
                property.map(|property| graph.property(property).data_type),
                columns(table.as_slice()),
            )
        }
        FilterOwner::Tables(tables) => {
            let columns = columns(tables);
            let mut types = columns
                .iter()
                .map(|column| column.and_then(|column| model.column_type(column)));
            let first = types.next().flatten();
            let data_type = first.filter(|_| types.all(|other| other == first));
            (None, data_type, columns)
        }
    };
    let stored = |fact: fn(&Catalog<_>, ColumnId) -> bool| {
        columns
            .iter()
            .all(|column| column.is_some_and(|column| fact(model, column)))
    };
    let in_sort_key = match owner {
        FilterOwner::Entity(_) => !columns.is_empty(),
        FilterOwner::Tables(_) => true,
    } && stored(Catalog::in_sort_key);
    let tuple = match &leaf {
        Leaf::InTuple { left, .. } => left
            .iter()
            .filter_map(|expr| match expr {
                Logical::Property { property, .. } => {
                    let table = model.entity_table(graph.property(*property).entity)?;
                    let column = model.column_of(Target::Property(*property), table)?;
                    Some((column, graph.property(*property).data_type))
                }
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    let epoch = |target: Option<Target>| match target {
        Some(Target::Property(property)) if model.null_as_epoch(property) => {
            Some(graph.property(property).data_type)
        }
        _ => None,
    };
    BoundFilter {
        leaf,
        target,
        rhs,
        data_type,
        selectivity: property
            .map(|property| {
                model.selectivity(property).unwrap_or_else(|| {
                    query_data_model::FieldSelectivity::from_data_type(
                        graph.property(property).data_type,
                    )
                })
            })
            .or_else(|| data_type.map(query_data_model::FieldSelectivity::from_data_type))
            .unwrap_or_default(),
        namespace_anchor: property
            .is_some_and(|property| anchor_keys(model).iter().any(|(key, _)| *key == property)),
        foreign_key: property.is_some_and(|property| model.model().holds_foreign_key(property)),
        in_sort_key,
        text_indexed: property.is_some() && stored(Catalog::text_indexed),
        unique: property.is_some_and(|property| graph.property(property).unique),
        immutable: property.is_some_and(|property| model.model().immutable(property)),
        null_as_epoch: NullAsEpoch {
            column: epoch(property.map(Target::Property)),
            rhs: epoch(rhs),
        },
        tuple,
    }
}

pub fn lowerable_denorm<'a>(
    filter: &BoundFilter,
    direction: DenormalizedDirection,
    denormalized: &'a HashMap<DenormalizedKey, DenormalizedProperty>,
) -> Option<&'a DenormalizedProperty> {
    denormalized
        .get(&DenormalizedKey {
            property: filter.property()?,
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
    anchor: &Anchor<'_>,
    model: &Catalog<B>,
) -> Vec<(Target, BoundFilter)> {
    let owner = FilterOwner::Entity(anchor.binding.entity);
    anchor
        .filters
        .iter()
        .filter_map(|fact| {
            let Logical::Property { property, .. } = fact.left() else {
                return None;
            };
            let rhs = match fact.right() {
                Some(Logical::Property { property, .. }) => Some(Target::Property(property)),
                Some(_) => return None,
                None => None,
            };
            let target = Target::Property(property);
            let bound = bind_filter(target, (*fact.predicate).clone(), rhs, &owner, model);
            Some((target, bound))
        })
        .collect()
}

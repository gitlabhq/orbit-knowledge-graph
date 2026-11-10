//! Binds a filter test to the backend facts about its property (sort key, text index, selectivity,
//! uniqueness, mutability). Those facts decide where the planner may apply the filter.

use crate::passes::codegen::RelationalModel;
use std::collections::HashMap;

use crate::tp_bounds::anchor_keys;
use query_data_model::Catalog;
use query_data_model::implementations::relational::RelationalBackend;
use query_data_model::plan::logical::{
    Comparison, Expr as Logical, Literal, Matching, Operand, Predicate as Leaf,
};
use serde_json::Value as Json;

use super::predicates::{Column, Predicate};
use super::{DenormalizedDirection, DenormalizedKey, DenormalizedProperty};
use crate::logical::Query;
use crate::logical::facts::Anchor;

pub type NullAsEpochColumns = HashMap<String, query_data_model::DataType>;

/// The type of each compared column that stores null as the epoch, so the epoch literal matches it.
#[derive(Clone, Copy, PartialEq)]
pub struct NullAsEpoch {
    pub column: Option<query_data_model::DataType>,
    pub rhs: Option<query_data_model::DataType>,
}

/// What a filter tests on its column, with any complement already folded into the operator.
#[derive(Clone, PartialEq)]
pub enum Test {
    Compare(Comparison, Literal),
    Column(Comparison, Column),
    In(Vec<Literal>),
    Null(bool),
    Text(Matching, String),
}

impl Test {
    /// The test of a logical leaf; `complemented` folds a negation into `=`, `<>`, and null checks.
    pub fn of(
        leaf: &Leaf,
        complemented: bool,
        rhs: impl FnOnce(Logical) -> Option<Column>,
    ) -> Option<Self> {
        let flip = |comparison: Comparison| match (comparison, complemented) {
            (Comparison::Eq, true) => Comparison::Ne,
            (Comparison::Ne, true) => Comparison::Eq,
            (comparison, _) => comparison,
        };
        Some(match leaf {
            Leaf::Compare {
                comparison,
                right: Operand::Literal(literal),
                ..
            } => Self::Compare(flip(*comparison), literal.clone()),
            Leaf::Compare {
                comparison,
                right: Operand::Expr(right),
                ..
            } => Self::Column(flip(*comparison), rhs(*right)?),
            Leaf::In { values, .. } => Self::In(values.clone()),
            Leaf::Null { null, .. } => Self::Null(*null != complemented),
            Leaf::Text {
                matching, needle, ..
            } => Self::Text(*matching, needle.clone()),
            Leaf::And(_) | Leaf::Not(_) => return None,
        })
    }

    pub fn rhs(&self) -> Option<&Column> {
        match self {
            Self::Column(_, rhs) => Some(rhs),
            _ => None,
        }
    }

    pub fn rhs_mut(&mut self) -> Option<&mut Column> {
        match self {
            Self::Column(_, rhs) => Some(rhs),
            _ => None,
        }
    }

    pub fn matches_values(&self) -> bool {
        matches!(self, Self::Compare(Comparison::Eq, _) | Self::In(_))
    }

    pub fn literals(&self) -> &[Literal] {
        match self {
            Self::Compare(_, literal) => std::slice::from_ref(literal),
            Self::In(values) => values,
            _ => &[],
        }
    }

    pub fn tags(&self, key: &str) -> Option<Vec<String>> {
        let tag = |literal: &Literal| match json(literal) {
            Json::String(text) => format!("{key}:{text}"),
            value => format!("{key}:{value}"),
        };
        match self {
            Self::Compare(Comparison::Eq, literal) => Some(vec![tag(literal)]),
            Self::In(values) if !values.is_empty() => Some(values.iter().map(tag).collect()),
            _ => None,
        }
    }
}

pub fn json(literal: &Literal) -> Json {
    match literal {
        Literal::String(text) | Literal::Temporal(text) => Json::String(text.clone()),
        Literal::Int(number) => Json::from(*number),
        Literal::Float(number) => Json::from(*number),
        Literal::Bool(flag) => Json::Bool(*flag),
    }
}

#[derive(Clone)]
pub struct BoundFilter {
    pub test: Test,
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
        match self.test {
            Test::Column(..) | Test::Compare(Comparison::Ne, _) | Test::Null(false) => false,
            Test::Text(..) => self.text_indexed,
            Test::Compare(Comparison::Eq, _) | Test::In(_) => high || self.text_indexed,
            Test::Compare(..) | Test::Null(true) => high,
        }
    }

    pub fn at(&self, column: Column) -> Predicate {
        Predicate::Property {
            column,
            test: self.test.clone(),
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

/// A sort-key match already narrows the read, so no filter in the set also asks for the text index.
pub fn unindex_if_key_narrowed(filters: &mut [(String, BoundFilter)]) {
    if filters
        .iter()
        .any(|(_, filter)| filter.in_sort_key && filter.test.matches_values())
    {
        for (_, filter) in filters {
            filter.text_indexed = false;
        }
    }
}

pub fn bind_filter(
    property: &str,
    test: Test,
    owner: &FilterOwner<'_>,
    model: &impl RelationalModel,
) -> BoundFilter {
    let catalog = model.catalog();
    let graph = catalog.graph();
    let (metadata, in_sort_key) = match owner {
        FilterOwner::Entity(entity) => {
            let table = catalog.table_of(*entity);
            (
                graph
                    .property_id(*entity, property)
                    .map(|property| graph.property(property))
                    .map(|property| (Some(property.id), Some(property.data_type))),
                table.is_some_and(|table| in_sort_key(catalog, table, property)),
            )
        }
        FilterOwner::Tables(tables) => {
            let data_type = tables.first().and_then(|first| {
                let data_type = column_type(catalog, first, property)?;
                tables[1..]
                    .iter()
                    .all(|table| column_type(catalog, table, property) == Some(data_type))
                    .then_some(data_type)
            });
            (
                Some((None, data_type)),
                tables
                    .iter()
                    .all(|table| in_sort_key(catalog, table, property)),
            )
        }
    };
    let (property_id, data_type) = metadata.unwrap_or_default();
    let column = property_id
        .and_then(|property| catalog.column(property))
        .unwrap_or(property);
    BoundFilter {
        test,
        property: property_id,
        data_type,
        selectivity: property_id
            .map(|property| graph.property(property).selectivity)
            .or_else(|| data_type.map(query_data_model::FieldSelectivity::from_data_type))
            .unwrap_or_default(),
        namespace_anchor: anchor_keys(model).iter().any(|(name, _)| name == column),
        foreign_key: property_id
            .is_some_and(|property| !graph.property(property).references.is_empty()),
        in_sort_key,
        text_indexed: property_id.is_some_and(|property| catalog.text_index(property)),
        unique: property_id.is_some_and(|property| graph.property(property).unique),
        immutable: property_id.is_some_and(|property| !graph.property(property).mutable),
        null_as_epoch: NullAsEpoch {
            column: data_type
                .filter(|_| property_id.is_some_and(|property| catalog.null_as_epoch(property))),
            rhs: None,
        },
    }
}

fn in_sort_key<B: RelationalBackend>(catalog: &Catalog<B>, table: &str, column: &str) -> bool {
    catalog
        .table_facts(table)
        .is_some_and(|facts| facts.sort_key.iter().any(|key| key == column))
}

fn column_type<B: RelationalBackend>(
    catalog: &Catalog<B>,
    table: &str,
    column: &str,
) -> Option<query_data_model::DataType> {
    catalog
        .table_facts(table)?
        .column_types
        .get(column)
        .copied()
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
        .filter(|facts| filter.test.tags(&facts.tag_key).is_some())
}

/// The denormalized facts of every property a filter or condition tests on a binding.
pub fn denormalized_facts(
    query: Query<'_>,
    model: &impl RelationalModel,
) -> HashMap<DenormalizedKey, DenormalizedProperty> {
    query
        .leaves()
        .into_iter()
        .filter_map(|leaf| match leaf.left {
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
                    model
                        .catalog()
                        .denormalized(key)
                        .cloned()
                        .map(|facts| (key, facts))
                })
        })
        .collect()
}

/// The anchor's attached filters, each bound to its property on the binding's entity.
pub fn bind_facts<M: RelationalModel>(
    query: Query<'_>,
    anchor: &Anchor<'_>,
    alias: &str,
    model: &M,
) -> Vec<(String, BoundFilter)> {
    let graph = query.model.graph();
    let owner = FilterOwner::Entity(anchor.binding.entity);
    anchor
        .filters
        .iter()
        .filter_map(|fact| {
            let Logical::Property { property, .. } = fact.left else {
                return None;
            };
            let name = &graph.property(property).name;
            let mut rhs = None;
            let test = Test::of(fact.leaf, fact.complemented, |right| {
                let Logical::Property { property, .. } = right else {
                    return None;
                };
                rhs = Some(property);
                Some(Column::new(alias, &graph.property(property).name))
            })?;
            let mut bound = bind_filter(name, test, &owner, model);
            bound.null_as_epoch.rhs = rhs
                .filter(|rhs| model.catalog().null_as_epoch(*rhs))
                .map(|rhs| graph.property(rhs).data_type);
            Some((name.clone(), bound))
        })
        .collect()
}

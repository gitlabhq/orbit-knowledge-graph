use ontology::constants::*;

use super::{BoundFilter, NodePlan};
use crate::input::InputFilter;
use crate::passes::shared::{denorm_tag_values, requested_columns};

#[derive(Clone, PartialEq, Eq)]
pub struct Column {
    pub source: String,
    pub name: String,
}

impl Column {
    pub fn new(source: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            name: name.into(),
        }
    }
}

#[derive(Clone)]
pub enum Predicate {
    Property {
        column: Column,
        filter: InputFilter,
        data_type: Option<ontology::DataType>,
    },
    Ids {
        column: Column,
        values: Vec<i64>,
    },
    IdRange {
        column: Column,
        start: i64,
        end: i64,
    },
    Live {
        alias: String,
    },
    EntityKind {
        column: Column,
        entity: String,
    },
    RelationshipKinds {
        alias: String,
        kinds: Vec<String>,
    },
    Tags {
        column: Column,
        values: Vec<String>,
    },
    Membership {
        column: Column,
        definition: String,
        key: String,
    },
}

#[derive(Clone)]
pub enum OutputValue {
    Column(Column),
    Text(String),
    Depth(u32),
    Path(Vec<(Column, Column)>),
}

#[derive(Clone)]
pub struct Projection {
    pub value: OutputValue,
    pub name: String,
}

impl Projection {
    pub fn new(value: OutputValue, name: impl Into<String>) -> Self {
        Self {
            value,
            name: name.into(),
        }
    }
    pub fn col(alias: &str, column: &str) -> Self {
        Self::new(OutputValue::Column(Column::new(alias, column)), column)
    }
}

pub fn property_filter(alias: &str, property: &str, bound: &BoundFilter) -> Predicate {
    Predicate::Property {
        column: Column::new(alias, property),
        filter: bound.filter.clone(),
        data_type: bound.data_type,
    }
}

pub fn id_list(alias: &str, column: &str, ids: &[i64]) -> Predicate {
    Predicate::Ids {
        column: Column::new(alias, column),
        values: ids.to_vec(),
    }
}

pub fn live(alias: &str) -> Predicate {
    Predicate::Live {
        alias: alias.into(),
    }
}

pub fn relationship_kinds(alias: &str, kinds: &[String]) -> Option<Predicate> {
    (!crate::passes::normalize::is_wildcard(kinds) && !kinds.is_empty()).then(|| {
        Predicate::RelationshipKinds {
            alias: alias.into(),
            kinds: kinds.to_vec(),
        }
    })
}

pub fn tag_filter(alias: &str, column: &str, key: &str, filter: &InputFilter) -> Option<Predicate> {
    denorm_tag_values(key, filter).map(|values| Predicate::Tags {
        column: Column::new(alias, column),
        values,
    })
}

pub fn node_predicates(node: &NodePlan) -> Vec<Predicate> {
    let mut predicates: Vec<_> = node
        .filters
        .iter()
        .map(|(property, filter)| property_filter(&node.alias, property, filter))
        .collect();
    if !node.node_ids.is_empty() {
        predicates.push(id_list(&node.alias, DEFAULT_PRIMARY_KEY, &node.node_ids));
    }
    if let Some(range) = &node.id_range {
        predicates.push(Predicate::IdRange {
            column: Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
            start: range.start,
            end: range.end,
        });
    }
    predicates.push(live(&node.alias));
    predicates
}

pub fn node_outputs(node: &NodePlan) -> Vec<Projection> {
    if !node.emit_select {
        return vec![];
    }
    requested_columns(&node.columns)
        .into_iter()
        .map(|column| {
            Projection::new(
                OutputValue::Column(Column::new(&node.alias, &column)),
                format!("{}_{column}", node.alias),
            )
        })
        .collect()
}

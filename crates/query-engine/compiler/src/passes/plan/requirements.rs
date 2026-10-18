use ontology::constants::*;

use super::helpers::{denorm_tag_values, requested_columns};
use super::physical::RelationId;
use super::{BoundFilter, NodePlan};
use crate::input::InputFilter;

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
    Boolean(crate::input::BooleanExpression<Box<Predicate>>),
    ColumnsEqual {
        left: Column,
        right: Column,
    },
    Property {
        column: Column,
        filter: InputFilter,
        data_type: Option<ontology::DataType>,
        selective: bool,
        in_sort_key: bool,
        text_indexed: bool,
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
    ColumnEq {
        left: Column,
        right: Column,
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
        columns: Vec<Column>,
        relation: RelationId,
        keys: Vec<String>,
    },
    Scope {
        alias: String,
        proof: crate::scope::ScopeProof,
    },
}

impl Predicate {
    pub fn is_selective(&self) -> bool {
        match self {
            Self::Boolean(expression) => expression.fold(
                &|predicate| predicate.is_selective(),
                &|predicates| predicates.into_iter().any(|selective| selective),
                &|_| false,
            ),
            Self::Property { selective, .. } => *selective,
            Self::Ids { .. } | Self::Membership { .. } => true,
            Self::IdRange { start, end, .. } => {
                end.saturating_sub(*start) <= crate::passes::validate::MAX_ID_RANGE_SPAN
            }
            _ => false,
        }
    }
}

#[derive(Clone)]
pub enum OutputValue {
    Column(Column),
    Text(String),
    Null,
    EmptyTextArray,
    Depth(u32),
    Path(Vec<(Column, Column)>),
    PathAppend {
        path: Column,
        id: Column,
        kind: Column,
    },
    PathPrepend {
        path: Column,
        id: Column,
        kind: Column,
    },
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
    bound.at(Column::new(alias, property))
}

pub fn id_list(alias: &str, column: &str, ids: &[i64]) -> Predicate {
    Predicate::Ids {
        column: Column::new(alias, column),
        values: ids.to_vec(),
    }
}

pub fn membership(column: Column, (relation, key): (RelationId, String)) -> Predicate {
    Predicate::Membership {
        columns: vec![column],
        relation,
        keys: vec![key],
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
    predicates.extend(node.predicates.iter().cloned());
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
    node_outputs_at(node, &node.alias)
}

pub fn node_outputs_at(node: &NodePlan, source: &str) -> Vec<Projection> {
    if !node.emit_select {
        return vec![];
    }
    requested_columns(&node.columns)
        .into_iter()
        .map(|column| {
            Projection::new(
                OutputValue::Column(Column::new(source, &column)),
                format!("{}_{column}", node.alias),
            )
        })
        .collect()
}

//! Predicates and projections the planner attaches to relations, and the facts about a predicate the
//! planner asks for: selective, a point lookup, or filtering the sort key.

use ontology::constants::*;

use super::filters::BoundFilter;
use super::graph::RelationId;
use super::pattern::NodePlan;
use crate::input::{BooleanExpression, FilterOp, InputFilter};

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

#[derive(Clone, PartialEq)]
pub enum Predicate {
    Boolean(BooleanExpression<Box<Predicate>>),
    Property {
        column: Column,
        filter: InputFilter,
        data_type: Option<ontology::DataType>,
        selective: bool,
        in_sort_key: bool,
        text_indexed: bool,
        unique: bool,
        immutable: bool,
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

    pub fn is_point_lookup(&self) -> bool {
        match self {
            Self::Ids { values, .. } => values.len() == 1,
            Self::Property { unique, filter, .. } => {
                *unique
                    && matches!(filter.op, None | Some(FilterOp::Eq))
                    && !matches!(filter.value, Some(serde_json::Value::Array(_)))
            }
            _ => false,
        }
    }

    pub fn filters_sort_key(&self, sort_key: &[String]) -> bool {
        let in_key = |column: &str| sort_key.iter().any(|key| key == column);
        match self {
            Self::Boolean(expression) => expression.fold(
                &|predicate| predicate.filters_sort_key(sort_key),
                &|children| children.into_iter().all(|filters| filters),
                &|filters| filters,
            ),
            Self::Property {
                column,
                filter,
                immutable,
                ..
            } => filter.rhs_column.is_none() && (*immutable || in_key(&column.name)),
            Self::Ids { column, .. }
            | Self::IdRange { column, .. }
            | Self::EntityKind { column, .. } => in_key(&column.name),
            Self::Membership { columns, .. } => columns.iter().all(|column| in_key(&column.name)),
            Self::RelationshipKinds { .. } => in_key(RELATIONSHIP_KIND_COLUMN),
            Self::Scope { .. } => in_key(TRAVERSAL_PATH_COLUMN),
            _ => false,
        }
    }
}

#[derive(Clone)]
pub enum OutputValue {
    Column(Column),
    Text(String),
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

pub fn kind_column(id_column: &str) -> &'static str {
    if id_column == SOURCE_ID_COLUMN {
        SOURCE_KIND_COLUMN
    } else {
        TARGET_KIND_COLUMN
    }
}

pub fn property_filter(alias: &str, property: &str, bound: &BoundFilter) -> Predicate {
    bound.at(Column::new(alias, property))
}

pub fn id_predicates(node: &NodePlan, column: Column) -> Vec<Predicate> {
    let mut predicates = Vec::new();
    if !node.node_ids.is_empty() {
        predicates.push(Predicate::Ids {
            column: column.clone(),
            values: node.node_ids.clone(),
        });
    }
    if let Some(range) = &node.id_range {
        predicates.push(Predicate::IdRange {
            column,
            start: range.start,
            end: range.end,
        });
    }
    predicates
}

pub fn membership(column: Column, (relation, key): (RelationId, String)) -> Predicate {
    Predicate::Membership {
        columns: vec![column],
        relation,
        keys: vec![key],
    }
}

pub fn column_eq(left: Column, right: Column) -> Predicate {
    Predicate::ColumnEq { left, right }
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
    super::filters::denorm_tag_values(key, filter).map(|values| Predicate::Tags {
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
    predicates.extend(id_predicates(
        node,
        Column::new(&node.alias, DEFAULT_PRIMARY_KEY),
    ));
    predicates.push(live(&node.alias));
    predicates
}

pub fn node_outputs(node: &NodePlan) -> Vec<Projection> {
    if !node.emit_select {
        return vec![];
    }
    node.requested_columns()
        .into_iter()
        .map(|column| {
            Projection::new(
                OutputValue::Column(Column::new(&node.alias, &column)),
                format!("{}_{column}", node.alias),
            )
        })
        .collect()
}

//! Predicates and projections the planner attaches to relations, and the facts about a predicate the
//! planner asks for: selective, a point lookup, or filtering the sort key.

use ontology::constants::*;

use query_data_model::plan::logical::Comparison;

use super::filters::{BoundFilter, NullAsEpoch, NullAsEpochColumns, Test};
use super::rows::Rel;
use crate::input::BooleanExpression;

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
        test: Test,
        data_type: Option<ontology::DataType>,
        selective: bool,
        in_sort_key: bool,
        text_indexed: bool,
        unique: bool,
        immutable: bool,
        foreign_key: bool,
        null_as_epoch: NullAsEpoch,
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
        relation: Rel,
        keys: Vec<String>,
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
                end.saturating_sub(*start) <= crate::query_limits::MAX_ID_RANGE_SPAN
            }
            _ => false,
        }
    }

    /// A text match on a changing column finds rows through the text index but does not bound a keyed re-read, so that scan keeps FINAL.
    pub fn is_text_match(&self) -> bool {
        matches!(
            self,
            Self::Property {
                test: Test::Text(..),
                ..
            }
        )
    }

    pub fn narrows_keys(&self) -> bool {
        match self {
            Self::Ids { .. } | Self::IdRange { .. } | Self::Membership { .. } => true,
            Self::Property {
                in_sort_key,
                foreign_key,
                test,
                ..
            } => (*in_sort_key || *foreign_key) && test.matches_values(),
            _ => false,
        }
    }

    pub fn is_point_lookup(&self) -> bool {
        match self {
            Self::Ids { values, .. } => values.len() == 1,
            Self::Property { unique, test, .. } => {
                *unique
                    && matches!(
                        test,
                        Test::Compare(Comparison::Eq, _) | Test::Column(Comparison::Eq, _)
                    )
            }
            _ => false,
        }
    }

    pub fn leaves(&self) -> Vec<&Predicate> {
        match self {
            Self::Boolean(expression) => expression
                .leaves()
                .flat_map(|predicate| predicate.leaves())
                .collect(),
            predicate => vec![predicate],
        }
    }

    pub fn filters_sort_key(&self, sort_key: &[String], immutable_ok: bool) -> bool {
        let in_key = |column: &str| sort_key.iter().any(|key| key == column);
        match self {
            Self::Boolean(expression) => expression.fold(
                &|predicate| predicate.filters_sort_key(sort_key, immutable_ok),
                &|children| children.into_iter().all(|filters| filters),
                &|filters| filters,
            ),
            Self::Property {
                column,
                test,
                immutable,
                ..
            } => test.rhs().is_none() && (*immutable && immutable_ok || in_key(&column.name)),
            Self::Ids { column, .. }
            | Self::IdRange { column, .. }
            | Self::EntityKind { column, .. } => in_key(&column.name),
            Self::Membership { columns, .. } => columns.iter().all(|column| in_key(&column.name)),
            Self::RelationshipKinds { .. } => in_key(RELATIONSHIP_KIND_COLUMN),
            _ => false,
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum OutputValue {
    Column(Column),
    EpochAsNull(Column, ontology::DataType),
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

impl OutputValue {
    pub fn property(null_as_epoch: &NullAsEpochColumns, column: Column) -> Self {
        match null_as_epoch.get(&column.name) {
            Some(data_type) => Self::EpochAsNull(column, *data_type),
            None => Self::Column(column),
        }
    }

    pub fn column(&self) -> Option<&Column> {
        match self {
            Self::Column(column) | Self::EpochAsNull(column, _) => Some(column),
            _ => None,
        }
    }
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

pub fn membership(column: Column, (relation, key): (Rel, String)) -> Predicate {
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
    (!crate::input::is_wildcard(kinds) && !kinds.is_empty()).then(|| Predicate::RelationshipKinds {
        alias: alias.into(),
        kinds: kinds.to_vec(),
    })
}

pub fn tag_filter(alias: &str, column: &str, key: &str, test: &Test) -> Option<Predicate> {
    test.tags(key).map(|values| Predicate::Tags {
        column: Column::new(alias, column),
        values,
    })
}

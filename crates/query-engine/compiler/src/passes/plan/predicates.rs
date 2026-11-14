//! Predicates and projections the planner attaches to relations, and the facts about a predicate the
//! planner asks for: selective, a point lookup, or filtering the sort key.

use query_data_model::EdgeField;
use query_data_model::implementations::relational::Target;
use query_data_model::{GraphCatalog, PropertyId};

use query_data_model::plan::logical::{Comparison, Predicate as Leaf};

use super::filters::{BoundFilter, NullAsEpochColumns};
use super::rows::Rel;
use crate::logical::Boolean;

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
    Boolean(Boolean<Box<Predicate>>),
    Filter {
        column: Column,
        rhs: Option<Column>,
        filter: BoundFilter,
    },
    Ids {
        column: Column,
        values: Vec<orbit_authorization::Key>,
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
        entity: query_data_model::EntityId,
    },
    ColumnEq {
        left: Column,
        right: Column,
    },
    RelationshipKinds {
        column: Column,
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
    pub fn is_selective(&self, id_range_span: i64) -> bool {
        match self {
            Self::Boolean(expression) => expression.fold(
                &|predicate| predicate.is_selective(id_range_span),
                &|predicates| predicates.into_iter().any(|selective| selective),
                &|_| false,
            ),
            Self::Filter { filter, .. } => filter.is_selective(),
            Self::Ids { .. } | Self::Membership { .. } => true,
            Self::IdRange { start, end, .. } => end.saturating_sub(*start) <= id_range_span,
            _ => false,
        }
    }

    /// A text match on a changing column finds rows through the text index but does not bound a keyed re-read, so that scan keeps FINAL.
    pub fn is_text_match(&self) -> bool {
        matches!(
            self,
            Self::Filter {
                filter: BoundFilter {
                    leaf: Leaf::Text { .. },
                    ..
                },
                ..
            }
        )
    }

    pub fn narrows_keys(&self) -> bool {
        match self {
            Self::Ids { .. } | Self::IdRange { .. } | Self::Membership { .. } => true,
            Self::Filter { filter, .. } => {
                (filter.in_sort_key || filter.foreign_key) && filter.matches_values()
            }
            _ => false,
        }
    }

    pub fn is_point_lookup(&self) -> bool {
        match self {
            Self::Ids { values, .. } => values.len() == 1,
            Self::Filter { filter, .. } => {
                filter.unique
                    && matches!(
                        filter.leaf,
                        Leaf::Compare {
                            comparison: Comparison::Eq,
                            ..
                        }
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
            Self::Filter { column, filter, .. } => {
                filter.rhs.is_none() && (filter.immutable && immutable_ok || in_key(&column.name))
            }
            Self::Ids { column, .. }
            | Self::IdRange { column, .. }
            | Self::EntityKind { column, .. } => in_key(&column.name),
            Self::Membership { columns, .. } => columns.iter().all(|column| in_key(&column.name)),
            Self::RelationshipKinds { column, .. } => in_key(&column.name),
            _ => false,
        }
    }
}

#[derive(Clone, PartialEq)]
pub enum OutputValue {
    Column(Column),
    EpochAsNull(Column, query_data_model::DataType),
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

pub fn kind_field(id: EdgeField) -> EdgeField {
    match id {
        EdgeField::SourceId => EdgeField::SourceKind,
        _ => EdgeField::TargetKind,
    }
}

pub fn property_column(alias: &str, property: PropertyId, graph: &GraphCatalog) -> Column {
    Column::new(alias, &graph.property(property).name)
}

pub fn property_columns(
    alias: &str,
    bound: &BoundFilter,
    graph: &GraphCatalog,
) -> Option<(Column, Option<Column>)> {
    let property = bound.property()?;
    let rhs = match bound.rhs {
        Some(Target::Property(rhs)) => Some(property_column(alias, rhs, graph)),
        _ => None,
    };
    Some((property_column(alias, property, graph), rhs))
}

pub fn property_filter(
    alias: &str,
    bound: &BoundFilter,
    graph: &GraphCatalog,
) -> Option<Predicate> {
    let (column, rhs) = property_columns(alias, bound, graph)?;
    Some(bound.at(column, rhs))
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

pub fn relationship_kinds(column: Column, kinds: &[String]) -> Option<Predicate> {
    (!kinds.is_empty()).then(|| Predicate::RelationshipKinds {
        column,
        kinds: kinds.to_vec(),
    })
}

pub fn tag_filter(alias: &str, column: &str, key: &str, filter: &BoundFilter) -> Option<Predicate> {
    filter.tags(key).map(|values| Predicate::Tags {
        column: Column::new(alias, column),
        values,
    })
}

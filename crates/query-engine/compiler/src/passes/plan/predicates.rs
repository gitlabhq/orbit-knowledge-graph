//! Predicates and projections the planner attaches to relations, and the facts about a predicate the
//! planner asks for: selective, a point lookup, or filtering the sort key.

use ontology::constants::*;

use super::filters::BoundFilter;
use super::graph::RelationId;
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
    /// A tombstone keeps only its sort key, so a pre-pick filter on other columns must let it through.
    OrTombstone {
        predicates: Vec<Predicate>,
        alias: String,
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
            Self::OrTombstone { predicates, .. } => predicates.iter().any(Predicate::is_selective),
            Self::Ids { .. } | Self::Membership { .. } => true,
            Self::IdRange { start, end, .. } => {
                end.saturating_sub(*start) <= crate::passes::validate::MAX_ID_RANGE_SPAN
            }
            _ => false,
        }
    }

    /// A text match on a changing column finds rows through the text index but does not bound a keyed re-read, so that scan keeps FINAL.
    pub fn is_text_match(&self) -> bool {
        matches!(self, Self::Property { filter, .. } if filter.op.is_some_and(FilterOp::is_text))
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

    pub fn leaves(&self) -> Vec<&Predicate> {
        match self {
            Self::Boolean(expression) => expression
                .leaves()
                .flat_map(|predicate| predicate.leaves())
                .collect(),
            predicate => vec![predicate],
        }
    }

    pub fn filters_sort_key(&self, sort_key: &[String]) -> bool {
        match self {
            Self::Boolean(expression) => expression.fold(
                &|predicate| predicate.filters_sort_key(sort_key),
                &|children| children.into_iter().all(|filters| filters),
                &|filters| filters,
            ),
            Self::Property {
                filter,
                immutable: true,
                ..
            } => filter.rhs_column.is_none(),
            _ => self.on_sort_key(sort_key),
        }
    }

    pub fn on_sort_key(&self, sort_key: &[String]) -> bool {
        let in_key = |column: &str| sort_key.iter().any(|key| key == column);
        match self {
            Self::Boolean(expression) => expression.fold(
                &|predicate| predicate.on_sort_key(sort_key),
                &|children| children.into_iter().all(|on_key| on_key),
                &|on_key| on_key,
            ),
            Self::Property { column, filter, .. } => {
                filter.rhs_column.is_none() && in_key(&column.name)
            }
            Self::Ids { column, .. }
            | Self::IdRange { column, .. }
            | Self::EntityKind { column, .. } => in_key(&column.name),
            Self::Membership { columns, .. } => columns.iter().all(|column| in_key(&column.name)),
            Self::RelationshipKinds { .. } => in_key(RELATIONSHIP_KIND_COLUMN),
            Self::Scope { .. } => in_key(TRAVERSAL_PATH_COLUMN),
            _ => false,
        }
    }

    pub fn guard_tombstones(
        predicates: Vec<Predicate>,
        sort_key: &[String],
        alias: &str,
    ) -> Vec<Predicate> {
        let (mut keyed, guarded): (Vec<_>, Vec<_>) = predicates
            .into_iter()
            .partition(|predicate| predicate.on_sort_key(sort_key));
        if !guarded.is_empty() {
            keyed.push(Self::OrTombstone {
                predicates: guarded,
                alias: alias.to_string(),
            });
        }
        keyed
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

impl OutputValue {}

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

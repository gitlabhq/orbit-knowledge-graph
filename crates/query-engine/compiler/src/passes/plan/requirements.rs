use ontology::constants::*;

use super::context::PlanningContext;
use super::helpers::{denorm_tag_values, requested_columns};
use super::{BoundFilter, NodePlan};
use crate::error::Result;
use crate::input::InputFilter;
pub use query_data_model::bindings::ColumnRef as Column;
use query_data_model::{QueryDataModel, bindings::RelationId};

#[derive(Clone)]
pub enum Predicate {
    PathPrefixes {
        column: Column,
        paths: PrefixPaths,
    },
    Property {
        column: Column,
        rhs: Option<Column>,
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
        column: Column,
    },
    EntityKind {
        column: Column,
        entity: String,
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
        column: Column,
        definition: query_data_model::bindings::DefinitionId,
        key: query_data_model::bindings::ExportId,
    },
}

impl Predicate {
    pub fn map_columns(&self, mut map: impl FnMut(Column) -> Result<Column>) -> Result<Self> {
        let mut predicate = self.clone();
        let column = match &mut predicate {
            Self::Property { column, rhs, .. } => {
                *rhs = rhs.map(&mut map).transpose()?;
                column
            }
            Self::PathPrefixes { column, .. }
            | Self::Ids { column, .. }
            | Self::IdRange { column, .. }
            | Self::Live { column }
            | Self::EntityKind { column, .. }
            | Self::RelationshipKinds { column, .. }
            | Self::Tags { column, .. }
            | Self::Membership { column, .. } => column,
        };
        *column = map(*column)?;
        Ok(predicate)
    }
}

#[derive(Clone)]
pub enum PrefixPaths {
    Union(Vec<orbit_utils::traversal_path::TraversalPath>),
    Set(Vec<orbit_utils::traversal_path::TraversalPath>),
}

#[derive(Clone)]
pub enum OutputValue {
    Properties(Vec<(String, Column)>),
    Column(Column),
    Text(String),
    Depth(u32),
    Path(Vec<(Column, Column)>),
}

#[derive(Clone)]
pub struct Projection {
    pub value: OutputValue,
    pub name: query_data_model::bindings::ExportId,
}

pub fn id_list(column: Column, ids: &[i64]) -> Predicate {
    Predicate::Ids {
        column,
        values: ids.to_vec(),
    }
}

pub fn live(column: Column) -> Predicate {
    Predicate::Live { column }
}

pub fn relationship_kinds(column: Column, kinds: &[String]) -> Option<Predicate> {
    (!crate::passes::normalize::is_wildcard(kinds) && !kinds.is_empty()).then(|| {
        Predicate::RelationshipKinds {
            column,
            kinds: kinds.to_vec(),
        }
    })
}

pub fn tag_filter(column: Column, key: &str, filter: &InputFilter) -> Option<Predicate> {
    denorm_tag_values(key, filter).map(|values| Predicate::Tags { column, values })
}

impl<M: QueryDataModel + ?Sized> PlanningContext<'_, M> {
    pub fn property_filter(&self, column: Column, bound: &BoundFilter) -> Result<Predicate> {
        let rhs = bound
            .filter
            .rhs_column
            .as_ref()
            .map(|(_, property)| self.column(column.relation(), property))
            .transpose()?;
        Ok(Predicate::Property {
            column,
            rhs,
            filter: bound.filter.clone(),
            data_type: bound.data_type,
        })
    }
    pub fn node_predicates(&self, relation: RelationId, node: &NodePlan) -> Result<Vec<Predicate>> {
        let mut predicates: Vec<_> = node
            .filters
            .iter()
            .map(|(property, filter)| {
                self.property_filter(self.column(relation, property)?, filter)
            })
            .collect::<Result<_>>()?;
        if !node.node_ids.is_empty() {
            predicates.push(id_list(
                self.column(relation, DEFAULT_PRIMARY_KEY)?,
                &node.node_ids,
            ));
        }
        if let Some(range) = &node.id_range {
            predicates.push(Predicate::IdRange {
                column: self.column(relation, DEFAULT_PRIMARY_KEY)?,
                start: range.start,
                end: range.end,
            });
        }
        if let Some(column) =
            self.deletion_column(relation, node.table.as_deref().expect("bound node table"))?
        {
            predicates.push(live(column));
        }
        Ok(predicates)
    }

    pub fn node_outputs(&mut self, relation: RelationId, alias: &str) -> Result<Vec<Projection>> {
        let node = self.node(alias)?;
        if !node.emit_select {
            return Ok(vec![]);
        }
        let columns = requested_columns(&node.columns);
        let scope = self
            .bindings
            .relation_scope(relation)
            .map_err(|error| crate::error::QueryError::Lowering(error.to_string()))?;
        columns
            .into_iter()
            .map(|column| {
                self.projection(
                    scope,
                    OutputValue::Column(self.column(relation, &column)?),
                    format!("{alias}_{column}"),
                )
            })
            .collect()
    }
}

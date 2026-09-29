use std::convert::Infallible;

use query_data_model::{QueryBackendCatalog, QueryDataModel};

use super::bind::Source;
use super::generic::{Function, Operation, Schema, ValueId, ValueType, Values};
use crate::error::{QueryError, Result};

pub struct Read {
    pub table: String,
    pub columns: Vec<(ValueId, String)>,
    pub current_rows: CurrentRows,
}

#[derive(Clone, Copy)]
pub enum CurrentRows {
    Snapshot,
    Final,
}

impl Read {
    pub fn select(
        source: Source,
        model: &impl QueryDataModel,
        current_rows: CurrentRows,
    ) -> Result<Self> {
        let Source::Entity { entity, properties } = source else {
            return Self::select_edge(source, model, current_rows);
        };

        let entity_id = entity;
        let entity = &model.graph().entity(entity_id).name;
        let table = model
            .entity_table(entity)
            .ok_or_else(|| QueryError::ReferenceError(format!("entity {entity} is unavailable")))?;
        let columns = properties
            .into_iter()
            .map(|(value, property)| {
                if model.graph().property(property).entity != entity_id {
                    return Err(QueryError::ReferenceError(
                        "read properties must belong to one entity".into(),
                    ));
                }

                let column = model.property_column(property).ok_or_else(|| {
                    QueryError::ReferenceError("selected property is not stored".into())
                })?;
                Ok((value, column.to_string()))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            table: table.to_string(),
            columns,
            current_rows,
        })
    }

    fn select_edge(
        source: Source,
        model: &impl QueryDataModel,
        current_rows: CurrentRows,
    ) -> Result<Self> {
        let Source::Edge {
            relationships,
            fields,
        } = source
        else {
            unreachable!()
        };
        let tables = model.query_backend().edge_tables(&relationships);
        let [table] = tables.as_slice() else {
            return Err(QueryError::Lowering(
                "multi-table edges require union source selection".into(),
            ));
        };

        let columns = fields
            .into_iter()
            .map(|(value, field)| {
                let column = model
                    .query_backend()
                    .edge_field_column(table, field)
                    .ok_or_else(|| {
                        QueryError::ReferenceError(format!("edge field {field:?} is unavailable"))
                    })?;
                Ok((value, column.to_string()))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            table: table.clone(),
            columns,
            current_rows,
        })
    }
}

impl Operation for Read {
    fn output(&self, inputs: &[Schema], _: &Values) -> Result<Schema> {
        if !inputs.is_empty() {
            return Err(QueryError::PipelineInvariant(
                "read cannot have inputs".into(),
            ));
        }

        Ok(self.columns.iter().map(|(value, _)| *value).collect())
    }
}

impl Operation for Infallible {
    fn output(&self, _: &[Schema], _: &Values) -> Result<Schema> {
        match *self {}
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Scalar {
    Equal,
    NotEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    And,
    Or,
    IsNull,
    IsNotNull,
}

impl Function for Scalar {
    fn return_type(&self, arguments: &[ValueType]) -> Result<ValueType> {
        let base = |data_type: &ValueType| match data_type {
            ValueType::Nullable(inner) => inner.as_ref().clone(),
            other => other.clone(),
        };

        let valid = match (self, arguments) {
            (
                Self::Equal
                | Self::NotEqual
                | Self::Greater
                | Self::GreaterEqual
                | Self::Less
                | Self::LessEqual,
                [left, right],
            ) => base(left) == base(right),
            (Self::And | Self::Or, [left, right]) => {
                base(left) == ValueType::Bool && base(right) == ValueType::Bool
            }
            (Self::IsNull | Self::IsNotNull, [_]) => true,
            _ => false,
        };

        if !valid {
            return Err(QueryError::PipelineInvariant(
                "invalid scalar argument types".into(),
            ));
        }

        Ok(
            if !matches!(self, Self::IsNull | Self::IsNotNull)
                && arguments
                    .iter()
                    .any(|t| matches!(t, ValueType::Nullable(_)))
            {
                ValueType::Nullable(Box::new(ValueType::Bool))
            } else {
                ValueType::Bool
            },
        )
    }
}

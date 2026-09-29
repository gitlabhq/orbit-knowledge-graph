use std::convert::Infallible;

use query_data_model::QueryDataModel;

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
        let entity = &model.graph().entity(source.entity).name;
        let table = model
            .entity_table(entity)
            .ok_or_else(|| QueryError::ReferenceError(format!("entity {entity} is unavailable")))?;
        let columns = source
            .properties
            .into_iter()
            .map(|(value, property)| {
                if model.graph().property(property).entity != source.entity {
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

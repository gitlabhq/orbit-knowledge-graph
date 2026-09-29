use crate::error::{QueryError, Result};

use super::{Schema, require};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ValueId(usize);

pub use crate::ast::ValueType;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Values(Vec<ValueType>);

impl Values {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn prefix(&self, length: usize) -> Self {
        Self(self.0[..length].to_vec())
    }

    pub fn ids(&self) -> impl Iterator<Item = ValueId> {
        (0..self.0.len()).map(ValueId)
    }

    pub fn extends(&self, previous: &Self) -> bool {
        self.0.starts_with(&previous.0)
    }

    pub fn allocate(&mut self, data_type: ValueType) -> ValueId {
        let id = ValueId(self.0.len());
        self.0.push(data_type);

        id
    }

    pub fn data_type(&self, value: ValueId) -> Result<&ValueType> {
        self.0
            .get(value.0)
            .ok_or_else(|| QueryError::PipelineInvariant(format!("unknown plan value {}", value.0)))
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr<F> {
    Value(ValueId),
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Null(ValueType),
    Call {
        function: F,
        arguments: Vec<Self>,
    },
    Cast {
        value: Box<Self>,
        data_type: ValueType,
    },
}

pub trait Function {
    fn return_type(&self, arguments: &[ValueType]) -> Result<ValueType>;
}

impl<F> Expr<F> {
    pub fn map_values(&mut self, map: &mut impl FnMut(&mut ValueId)) {
        self.visit_mut(&mut |expression| {
            if let Self::Value(value) = expression {
                map(value);
            }
        });
    }

    pub fn visit_mut(&mut self, callback: &mut impl FnMut(&mut Self)) {
        match self {
            Self::Call { arguments, .. } => {
                for argument in arguments {
                    argument.visit_mut(callback);
                }
            }
            Self::Cast { value, .. } => value.visit_mut(callback),
            _ => {}
        }

        callback(self);
    }
}

impl<F: Function> Expr<F> {
    pub fn data_type(&self, available: &Schema, values: &Values) -> Result<ValueType> {
        Ok(match self {
            Self::Value(value) => {
                require(
                    available.contains(value),
                    "expression uses an unavailable value",
                )?;
                values.data_type(*value)?.clone()
            }
            Self::Bool(_) => ValueType::Bool,
            Self::Int64(_) => ValueType::Int64,
            Self::UInt64(_) => ValueType::UInt64,
            Self::Float64(_) => ValueType::Float64,
            Self::String(_) => ValueType::String,
            Self::Null(data_type) => {
                require(
                    matches!(data_type, ValueType::Nullable(_)),
                    "null requires a nullable type",
                )?;
                data_type.clone()
            }
            Self::Call {
                function,
                arguments,
            } => {
                let types = arguments
                    .iter()
                    .map(|argument| argument.data_type(available, values))
                    .collect::<Result<Vec<_>>>()?;

                function.return_type(&types)?
            }
            Self::Cast { value, data_type } => {
                value.data_type(available, values)?;
                data_type.clone()
            }
        })
    }
}

use crate::error::{QueryError, Result};

use super::{Schema, require};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ValueId(usize);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueType {
    Bool,
    Int64,
    UInt64,
    Float64,
    String,
    Date,
    DateTime,
    Nullable(Box<ValueType>),
    List(Box<ValueType>),
    Record(Vec<ValueType>),
}

#[derive(Clone, Debug, Default)]
pub struct Values(Vec<ValueType>);

impl Values {
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

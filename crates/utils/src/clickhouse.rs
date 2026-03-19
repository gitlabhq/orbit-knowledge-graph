//! ClickHouse parameter types shared between `compiler` and `clickhouse-client`.

use std::fmt;

use serde_json::Value;

/// ClickHouse scalar types for parameterized query placeholders.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
pub enum ChScalar {
    String,
    Int64,
    Float64,
    Bool,
}

impl ChScalar {
    /// Infer scalar type from a JSON value.
    pub fn from_value(v: Option<&Value>) -> Self {
        match v {
            Some(Value::Number(n)) if n.is_i64() => ChScalar::Int64,
            Some(Value::Number(_)) => ChScalar::Float64,
            Some(Value::Bool(_)) => ChScalar::Bool,
            _ => ChScalar::String,
        }
    }
}

/// ClickHouse types used in parameterized query placeholders (`{pN:Type}`).
///
/// Scalar variants map directly to ClickHouse types. `Array(ChScalar)` maps
/// to `Array(T)` for any scalar `T`, used in `IN` clauses with multiple values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChType {
    String,
    Int64,
    Float64,
    Bool,
    Array(ChScalar),
}

impl fmt::Display for ChType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ChType::String => write!(f, "String"),
            ChType::Int64 => write!(f, "Int64"),
            ChType::Float64 => write!(f, "Float64"),
            ChType::Bool => write!(f, "Bool"),
            ChType::Array(s) => write!(f, "Array({s})"),
        }
    }
}

impl ChType {
    /// Infer ClickHouse type from a JSON value.
    /// For arrays, inspects the first element to determine the element type.
    pub fn from_value(v: &Value) -> Self {
        match v {
            Value::Number(n) if n.is_i64() => ChType::Int64,
            Value::Number(_) => ChType::Float64,
            Value::Bool(_) => ChType::Bool,
            Value::Array(arr) => ChType::Array(ChScalar::from_value(arr.first())),
            _ => ChType::String,
        }
    }

    /// Promote a scalar type to its array equivalent.
    pub fn to_array(self) -> Self {
        match self {
            ChType::String => ChType::Array(ChScalar::String),
            ChType::Int64 => ChType::Array(ChScalar::Int64),
            ChType::Float64 => ChType::Array(ChScalar::Float64),
            ChType::Bool => ChType::Array(ChScalar::Bool),
            ChType::Array(_) => self,
        }
    }
}

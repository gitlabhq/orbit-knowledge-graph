use std::collections::HashMap;

use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeZone {
    Utc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScalarType {
    String,
    Int64,
    UInt32,
    Float64,
    Bool,
    Date,
    Timestamp {
        precision: u8,
        timezone: Option<TimeZone>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SqlType {
    String,
    Int64,
    UInt32,
    Float64,
    Bool,
    Date,
    Timestamp {
        precision: u8,
        timezone: Option<TimeZone>,
    },
    Array(ScalarType),
}

impl SqlType {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Number(number) if number.is_i64() => Self::Int64,
            Value::Number(_) => Self::Float64,
            Value::Bool(_) => Self::Bool,
            Value::Array(values) => {
                Self::from_value(values.first().unwrap_or(&Value::Null)).to_array()
            }
            _ => Self::String,
        }
    }

    pub fn to_array(self) -> Self {
        Self::Array(match self {
            Self::String => ScalarType::String,
            Self::Int64 => ScalarType::Int64,
            Self::UInt32 => ScalarType::UInt32,
            Self::Float64 => ScalarType::Float64,
            Self::Bool => ScalarType::Bool,
            Self::Date => ScalarType::Date,
            Self::Timestamp {
                precision,
                timezone,
            } => ScalarType::Timestamp {
                precision,
                timezone,
            },
            Self::Array(element) => element,
        })
    }
}

impl From<ScalarType> for SqlType {
    fn from(value: ScalarType) -> Self {
        match value {
            ScalarType::String => Self::String,
            ScalarType::Int64 => Self::Int64,
            ScalarType::UInt32 => Self::UInt32,
            ScalarType::Float64 => Self::Float64,
            ScalarType::Bool => Self::Bool,
            ScalarType::Date => Self::Date,
            ScalarType::Timestamp {
                precision,
                timezone,
            } => Self::Timestamp {
                precision,
                timezone,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ParamValue {
    pub data_type: SqlType,
    pub value: Value,
}

#[derive(Debug, Default)]
pub struct ParamBindings {
    params: HashMap<String, ParamValue>,
    index: HashMap<(SqlType, String), String>,
}

impl ParamBindings {
    pub fn intern(&mut self, data_type: SqlType, value: &Value) -> String {
        let key = (data_type, value.to_string());
        if let Some(name) = self.index.get(&key) {
            return name.clone();
        }
        let name = format!("p{}", self.params.len());
        self.index.insert(key, name.clone());
        self.params.insert(
            name.clone(),
            ParamValue {
                data_type,
                value: value.clone(),
            },
        );
        name
    }

    pub fn into_map(self) -> HashMap<String, ParamValue> {
        self.params
    }
}

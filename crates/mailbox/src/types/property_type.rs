//! Property type definitions for plugin schemas.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PropertyType {
    String,
    Int64,
    Float,
    Boolean,
    Date,
    Timestamp,
    Enum,
}

impl PropertyType {
    pub fn to_clickhouse_type(self, nullable: bool) -> String {
        let base_type = match self {
            PropertyType::String | PropertyType::Enum => "String",
            PropertyType::Int64 => "Int64",
            PropertyType::Float => "Float64",
            PropertyType::Boolean => "Bool",
            PropertyType::Date => "Date",
            PropertyType::Timestamp => "DateTime64(6, 'UTC')",
        };

        if nullable {
            format!("Nullable({base_type})")
        } else {
            base_type.to_string()
        }
    }

    pub fn to_arrow_data_type(self) -> arrow::datatypes::DataType {
        use arrow::datatypes::{DataType, TimeUnit};

        match self {
            PropertyType::String | PropertyType::Enum => DataType::Utf8,
            PropertyType::Int64 => DataType::Int64,
            PropertyType::Float => DataType::Float64,
            PropertyType::Boolean => DataType::Boolean,
            PropertyType::Date => DataType::Date32,
            PropertyType::Timestamp => {
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            }
        }
    }
}

impl std::fmt::Display for PropertyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropertyType::String => write!(f, "string"),
            PropertyType::Int64 => write!(f, "int64"),
            PropertyType::Float => write!(f, "float"),
            PropertyType::Boolean => write!(f, "boolean"),
            PropertyType::Date => write!(f, "date"),
            PropertyType::Timestamp => write!(f, "timestamp"),
            PropertyType::Enum => write!(f, "enum"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clickhouse_type_mapping() {
        assert_eq!(PropertyType::String.to_clickhouse_type(false), "String");
        assert_eq!(PropertyType::Int64.to_clickhouse_type(false), "Int64");
        assert_eq!(PropertyType::Float.to_clickhouse_type(false), "Float64");
        assert_eq!(PropertyType::Boolean.to_clickhouse_type(false), "Bool");
        assert_eq!(PropertyType::Date.to_clickhouse_type(false), "Date");
        assert_eq!(
            PropertyType::Timestamp.to_clickhouse_type(false),
            "DateTime64(6, 'UTC')"
        );
        assert_eq!(PropertyType::Enum.to_clickhouse_type(false), "String");
    }

    #[test]
    fn nullable_clickhouse_types() {
        assert_eq!(
            PropertyType::String.to_clickhouse_type(true),
            "Nullable(String)"
        );
        assert_eq!(
            PropertyType::Int64.to_clickhouse_type(true),
            "Nullable(Int64)"
        );
    }

    #[test]
    fn serde_roundtrip() {
        let types = vec![
            PropertyType::String,
            PropertyType::Int64,
            PropertyType::Float,
            PropertyType::Boolean,
            PropertyType::Date,
            PropertyType::Timestamp,
            PropertyType::Enum,
        ];

        for property_type in types {
            let json = serde_json::to_string(&property_type).unwrap();
            let parsed: PropertyType = serde_json::from_str(&json).unwrap();
            assert_eq!(property_type, parsed);
        }
    }
}

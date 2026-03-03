pub mod diff;
pub mod from_clickhouse;
pub mod from_ontology;
pub mod sql;

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub engine: Engine,
    pub order_by: Vec<String>,
    pub primary_key: Vec<String>,
    pub settings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSchema {
    pub name: String,
    pub column_type: ClickHouseType,
    pub nullable: bool,
    pub default_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClickHouseType {
    Int64,
    UInt8,
    UInt64,
    Float64,
    String,
    Bool,
    Date32,
    DateTime64,
    UUID,
}

impl fmt::Display for ClickHouseType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClickHouseType::Int64 => write!(f, "Int64"),
            ClickHouseType::UInt8 => write!(f, "UInt8"),
            ClickHouseType::UInt64 => write!(f, "UInt64"),
            ClickHouseType::Float64 => write!(f, "Float64"),
            ClickHouseType::String => write!(f, "String"),
            ClickHouseType::Bool => write!(f, "Bool"),
            ClickHouseType::Date32 => write!(f, "Date32"),
            ClickHouseType::DateTime64 => write!(f, "DateTime64(6, 'UTC')"),
            ClickHouseType::UUID => write!(f, "UUID"),
        }
    }
}

impl ClickHouseType {
    pub fn to_sql(&self, nullable: bool) -> String {
        let base = self.to_string();
        if nullable {
            format!("Nullable({base})")
        } else {
            base
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Engine {
    ReplacingMergeTree {
        version_column: String,
        deleted_column: Option<String>,
    },
}

impl fmt::Display for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Engine::ReplacingMergeTree {
                version_column,
                deleted_column: Some(deleted),
            } => write!(f, "ReplacingMergeTree({version_column}, {deleted})"),
            Engine::ReplacingMergeTree {
                version_column,
                deleted_column: None,
            } => write!(f, "ReplacingMergeTree({version_column})"),
        }
    }
}

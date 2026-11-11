use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum DataType {
    #[serde(rename = "string")]
    String,
    #[serde(rename = "int64")]
    Int,
    #[serde(rename = "float64")]
    Float,
    #[serde(rename = "boolean")]
    Bool,
    #[serde(rename = "date")]
    Date,
    #[serde(rename = "timestamp")]
    DateTime,
    #[serde(rename = "enum")]
    Enum,
    #[serde(rename = "uuid")]
    Uuid,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::String => write!(f, "String"),
            DataType::Int => write!(f, "Int"),
            DataType::Float => write!(f, "Float"),
            DataType::Bool => write!(f, "Bool"),
            DataType::Date => write!(f, "Date"),
            DataType::DateTime => write!(f, "DateTime"),
            DataType::Enum => write!(f, "Enum"),
            DataType::Uuid => write!(f, "Uuid"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnumType {
    /// Integer values that map to string labels (requires CASE transformation).
    #[default]
    Int,
    /// Already stored as strings with constrained values (pass-through, no transformation).
    String,
}

/// Filter selectivity hint for the query planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSelectivity {
    /// Few distinct values (enums, booleans). Filtering narrows weakly.
    Low,
    /// Many distinct values (IDs, strings, timestamps). Filtering narrows effectively.
    #[default]
    High,
}

impl FieldSelectivity {
    pub fn from_data_type(dt: DataType) -> Self {
        match dt {
            DataType::Enum | DataType::Bool => Self::Low,
            _ => Self::High,
        }
    }
}

/// How many rows of each endpoint one row of the other endpoint links to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cardinality {
    OneToOne,
    OneToMany,
    ManyToOne,
    ManyToMany,
}

impl Cardinality {
    /// Whether a foreign key on `holder` can carry the variant.
    pub fn allows_key_on(self, holder: crate::Endpoint) -> bool {
        match self {
            Self::OneToOne => true,
            Self::OneToMany => holder == crate::Endpoint::Target,
            Self::ManyToOne => holder == crate::Endpoint::Source,
            Self::ManyToMany => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalPathKind {
    Id,
    FullPath,
}

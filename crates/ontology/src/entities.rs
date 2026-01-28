//! Data model definitions for ontology entities.

use std::{collections::BTreeMap, fmt};

use crate::etl::EtlConfig;

/// A node entity representing a record or row in the knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntity {
    /// The name of the entity (e.g., "User", "Project").
    pub name: String,
    /// The fields that make up this entity.
    pub fields: Vec<Field>,
    /// The field names that form the primary key.
    pub primary_keys: Vec<String>,
    /// The destination table name for this entity.
    pub destination_table: String,
    /// ETL configuration for indexing this entity.
    pub etl: Option<EtlConfig>,
}

impl fmt::Display for NodeEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.name)
    }
}

/// An edge entity representing a relationship between nodes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEntity {
    /// The name of the relationship (e.g., "AUTHORED", "CONTAINS").
    pub relationship_kind: String,
    /// The field containing the source node identifier.
    pub source: String,
    /// The kind of the source node.
    pub source_kind: String,
    /// The field containing the target node identifier.
    pub target: String,
    /// The kind of the target node.
    pub target_kind: String,
}

impl fmt::Display for EdgeEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Edge({}: {} -> {})",
            self.relationship_kind, self.source_kind, self.target_kind
        )
    }
}

/// A field definition within an entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    /// The name of the field.
    pub name: String,
    /// The source column name in the source table.
    pub source: String,
    /// The data type of the field.
    pub data_type: DataType,
    /// Whether the field can contain null values.
    pub nullable: bool,
    /// Integer value to string label mapping for enum types.
    pub enum_values: Option<BTreeMap<i64, String>>,
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nullable = if self.nullable { "?" } else { "" };
        write!(f, "{}: {}{}", self.name, self.data_type, nullable)
    }
}

/// Supported data types for entity fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    /// A UTF-8 string.
    String,
    /// A 64-bit signed integer.
    Int,
    /// A 64-bit floating point number.
    Float,
    /// A boolean value.
    Bool,
    /// A date value (no time component).
    Date,
    /// A date and time value.
    DateTime,
    /// Enum
    Enum,
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
        }
    }
}

impl DataType {
    /// Convert to JSON Schema type string.
    #[must_use]
    pub fn to_json_schema_type(self) -> &'static str {
        match self {
            DataType::String | DataType::Date | DataType::DateTime | DataType::Enum => "string",
            DataType::Int => "integer",
            DataType::Float => "number",
            DataType::Bool => "boolean",
        }
    }
}

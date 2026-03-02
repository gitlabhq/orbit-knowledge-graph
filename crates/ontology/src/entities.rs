//! Data model definitions for ontology entities.

use std::{collections::BTreeMap, fmt};

use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::etl::EtlConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainInfo {
    pub name: String,
    pub description: String,
    pub node_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeStyle {
    pub size: i32,
    pub color: String,
}

impl Default for NodeStyle {
    fn default() -> Self {
        Self {
            size: 30,
            color: "#6B7280".to_string(),
        }
    }
}

/// Redaction configuration for an entity.
///
/// Defines how this entity should be validated against Rails' RedactionService
/// to ensure users have permission to view the entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedactionConfig {
    /// Rails resource type (e.g., "projects", "merge_requests", "groups", "users").
    /// This maps to the key used in `Authz::RedactionService::RESOURCE_CLASSES`.
    pub resource_type: String,
    /// Column containing the ID for redaction (defaults to "id").
    pub id_column: String,
    /// The ability to check for this resource (e.g., "read_project", "read_group").
    /// Defaults to "read".
    pub ability: String,
}

/// A node entity representing a record or row in the knowledge graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntity {
    /// The name of the entity (e.g., "User", "Project").
    pub name: String,
    pub domain: String,
    pub description: String,
    pub label: String,
    /// The fields that make up this entity.
    pub fields: Vec<Field>,
    /// The field names that form the primary key.
    pub primary_keys: Vec<String>,
    /// The destination table name for this entity.
    pub destination_table: String,
    /// Columns returned by default when this node appears in dynamic query results.
    /// If empty, all columns are returned.
    pub default_columns: Vec<String>,
    /// ClickHouse ORDER BY columns for this node's destination table.
    /// Used as the deduplication key for ReplacingMergeTree.
    pub sort_key: Vec<String>,
    /// ETL configuration for indexing this entity.
    pub etl: Option<EtlConfig>,
    /// Redaction configuration for permission checks.
    /// If `None`, this entity does not require redaction validation.
    pub redaction: Option<RedactionConfig>,
    pub style: NodeStyle,
}

impl Default for NodeEntity {
    fn default() -> Self {
        Self {
            name: String::new(),
            domain: String::new(),
            description: String::new(),
            label: String::new(),
            fields: vec![],
            primary_keys: vec![DEFAULT_PRIMARY_KEY.to_string()],
            default_columns: vec![],
            sort_key: vec![],
            destination_table: String::new(),
            etl: None,
            redaction: None,
            style: NodeStyle::default(),
        }
    }
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

/// ETL configuration for edges sourced from join tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeSourceEtlConfig {
    /// Whether this is global or namespaced.
    pub scope: crate::etl::EtlScope,
    /// Source table name.
    pub source: String,
    /// Column name for watermark (version tracking).
    pub watermark: String,
    /// Column name for soft delete flag.
    pub deleted: String,
    /// Source endpoint configuration.
    pub from: EdgeEndpoint,
    /// Target endpoint configuration.
    pub to: EdgeEndpoint,
}

/// Configuration for an edge endpoint (source or target).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEndpoint {
    /// Column containing the ID of the node.
    pub id_column: String,
    /// How the node type is determined.
    pub node_type: EdgeEndpointType,
}

/// How an edge endpoint's node type is determined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeEndpointType {
    /// A fixed node type (e.g., "Label").
    Literal(String),
    /// Type read from a column at runtime (e.g., "target_type").
    Column {
        column: String,
        type_mapping: std::collections::BTreeMap<String, String>,
    },
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
    /// Integer value to string label mapping for int-based enum types.
    pub enum_values: Option<BTreeMap<i64, String>>,
    /// How the enum is stored in the source (int or string). Defaults to Int.
    pub enum_type: EnumType,
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
    /// A UUID value.
    Uuid,
}

/// Enum storage type - how the enum is stored in the source database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum EnumType {
    /// Integer values that map to string labels (requires CASE transformation).
    #[default]
    Int,
    /// Already stored as strings with constrained values (pass-through, no transformation).
    String,
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

impl DataType {
    /// Convert to JSON Schema type string.
    #[must_use]
    pub fn to_json_schema_type(self) -> &'static str {
        match self {
            DataType::String
            | DataType::Date
            | DataType::DateTime
            | DataType::Enum
            | DataType::Uuid => "string",
            DataType::Int => "integer",
            DataType::Float => "number",
            DataType::Bool => "boolean",
        }
    }
}

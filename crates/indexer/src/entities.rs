//! Data model definitions. [`Entity::Node`] for records, [`Entity::Edge`] for relationships.

/// An entity definition describing a data structure.
///
/// Entities are either nodes (standalone records) or edges (relationships
/// between nodes). Modules declare their entities to describe what data
/// they produce.
#[derive(Clone)]
pub enum Entity {
    /// A node entity representing a record or row.
    Node {
        /// The name of the entity (e.g., table name).
        name: String,
        /// The fields that make up this entity.
        fields: Vec<Field>,
        /// The field names that form the primary key.
        primary_keys: Vec<String>,
    },

    /// An edge entity representing a relationship between nodes.
    Edge {
        /// The field containing the source node identifier.
        source: String,
        /// The kind of the source node.
        source_kind: String,
        /// The name of the relationship.
        relationship_kind: String,
        /// The field containing the target node identifier.
        target: String,
        /// The kind of the target node.
        target_kind: String,
    },
}

/// A field definition within an entity.
#[derive(Clone)]
pub struct Field {
    /// The name of the field.
    pub name: String,
    /// The data type of the field.
    pub data_type: DataType,
    /// Whether the field can contain null values.
    pub nullable: bool,
    /// The default value for this field, if any.
    pub default: Option<DefaultValue>,
}

/// Supported data types for entity fields.
#[derive(Clone)]
pub enum DataType {
    /// A UTF-8 string.
    String,
    /// A 64-bit signed integer.
    Int,
    /// A 64-bit floating point number.
    Float,
    /// A boolean value.
    Bool,
    /// A date and time value.
    DateTime,
}

/// Default values for entity fields.
///
/// Used when a field value is not provided.
#[derive(Clone)]
pub enum DefaultValue {
    /// A string default value.
    String(String),
    /// An integer default value.
    Int(i64),
    /// A float default value.
    Float(f64),
    /// A boolean default value.
    Bool(bool),
    /// The current timestamp.
    Now,
    /// A null value.
    Null,
}

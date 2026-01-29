//! Ontology loading from YAML files.
//!
//! This crate loads ontology definitions from YAML files and converts them
//! into strongly-typed entity definitions for the Knowledge Graph.
//!
//! # Example
//!
//! ```ignore
//! use ontology::Ontology;
//!
//! // Load from embedded files (compiled into binary)
//! let ontology = Ontology::load_embedded()?;
//!
//! // Or load from a directory on disk
//! let ontology = Ontology::load_from_dir("fixtures/ontology")?;
//! let user = ontology.get_node("User").expect("User node exists");
//! ```

mod entities;
pub mod etl;

pub use entities::{DataType, EdgeEntity, Field, NodeEntity};
pub use etl::{
    DELETED_COLUMN, EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope,
    TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};

use rust_embed::Embed;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

/// Embedded ontology files from `fixtures/ontology/`.
#[derive(Embed)]
#[folder = "$CARGO_MANIFEST_DIR/../../fixtures/ontology/"]
struct EmbeddedOntology;

/// Primary key field name used by default.
const DEFAULT_PRIMARY_KEY: &str = "id";

/// Reserved columns that exist on all nodes.
pub const NODE_RESERVED_COLUMNS: &[&str] = &["id"];

/// Reserved columns on the edge table (matches EdgeEntity schema).
pub const EDGE_RESERVED_COLUMNS: &[&str] = &[
    "relationship_kind",
    "source",
    "source_kind",
    "target",
    "target_kind",
];

/// Edge table name in ClickHouse.
pub const EDGE_TABLE: &str = "kg_edges";

/// Errors that can occur when loading or validating an ontology.
#[derive(Debug)]
pub enum OntologyError {
    /// Failed to read a file.
    Io {
        path: String,
        source: std::io::Error,
    },
    /// Failed to parse YAML.
    Yaml {
        path: String,
        source: serde_yaml::Error,
    },
    /// Ontology validation failed.
    Validation(String),
}

impl std::error::Error for OntologyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            OntologyError::Io { source, .. } => Some(source),
            OntologyError::Yaml { source, .. } => Some(source),
            OntologyError::Validation(_) => None,
        }
    }
}

impl fmt::Display for OntologyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OntologyError::Io { path, source } => {
                write!(f, "failed to read '{}': {}", path, source)
            }
            OntologyError::Yaml { path, source } => {
                write!(f, "failed to parse '{}': {}", path, source)
            }
            OntologyError::Validation(msg) => write!(f, "validation error: {}", msg),
        }
    }
}

/// A loaded ontology containing all node and edge entities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ontology {
    nodes: BTreeMap<String, NodeEntity>,
    edges: BTreeMap<String, Vec<EdgeEntity>>,
}

impl Default for Ontology {
    fn default() -> Self {
        Self::new()
    }
}

impl Ontology {
    /// Create an empty ontology.
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
        }
    }

    /// Add nodes by name.
    #[must_use]
    pub fn with_nodes(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for name in names {
            let name = name.into();
            self.nodes.insert(
                name.clone(),
                NodeEntity {
                    name: name.clone(),
                    fields: vec![],
                    primary_keys: vec![DEFAULT_PRIMARY_KEY.to_string()],
                    destination_table: format!("gl_{}", name.to_lowercase()),
                    etl: None,
                },
            );
        }
        self
    }

    /// Add edge types by name.
    #[must_use]
    pub fn with_edges(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for name in names {
            self.edges.insert(name.into(), vec![]);
        }
        self
    }

    /// Add fields to an existing node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node doesn't exist.
    pub fn try_with_fields(
        mut self,
        node_name: &str,
        fields: impl IntoIterator<Item = (impl Into<String>, DataType, bool)>,
    ) -> Result<Self, OntologyError> {
        let node = self.nodes.get_mut(node_name).ok_or_else(|| {
            OntologyError::Validation(format!("node \"{node_name}\" does not exist"))
        })?;
        for (field_name, data_type, nullable) in fields {
            let field_name_string: String = field_name.into();
            node.fields.push(Field {
                name: field_name_string.clone(),
                source: field_name_string,
                data_type,
                nullable,
                enum_values: None,
            });
        }
        Ok(self)
    }

    /// Add fields to an existing node (convenience method, fields default to nullable).
    ///
    /// # Panics
    ///
    /// Panics if the node doesn't exist. Use [`try_with_fields`](Self::try_with_fields)
    /// for fallible version.
    #[must_use]
    pub fn with_fields(
        self,
        node_name: &str,
        fields: impl IntoIterator<Item = (impl Into<String>, DataType)>,
    ) -> Self {
        let fields_with_nullable = fields.into_iter().map(|(n, t)| (n, t, true));
        self.try_with_fields(node_name, fields_with_nullable)
            .unwrap_or_else(|e| panic!("{e}"))
    }

    /// Load ontology from a directory containing schema.yaml and referenced files.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any referenced file cannot be read
    /// - YAML parsing fails
    /// - Validation fails (duplicate nodes, invalid edge references, etc.)
    #[must_use = "this returns the loaded ontology, which should not be discarded"]
    pub fn load_from_dir(dir: impl AsRef<Path>) -> Result<Self, OntologyError> {
        Self::load_with(&DirReader(dir.as_ref()))
    }

    /// Load ontology from embedded files compiled into the binary.
    ///
    /// This uses the ontology files from `fixtures/ontology/` that were
    /// embedded at compile time.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Any referenced file cannot be found in embedded assets
    /// - YAML parsing fails
    /// - Validation fails (duplicate nodes, invalid edge references, etc.)
    #[must_use = "this returns the loaded ontology, which should not be discarded"]
    pub fn load_embedded() -> Result<Self, OntologyError> {
        Self::load_with(&EmbeddedReader)
    }

    fn load_with(reader: &impl ReadOntologyFile) -> Result<Self, OntologyError> {
        let schema_content = reader.read("schema.yaml")?;
        let schema: SchemaYaml = parse_yaml(&schema_content, "schema.yaml")?;

        let mut ontology = Ontology::new();

        for domain in schema.domains.values() {
            for (node_name, node_path) in &domain.nodes {
                if ontology.nodes.contains_key(node_name) {
                    return Err(OntologyError::Validation(format!(
                        "duplicate node definition: '{}'",
                        node_name
                    )));
                }

                let content = reader.read(node_path)?;
                let node_def: NodeYaml = parse_yaml(&content, node_path)?;

                let entity = node_def.into_entity(node_name.clone())?;
                ontology.nodes.insert(node_name.clone(), entity);
            }
        }

        for (edge_name, edge_path) in &schema.edges {
            let content = reader.read(edge_path)?;
            let edge_def: EdgeYaml = parse_yaml(&content, edge_path)?;

            let entities = edge_def.into_entities(edge_name.clone());

            for entity in &entities {
                if !ontology.nodes.contains_key(&entity.source_kind) {
                    return Err(OntologyError::Validation(format!(
                        "edge '{}' references unknown source node '{}'",
                        edge_name, entity.source_kind
                    )));
                }
                if !ontology.nodes.contains_key(&entity.target_kind) {
                    return Err(OntologyError::Validation(format!(
                        "edge '{}' references unknown target node '{}'",
                        edge_name, entity.target_kind
                    )));
                }
            }

            ontology.edges.insert(edge_name.clone(), entities);
        }

        Ok(ontology)
    }

    /// Get a node by name.
    #[must_use]
    pub fn get_node(&self, name: &str) -> Option<&NodeEntity> {
        self.nodes.get(name)
    }

    /// Get all variants of an edge by relationship name.
    #[must_use]
    pub fn get_edge(&self, name: &str) -> Option<&[EdgeEntity]> {
        self.edges.get(name).map(|v| v.as_slice())
    }

    /// Get allowed target types for a polymorphic edge.
    ///
    /// Given a relationship and the node kind that has the FK column,
    /// returns the valid types on the other end based on edge schema variants.
    #[must_use]
    pub fn get_edge_target_types(
        &self,
        relationship_kind: &str,
        node_kind: &str,
        direction: EdgeDirection,
    ) -> Vec<String> {
        let Some(variants) = self.get_edge(relationship_kind) else {
            return Vec::new();
        };

        variants
            .iter()
            .filter_map(|edge| match direction {
                EdgeDirection::Outgoing if edge.source_kind == node_kind => {
                    Some(edge.target_kind.clone())
                }
                EdgeDirection::Incoming if edge.target_kind == node_kind => {
                    Some(edge.source_kind.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// Check if a node exists.
    #[must_use]
    pub fn has_node(&self, name: &str) -> bool {
        self.nodes.contains_key(name)
    }

    /// Check if an edge exists.
    #[must_use]
    pub fn has_edge(&self, name: &str) -> bool {
        self.edges.contains_key(name)
    }

    /// Iterator over all nodes.
    pub fn nodes(&self) -> impl Iterator<Item = &NodeEntity> {
        self.nodes.values()
    }

    /// Iterator over all node names.
    pub fn node_names(&self) -> impl Iterator<Item = &str> {
        self.nodes.keys().map(|s| s.as_str())
    }

    /// Iterator over all edges (flattened).
    pub fn edges(&self) -> impl Iterator<Item = &EdgeEntity> {
        self.edges.values().flatten()
    }

    /// Iterator over all edge names.
    pub fn edge_names(&self) -> impl Iterator<Item = &str> {
        self.edges.keys().map(|s| s.as_str())
    }

    /// Number of nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Number of edge types.
    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    // --- Query validation helpers ---

    /// Check if a field exists on a node, including reserved columns.
    ///
    /// Returns `true` if:
    /// - The node exists AND the field is a reserved column (`id`)
    /// - The node exists AND the field exists in the node's field definitions
    ///
    /// Returns `false` if the node doesn't exist.
    #[must_use]
    pub fn has_field(&self, node_name: &str, field_name: &str) -> bool {
        // Node must exist first
        let Some(node) = self.nodes.get(node_name) else {
            return false;
        };

        // Reserved columns on node tables
        if NODE_RESERVED_COLUMNS.contains(&field_name) {
            return true;
        }

        // Check defined fields
        node.fields.iter().any(|f| f.name == field_name)
    }

    /// Validate that a field exists on a node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The entity name is empty
    /// - The node doesn't exist in the ontology
    /// - The field doesn't exist on the node (and isn't a reserved column)
    pub fn validate_field(&self, node_name: &str, field_name: &str) -> Result<(), OntologyError> {
        if node_name.is_empty() {
            return Err(OntologyError::Validation(format!(
                "cannot validate field \"{field_name}\" without an entity type"
            )));
        }

        let node = self.nodes.get(node_name).ok_or_else(|| {
            OntologyError::Validation(format!("unknown node type \"{node_name}\""))
        })?;

        // Reserved columns on node tables
        if NODE_RESERVED_COLUMNS.contains(&field_name) {
            return Ok(());
        }

        if node.fields.iter().any(|f| f.name == field_name) {
            return Ok(());
        }

        Err(OntologyError::Validation(format!(
            "field \"{field_name}\" does not exist on node type \"{node_name}\""
        )))
    }

    /// Validate that a type is a valid node label or edge type.
    ///
    /// # Errors
    ///
    /// Returns an error if the type is neither a node label nor an edge type.
    pub fn validate_type(&self, type_name: &str) -> Result<(), OntologyError> {
        if self.has_node(type_name) || self.has_edge(type_name) {
            return Ok(());
        }
        Err(OntologyError::Validation(format!(
            "\"{type_name}\" is not a valid node label or relationship type"
        )))
    }

    /// Get the ClickHouse table name for a node label.
    ///
    /// Node tables follow the pattern `kg_{lowercase_label}`.
    /// Example: `User` → `kg_user`, `Project` → `kg_project`
    ///
    /// # Errors
    ///
    /// Returns an error if the node label is unknown.
    pub fn table_name(&self, node_label: &str) -> Result<String, OntologyError> {
        if !self.has_node(node_label) {
            return Err(OntologyError::Validation(format!(
                "unknown node label \"{node_label}\""
            )));
        }
        Ok(format!("kg_{}", node_label.to_lowercase()))
    }

    /// Generate a JSON Schema with ontology values populated.
    ///
    /// Given a base schema template, this populates:
    /// - `$defs.EntityType.enum` with valid entity types
    /// - `$defs.RelationshipTypeName.enum` with valid relationship types
    /// - `$defs.NodeProperties` with property definitions per node type
    ///
    /// # Errors
    ///
    /// Returns an error if the base schema is invalid JSON or missing required sections.
    pub fn derive_json_schema(&self, base_schema_json: &str) -> Result<Value, OntologyError> {
        let mut schema: Value = serde_json::from_str(base_schema_json)
            .map_err(|e| OntologyError::Validation(format!("failed to parse base schema: {e}")))?;

        let defs = schema
            .get_mut("$defs")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| OntologyError::Validation("schema missing $defs".into()))?;

        // Populate EntityType enum with valid entity names
        if let Some(entity_type) = defs.get_mut("EntityType").and_then(Value::as_object_mut) {
            let types: Vec<Value> = self
                .node_names()
                .map(|s| Value::String(s.to_string()))
                .collect();
            entity_type.insert("enum".to_string(), Value::Array(types));
        }

        // Populate RelationshipTypeName enum
        if let Some(rel_type) = defs
            .get_mut("RelationshipTypeName")
            .and_then(Value::as_object_mut)
        {
            let types: Vec<Value> = self
                .edge_names()
                .map(|s| Value::String(s.to_string()))
                .collect();
            rel_type.insert("enum".to_string(), Value::Array(types));
        }

        // Populate NodeProperties with property definitions per node type
        let node_props = self.build_node_properties_schema();
        defs.insert("NodeProperties".to_string(), node_props);

        Ok(schema)
    }

    /// Build the NodeProperties schema object from node field definitions.
    fn build_node_properties_schema(&self) -> Value {
        let mut node_props = Map::new();

        for node in self.nodes() {
            let mut prop_map = Map::new();

            for field in &node.fields {
                let mut prop_schema = Map::new();
                prop_schema.insert(
                    "type".to_string(),
                    Value::String(field.data_type.to_json_schema_type().to_string()),
                );

                if let Some(enum_values) = &field.enum_values {
                    let values: Vec<Value> = enum_values
                        .values()
                        .map(|v| Value::String(v.clone()))
                        .collect();
                    prop_schema.insert("enum".to_string(), Value::Array(values));
                }

                prop_map.insert(field.name.clone(), Value::Object(prop_schema));
            }

            node_props.insert(node.name.clone(), Value::Object(prop_map));
        }

        Value::Object(node_props)
    }
}

impl fmt::Display for Ontology {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Ontology({} nodes, {} edge types)",
            self.node_count(),
            self.edge_count()
        )
    }
}

// --- File reading ---

trait ReadOntologyFile {
    fn read(&self, path: &str) -> Result<String, OntologyError>;
}

struct DirReader<'a>(&'a Path);

impl ReadOntologyFile for DirReader<'_> {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        let full_path = self.0.join(path);
        std::fs::read_to_string(&full_path).map_err(|e| OntologyError::Io {
            path: full_path.to_string_lossy().to_string(),
            source: e,
        })
    }
}

struct EmbeddedReader;

impl ReadOntologyFile for EmbeddedReader {
    fn read(&self, path: &str) -> Result<String, OntologyError> {
        let file = EmbeddedOntology::get(path).ok_or_else(|| OntologyError::Io {
            path: path.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("embedded file not found: {}", path),
            ),
        })?;

        String::from_utf8(file.data.to_vec()).map_err(|e| OntologyError::Io {
            path: path.to_string(),
            source: std::io::Error::new(std::io::ErrorKind::InvalidData, e),
        })
    }
}

fn parse_yaml<T: for<'de> Deserialize<'de>>(content: &str, path: &str) -> Result<T, OntologyError> {
    serde_yaml::from_str(content).map_err(|e| OntologyError::Yaml {
        path: path.to_string(),
        source: e,
    })
}

fn parse_data_type(s: &str, field_name: &str) -> Result<DataType, OntologyError> {
    match s {
        "int64" | "int" | "integer" => Ok(DataType::Int),
        "float64" | "float" | "double" => Ok(DataType::Float),
        "boolean" | "bool" => Ok(DataType::Bool),
        "date" => Ok(DataType::Date),
        "timestamp" | "datetime" => Ok(DataType::DateTime),
        "string" => Ok(DataType::String),
        "enum" => Ok(DataType::Enum),
        other => Err(OntologyError::Validation(format!(
            "unknown data type '{}' for field '{}'",
            other, field_name
        ))),
    }
}

// --- YAML deserialization types ---

#[derive(Debug, Deserialize)]
struct SchemaYaml {
    #[serde(default)]
    domains: BTreeMap<String, DomainYaml>,
    #[serde(default)]
    edges: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct DomainYaml {
    #[serde(default)]
    nodes: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct NodeYaml {
    #[allow(dead_code)]
    node_type: String,
    #[allow(dead_code)]
    domain: String,
    #[serde(default)]
    #[allow(dead_code)]
    description: String,
    destination_table: String,
    #[serde(default)]
    properties: BTreeMap<String, PropertyYaml>,
    #[serde(default)]
    etl: Option<EtlYaml>,
}

#[derive(Debug, Deserialize)]
struct EtlYaml {
    #[serde(rename = "type")]
    etl_type: String,
    scope: String,
    #[serde(default)]
    source: Option<String>,
    #[serde(default)]
    watermark: Option<String>,
    #[serde(default)]
    deleted: Option<String>,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    edges: BTreeMap<String, EdgeMappingYaml>,
}

#[derive(Debug, Deserialize)]
struct EdgeMappingYaml {
    #[serde(rename = "to")]
    target_literal: Option<String>,
    #[serde(rename = "to_column")]
    target_column: Option<String>,
    #[serde(rename = "as")]
    relationship_kind: String,
    #[serde(default)]
    direction: EdgeDirection,
}

#[derive(Debug, Deserialize)]
struct PropertyYaml {
    #[serde(rename = "type")]
    property_type: String,
    source: String,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    #[allow(dead_code)]
    description: String,
    #[serde(default)]
    values: Option<BTreeMap<i64, String>>,
}

#[derive(Debug, Deserialize)]
struct EdgeYaml {
    #[serde(default)]
    variants: Vec<EdgeVariantYaml>,
}

#[derive(Debug, Deserialize)]
struct EdgeVariantYaml {
    from_node: EdgeNodeRef,
    to_node: EdgeNodeRef,
}

#[derive(Debug, Deserialize)]
struct EdgeNodeRef {
    #[serde(rename = "type")]
    node_type: String,
    id: String,
}

// --- Conversion implementations ---

impl NodeYaml {
    fn into_entity(self, name: String) -> Result<NodeEntity, OntologyError> {
        let mut primary_keys = Vec::new();

        let fields: Result<Vec<Field>, OntologyError> = self
            .properties
            .into_iter()
            .map(|(prop_name, prop_def)| {
                if prop_name == DEFAULT_PRIMARY_KEY {
                    primary_keys.push(prop_name.clone());
                }

                let data_type = parse_data_type(&prop_def.property_type, &prop_name)?;

                Ok(Field {
                    name: prop_name,
                    source: prop_def.source,
                    data_type,
                    nullable: prop_def.nullable,
                    enum_values: prop_def.values,
                })
            })
            .collect();

        let fields = fields?;

        // Default primary key if none found
        if primary_keys.is_empty() {
            primary_keys.push(DEFAULT_PRIMARY_KEY.to_string());
        }

        // Validate primary keys exist in fields
        for pk in &primary_keys {
            if !fields.iter().any(|f| &f.name == pk) {
                return Err(OntologyError::Validation(format!(
                    "primary key '{}' not found in fields for node '{}'",
                    pk, name
                )));
            }
        }

        // Convert ETL config
        let etl = self.etl.map(|e| e.into_config()).transpose()?;

        Ok(NodeEntity {
            name,
            fields,
            primary_keys,
            destination_table: self.destination_table,
            etl,
        })
    }
}

impl EtlYaml {
    fn into_config(self) -> Result<EtlConfig, OntologyError> {
        let scope = match self.scope.as_str() {
            "global" => EtlScope::Global,
            "namespaced" => EtlScope::Namespaced,
            other => {
                return Err(OntologyError::Validation(format!(
                    "invalid ETL scope: '{}', expected 'global' or 'namespaced'",
                    other
                )));
            }
        };

        let edges: Result<BTreeMap<String, EdgeMapping>, OntologyError> = self
            .edges
            .into_iter()
            .map(|(column, mapping)| {
                let target = match (mapping.target_literal, mapping.target_column) {
                    (Some(lit), None) => EdgeTarget::Literal(lit),
                    (None, Some(col)) => EdgeTarget::Column(col),
                    (Some(_), Some(_)) => {
                        return Err(OntologyError::Validation(format!(
                            "edge '{}': use 'to' or 'to_column', not both",
                            column
                        )));
                    }
                    (None, None) => {
                        return Err(OntologyError::Validation(format!(
                            "edge '{}': requires 'to' or 'to_column'",
                            column
                        )));
                    }
                };
                Ok((
                    column,
                    EdgeMapping {
                        target,
                        relationship_kind: mapping.relationship_kind,
                        direction: mapping.direction,
                    },
                ))
            })
            .collect();
        let edges = edges?;

        match self.etl_type.as_str() {
            "table" => {
                let source = self.source.ok_or_else(|| {
                    OntologyError::Validation(
                        "ETL type 'table' requires a 'source' field".to_string(),
                    )
                })?;
                let watermark = self.watermark.ok_or_else(|| {
                    OntologyError::Validation(
                        "ETL type 'table' requires a 'watermark' field".to_string(),
                    )
                })?;
                let deleted = self.deleted.ok_or_else(|| {
                    OntologyError::Validation(
                        "ETL type 'table' requires a 'deleted' field".to_string(),
                    )
                })?;
                Ok(EtlConfig::Table {
                    scope,
                    source,
                    watermark,
                    deleted,
                    edges,
                })
            }
            "query" => {
                let query = self.query.ok_or_else(|| {
                    OntologyError::Validation(
                        "ETL type 'query' requires a 'query' field".to_string(),
                    )
                })?;
                Ok(EtlConfig::Query {
                    scope,
                    query,
                    edges,
                })
            }
            other => Err(OntologyError::Validation(format!(
                "invalid ETL type: '{}', expected 'table' or 'query'",
                other
            ))),
        }
    }
}

impl EdgeYaml {
    fn into_entities(self, relationship_kind: String) -> Vec<EdgeEntity> {
        self.variants
            .into_iter()
            .map(|v| EdgeEntity {
                relationship_kind: relationship_kind.clone(),
                source: v.from_node.id,
                source_kind: v.from_node.node_type,
                target: v.to_node.id,
                target_kind: v.to_node.node_type,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixtures_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("crates directory should exist")
            .parent()
            .expect("workspace root should exist")
            .join("fixtures/ontology")
    }

    #[test]
    fn test_load_ontology() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        // Verify ontology has some nodes and edges (don't check for specific entities)
        assert!(
            ontology.node_count() > 0,
            "ontology should have at least one node"
        );
        assert!(
            ontology.edge_count() > 0,
            "ontology should have at least one edge"
        );
    }

    #[test]
    fn test_load_embedded() {
        let embedded = Ontology::load_embedded().expect("should load embedded ontology");
        let from_dir = Ontology::load_from_dir(fixtures_dir()).expect("should load from dir");

        // Embedded and directory-loaded ontologies should be identical
        assert_eq!(embedded, from_dir);
    }

    #[test]
    fn test_getters_and_iterators() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        // get_node
        let user = ontology.get_node("User").expect("User should exist");
        assert_eq!(user.name, "User");
        let field_names: Vec<_> = user.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"username"));
        assert!(field_names.contains(&"email"));
        assert!(user.primary_keys.contains(&"id".to_string()));

        // get_edge
        let authored = ontology
            .get_edge("AUTHORED")
            .expect("AUTHORED should exist");
        assert!(!authored.is_empty());
        assert_eq!(authored[0].relationship_kind, "AUTHORED");

        // node iterators
        let names: Vec<_> = ontology.node_names().collect();
        assert!(names.contains(&"User"));
        assert!(names.contains(&"Project"));
        let nodes: Vec<_> = ontology.nodes().collect();
        assert_eq!(nodes.len(), ontology.node_count());

        // edge iterators
        let edge_names: Vec<_> = ontology.edge_names().collect();
        assert!(edge_names.contains(&"AUTHORED"));
        let edges: Vec<_> = ontology.edges().collect();
        assert!(!edges.is_empty(), "edges should return at least one edge");
    }

    #[test]
    fn test_display() {
        // Ontology display
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let display = format!("{ontology}");
        assert!(display.contains("nodes"));
        assert!(display.contains("edge types"));

        // NodeEntity display
        let user = ontology.get_node("User").expect("User should exist");
        assert!(format!("{user}").contains("User"));

        // DataType display
        assert_eq!(format!("{}", DataType::String), "String");
        assert_eq!(format!("{}", DataType::Int), "Int");
        assert_eq!(format!("{}", DataType::Date), "Date");
        assert_eq!(format!("{}", DataType::DateTime), "DateTime");

        // Field display
        let field = Field {
            name: "email".into(),
            source: "email".into(),
            data_type: DataType::String,
            nullable: true,
            enum_values: None,
        };
        assert_eq!(format!("{field}"), "email: String?");
        let field = Field {
            name: "id".into(),
            source: "id".into(),
            data_type: DataType::Int,
            nullable: false,
            enum_values: None,
        };
        assert_eq!(format!("{field}"), "id: Int");
    }

    #[test]
    fn test_determinism_and_equality() {
        let ontology1 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let ontology2 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        // Equality
        assert_eq!(ontology1, ontology2);

        // Deterministic order
        let names1: Vec<_> = ontology1.node_names().collect();
        let names2: Vec<_> = ontology2.node_names().collect();
        assert_eq!(names1, names2);

        let edge_names1: Vec<_> = ontology1.edge_names().collect();
        let edge_names2: Vec<_> = ontology2.edge_names().collect();
        assert_eq!(edge_names1, edge_names2);
    }

    // --- Builder method tests ---

    #[test]
    fn test_builder_methods() {
        let ontology = Ontology::new()
            .with_nodes(["User", "Project", "Note"])
            .with_edges(["AUTHORED", "CONTAINS"])
            .with_fields(
                "User",
                [("username", DataType::String), ("age", DataType::Int)],
            );

        // with_nodes
        assert_eq!(ontology.node_count(), 3);
        assert!(ontology.has_node("User"));
        assert!(ontology.has_node("Project"));
        assert!(ontology.has_node("Note"));
        assert!(!ontology.has_node("Group"));

        // with_edges
        assert_eq!(ontology.edge_count(), 2);
        assert!(ontology.has_edge("AUTHORED"));
        assert!(ontology.has_edge("CONTAINS"));
        assert!(!ontology.has_edge("MEMBER_OF"));

        // with_fields
        let user = ontology.get_node("User").unwrap();
        assert_eq!(user.fields.len(), 2);
        let field_names: Vec<_> = user.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"username"));
        assert!(field_names.contains(&"age"));
        let username_field = user.fields.iter().find(|f| f.name == "username").unwrap();
        assert_eq!(username_field.data_type, DataType::String);
    }

    #[test]
    #[should_panic(expected = "node \"NonExistent\" does not exist")]
    fn test_with_fields_panics_on_missing_node() {
        let _ = Ontology::new().with_fields("NonExistent", [("field", DataType::String)]);
    }

    // --- Validation method tests ---

    #[test]
    fn test_has_field() {
        let ontology = Ontology::new()
            .with_nodes(["User"])
            .with_fields("User", [("username", DataType::String)]);

        // Reserved columns on nodes (only "id")
        assert!(ontology.has_field("User", "id"));

        // Defined fields
        assert!(ontology.has_field("User", "username"));
        assert!(!ontology.has_field("User", "nonexistent"));

        // Unknown node returns false even for reserved columns
        assert!(!ontology.has_field("Unknown", "id"));
        assert!(!ontology.has_field("Unknown", "field"));
    }

    #[test]
    fn test_validate_field() {
        let ontology = Ontology::new()
            .with_nodes(["User"])
            .with_fields("User", [("username", DataType::String)]);

        // Reserved columns pass for existing nodes
        assert!(ontology.validate_field("User", "id").is_ok());

        // Defined fields pass
        assert!(ontology.validate_field("User", "username").is_ok());

        // Unknown node fails (even for reserved columns)
        let err = ontology.validate_field("Unknown", "id").unwrap_err();
        assert!(err.to_string().contains("unknown node type"));
        let err = ontology.validate_field("Unknown", "field").unwrap_err();
        assert!(err.to_string().contains("unknown node type"));

        // Unknown field fails
        let err = ontology.validate_field("User", "nonexistent").unwrap_err();
        assert!(err.to_string().contains("does not exist"));

        // Empty entity name fails
        let err = Ontology::new().validate_field("", "field").unwrap_err();
        assert!(err.to_string().contains("without an entity type"));
    }

    #[test]
    fn test_validate_type() {
        let ontology = Ontology::new()
            .with_nodes(["User"])
            .with_edges(["AUTHORED"]);

        // Valid node
        assert!(ontology.validate_type("User").is_ok());

        // Valid edge
        assert!(ontology.validate_type("AUTHORED").is_ok());

        // Invalid type
        let err = ontology.validate_type("Invalid").unwrap_err();
        assert!(err.to_string().contains("not a valid node label"));
    }

    #[test]
    fn test_table_name() {
        let ontology = Ontology::new().with_nodes(["User", "Project"]);

        // Valid nodes
        assert_eq!(ontology.table_name("User").unwrap(), "kg_user");
        assert_eq!(ontology.table_name("Project").unwrap(), "kg_project");

        // Unknown node
        let err = ontology.table_name("Unknown").unwrap_err();
        assert!(err.to_string().contains("unknown node label"));
    }

    // --- JSON Schema tests ---

    #[test]
    fn test_data_type_to_json_schema_type() {
        assert_eq!(DataType::String.to_json_schema_type(), "string");
        assert_eq!(DataType::Int.to_json_schema_type(), "integer");
        assert_eq!(DataType::Float.to_json_schema_type(), "number");
        assert_eq!(DataType::Bool.to_json_schema_type(), "boolean");
        assert_eq!(DataType::Date.to_json_schema_type(), "string");
        assert_eq!(DataType::DateTime.to_json_schema_type(), "string");
        assert_eq!(DataType::Enum.to_json_schema_type(), "string");
    }

    fn base_schema() -> &'static str {
        include_str!("../schema.json")
    }

    #[test]
    fn test_derive_json_schema() {
        let ontology = Ontology::new()
            .with_nodes(["User", "Project"])
            .with_edges(["AUTHORED"])
            .with_fields("User", [("username", DataType::String)]);

        let result = ontology.derive_json_schema(base_schema()).unwrap();

        // Check EntityType enum
        let labels = result["$defs"]["EntityType"]["enum"].as_array().unwrap();
        let label_strs: Vec<_> = labels.iter().filter_map(|v| v.as_str()).collect();
        assert!(label_strs.contains(&"User"));
        assert!(label_strs.contains(&"Project"));

        // Check RelationshipTypeName enum
        let types = result["$defs"]["RelationshipTypeName"]["enum"]
            .as_array()
            .unwrap();
        let type_strs: Vec<_> = types.iter().filter_map(|v| v.as_str()).collect();
        assert!(type_strs.contains(&"AUTHORED"));

        // Check NodeProperties
        let user_props = &result["$defs"]["NodeProperties"]["User"];
        assert!(user_props.is_object());
        assert_eq!(user_props["username"]["type"], "string");
    }

    #[test]
    fn test_derive_json_schema_errors() {
        let ontology = Ontology::new();

        // Invalid JSON
        let err = ontology.derive_json_schema("not valid json").unwrap_err();
        assert!(err.to_string().contains("failed to parse base schema"));

        // Missing $defs
        let err = ontology.derive_json_schema("{}").unwrap_err();
        assert!(err.to_string().contains("missing $defs"));
    }

    #[test]
    fn test_derive_json_schema_with_enum_field() {
        let mut enum_values = std::collections::BTreeMap::new();
        enum_values.insert(1, "active".to_string());
        enum_values.insert(2, "inactive".to_string());
        enum_values.insert(3, "pending".to_string());

        let node = NodeEntity {
            name: "User".to_string(),
            fields: vec![Field {
                name: "status".to_string(),
                source: "status".to_string(),
                data_type: DataType::Enum,
                nullable: false,
                enum_values: Some(enum_values),
            }],
            primary_keys: vec!["id".to_string()],
            destination_table: "kg_user".to_string(),
            etl: None,
        };

        let mut ontology = Ontology::new();
        ontology.nodes.insert("User".to_string(), node);

        let result = ontology.derive_json_schema(base_schema()).unwrap();

        let status_schema = &result["$defs"]["NodeProperties"]["User"]["status"];
        assert_eq!(status_schema["type"], "string");

        let enum_array = status_schema["enum"].as_array().unwrap();
        let enum_values: Vec<_> = enum_array.iter().filter_map(|v| v.as_str()).collect();
        assert_eq!(enum_values, vec!["active", "inactive", "pending"]);
    }
}

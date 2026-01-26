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
//! let ontology = Ontology::load_from_dir("fixtures/ontology")?;
//! let user = ontology.get_node("User").expect("User node exists");
//! ```

mod entities;

pub use entities::{DataType, EdgeEntity, Field, NodeEntity};

use serde::Deserialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

/// Primary key field name used by default.
const DEFAULT_PRIMARY_KEY: &str = "id";

/// Reserved columns that exist on all nodes and edges.
/// These are always valid for filtering/projection regardless of ontology definition.
pub const RESERVED_COLUMNS: &[&str] = &["id", "label", "from_id", "to_id", "type"];

/// Edge table name in ClickHouse.
pub const EDGE_TABLE: &str = "kg_edges";

/// Errors that can occur when loading an ontology.
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
                    name,
                    fields: vec![],
                    primary_keys: vec!["id".to_string()],
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

    /// Add fields to an existing node. Panics if the node doesn't exist.
    #[must_use]
    pub fn with_fields(
        mut self,
        node_name: &str,
        fields: impl IntoIterator<Item = (impl Into<String>, DataType)>,
    ) -> Self {
        let node = self
            .nodes
            .get_mut(node_name)
            .unwrap_or_else(|| panic!("node '{node_name}' does not exist"));
        for (field_name, data_type) in fields {
            node.fields.push(Field {
                name: field_name.into(),
                data_type,
                nullable: true,
            });
        }
        self
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
        let dir = dir.as_ref();
        let schema_path = dir.join("schema.yaml");
        let schema_content = read_file(&schema_path)?;
        let schema: SchemaYaml =
            parse_yaml(&schema_content, schema_path.to_string_lossy().as_ref())?;

        let mut ontology = Ontology::new();

        // Load nodes from each domain (BTreeMap ensures deterministic order)
        for domain in schema.domains.values() {
            for (node_name, node_path) in &domain.nodes {
                if ontology.nodes.contains_key(node_name) {
                    return Err(OntologyError::Validation(format!(
                        "duplicate node definition: '{}'",
                        node_name
                    )));
                }

                let node_file = dir.join(node_path);
                let content = read_file(&node_file)?;
                let node_def: NodeYaml =
                    parse_yaml(&content, node_file.to_string_lossy().as_ref())?;

                let entity = node_def.into_entity(node_name.clone())?;
                ontology.nodes.insert(node_name.clone(), entity);
            }
        }

        // Load edges
        for (edge_name, edge_path) in &schema.edges {
            let edge_file = dir.join(edge_path);
            let content = read_file(&edge_file)?;
            let edge_def: EdgeYaml = parse_yaml(&content, edge_file.to_string_lossy().as_ref())?;

            let entities = edge_def.into_entities(edge_name.clone());

            // Validate edge references
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
    /// - The field is a reserved column (`id`, `label`, etc.)
    /// - The field exists in the node's field definitions
    #[must_use]
    pub fn has_field(&self, node_name: &str, field_name: &str) -> bool {
        // Reserved columns exist on all nodes
        if RESERVED_COLUMNS.contains(&field_name) {
            return true;
        }

        // Check if the node exists and has this field
        self.nodes
            .get(node_name)
            .map(|node| node.fields.iter().any(|f| f.name == field_name))
            .unwrap_or(false)
    }

    /// Validate that a field exists on a node.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The node label is empty
    /// - The node doesn't exist in the ontology
    /// - The field doesn't exist on the node (and isn't a reserved column)
    pub fn validate_field(&self, node_name: &str, field_name: &str) -> Result<(), OntologyError> {
        // Reserved columns exist on all nodes
        if RESERVED_COLUMNS.contains(&field_name) {
            return Ok(());
        }

        if node_name.is_empty() {
            return Err(OntologyError::Validation(format!(
                "cannot validate field \"{field_name}\" without a node label"
            )));
        }

        let node = self.nodes.get(node_name).ok_or_else(|| {
            OntologyError::Validation(format!("unknown node type \"{node_name}\""))
        })?;

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
            "type \"{type_name}\" is not a valid node label or relationship type"
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
                "unknown node label: {node_label}"
            )));
        }
        Ok(format!("kg_{}", node_label.to_lowercase()))
    }

    /// Generate a JSON Schema with ontology values populated.
    ///
    /// Given a base schema template, this populates:
    /// - `$defs.NodeLabel.enum` with valid node labels
    /// - `$defs.RelationshipTypeName.enum` with valid relationship types
    /// - `$defs.NodeProperties` with property definitions per node type
    ///
    /// # Errors
    ///
    /// Returns an error if the base schema is invalid JSON or missing required sections.
    pub fn derive_json_schema(&self, base_schema_json: &str) -> Result<Value, OntologyError> {
        let mut schema: Value = serde_json::from_str(base_schema_json).map_err(|e| {
            OntologyError::Validation(format!("failed to parse base schema: {e}"))
        })?;

        let defs = schema
            .get_mut("$defs")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| OntologyError::Validation("schema missing $defs".into()))?;

        // Populate NodeLabel enum
        if let Some(node_label) = defs.get_mut("NodeLabel").and_then(Value::as_object_mut) {
            let labels: Vec<Value> = self
                .node_names()
                .map(|s| Value::String(s.to_string()))
                .collect();
            node_label.insert("enum".to_string(), Value::Array(labels));
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
                    Value::String(field.data_type.to_json_schema_type()),
                );
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

// --- Helper functions ---

fn read_file(path: &Path) -> Result<String, OntologyError> {
    std::fs::read_to_string(path).map_err(|e| OntologyError::Io {
        path: path.to_string_lossy().to_string(),
        source: e,
    })
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
        "string" | "enum" => Ok(DataType::String),
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
    #[serde(default)]
    properties: BTreeMap<String, PropertyYaml>,
    #[serde(default)]
    additional_properties: BTreeMap<String, PropertyYaml>,
}

#[derive(Debug, Deserialize)]
struct PropertyYaml {
    #[serde(rename = "type")]
    property_type: String,
    #[serde(default)]
    nullable: bool,
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

        // Chain properties and additional_properties for single-pass collection
        let fields: Result<Vec<Field>, OntologyError> = self
            .properties
            .into_iter()
            .chain(self.additional_properties)
            .map(|(prop_name, prop_def)| {
                if prop_name == DEFAULT_PRIMARY_KEY {
                    primary_keys.push(prop_name.clone());
                }
                Ok(Field {
                    data_type: parse_data_type(&prop_def.property_type, &prop_name)?,
                    name: prop_name,
                    nullable: prop_def.nullable,
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

        Ok(NodeEntity {
            name,
            fields,
            primary_keys,
        })
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

        assert!(ontology.node_count() > 0);
        assert!(ontology.edge_count() > 0);

        // Check we have expected nodes
        assert!(ontology.has_node("User"));
        assert!(ontology.has_node("Group"));
        assert!(ontology.has_node("Project"));
        assert!(ontology.has_node("Note"));

        // Check we have expected edges
        assert!(ontology.has_edge("AUTHORED"));
        assert!(ontology.has_edge("CONTAINS"));
    }

    #[test]
    fn test_get_node() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let user = ontology.get_node("User").expect("User should exist");
        assert_eq!(user.name, "User");

        let field_names: Vec<_> = user.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"username"));
        assert!(field_names.contains(&"email"));
        assert!(user.primary_keys.contains(&"id".to_string()));
    }

    #[test]
    fn test_get_edge() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let authored = ontology
            .get_edge("AUTHORED")
            .expect("AUTHORED should exist");
        assert!(!authored.is_empty());

        let first = &authored[0];
        assert_eq!(first.relationship_kind, "AUTHORED");
    }

    #[test]
    fn test_node_iterators() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let names: Vec<_> = ontology.node_names().collect();
        assert!(names.contains(&"User"));
        assert!(names.contains(&"Project"));

        let nodes: Vec<_> = ontology.nodes().collect();
        assert_eq!(nodes.len(), ontology.node_count());
    }

    #[test]
    fn test_edge_iterators() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let names: Vec<_> = ontology.edge_names().collect();
        assert!(names.contains(&"AUTHORED"));

        let edges: Vec<_> = ontology.edges().collect();
        assert!(!edges.is_empty());
    }

    #[test]
    fn test_display() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let display = format!("{}", ontology);
        assert!(display.contains("nodes"));
        assert!(display.contains("edge types"));

        let user = ontology.get_node("User").expect("User should exist");
        let display = format!("{}", user);
        assert!(display.contains("User"));
    }

    #[test]
    fn test_data_type_display() {
        assert_eq!(format!("{}", DataType::String), "String");
        assert_eq!(format!("{}", DataType::Int), "Int");
        assert_eq!(format!("{}", DataType::Date), "Date");
        assert_eq!(format!("{}", DataType::DateTime), "DateTime");
    }

    #[test]
    fn test_field_display() {
        let field = Field {
            name: "email".to_string(),
            data_type: DataType::String,
            nullable: true,
        };
        assert_eq!(format!("{}", field), "email: String?");

        let field = Field {
            name: "id".to_string(),
            data_type: DataType::Int,
            nullable: false,
        };
        assert_eq!(format!("{}", field), "id: Int");
    }

    #[test]
    fn test_deterministic_order() {
        // Load twice and verify order is consistent
        let ontology1 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let ontology2 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let names1: Vec<_> = ontology1.node_names().collect();
        let names2: Vec<_> = ontology2.node_names().collect();
        assert_eq!(names1, names2);

        let edge_names1: Vec<_> = ontology1.edge_names().collect();
        let edge_names2: Vec<_> = ontology2.edge_names().collect();
        assert_eq!(edge_names1, edge_names2);
    }

    #[test]
    fn test_equality() {
        let ontology1 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let ontology2 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        assert_eq!(ontology1, ontology2);
    }
}

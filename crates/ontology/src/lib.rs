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
//! let ontology = Ontology::load_from_dir("config/ontology")?;
//! let user = ontology.get_node("User").expect("User node exists");
//! ```

pub mod constants;
mod entities;
pub mod etl;
mod json_schema;
mod loading;

pub use constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, EDGE_RESERVED_COLUMNS, EDGE_TABLE, GL_TABLE_PREFIX,
    NODE_RESERVED_COLUMNS, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
pub use entities::{
    DataType, DomainInfo, EdgeColumn, EdgeEndpoint, EdgeEndpointType, EdgeEntity,
    EdgeSourceEtlConfig, EnumType, Field, FieldSource, NodeEntity, NodeStyle, RedactionConfig,
    VirtualSource,
};
pub use etl::{EdgeDirection, EdgeMapping, EdgeTarget, EtlConfig, EtlScope};

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use loading::EtlSettings;

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
    schema_version: String,
    /// Prefix for all ClickHouse graph table names (e.g., `"gl_"`).
    pub(crate) table_prefix: String,
    /// ClickHouse table name for all graph edges (e.g., `"gl_edge"`).
    pub(crate) edge_table: String,
    /// Default ORDER BY columns for node tables (dedup key for ReplacingMergeTree).
    pub(crate) default_entity_sort_key: Vec<String>,
    /// ORDER BY columns for the edge table (dedup key for ReplacingMergeTree).
    pub(crate) edge_sort_key: Vec<String>,
    pub(crate) edge_columns: Vec<EdgeColumn>,
    pub(crate) domains: BTreeMap<String, DomainInfo>,
    pub(crate) nodes: BTreeMap<String, NodeEntity>,
    pub(crate) edges: BTreeMap<String, Vec<EdgeEntity>>,
    pub(crate) edge_descriptions: BTreeMap<String, String>,
    /// ETL configs for edges sourced from join tables (keyed by relationship kind).
    pub(crate) edge_etl_configs: BTreeMap<String, EdgeSourceEtlConfig>,
    pub(crate) etl_settings: EtlSettings,
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
            schema_version: String::new(),
            table_prefix: GL_TABLE_PREFIX.to_string(),
            edge_table: EDGE_TABLE.to_string(),
            default_entity_sort_key: vec![
                TRAVERSAL_PATH_COLUMN.to_string(),
                DEFAULT_PRIMARY_KEY.to_string(),
            ],
            edge_sort_key: EDGE_RESERVED_COLUMNS
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            edge_columns: Vec::new(),
            domains: BTreeMap::new(),
            nodes: BTreeMap::new(),
            edges: BTreeMap::new(),
            edge_descriptions: BTreeMap::new(),
            edge_etl_configs: BTreeMap::new(),
            etl_settings: EtlSettings {
                watermark: "_siphon_replicated_at".to_string(),
                deleted: "_siphon_deleted".to_string(),
                order_by: vec![
                    TRAVERSAL_PATH_COLUMN.to_string(),
                    DEFAULT_PRIMARY_KEY.to_string(),
                ],
            },
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
                    destination_table: format!("{}{}", self.table_prefix, name.to_lowercase()),
                    sort_key: self.default_entity_sort_key.clone(),
                    ..Default::default()
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

    /// Builder: set edge columns (for testing).
    #[must_use]
    pub fn with_edge_columns(
        mut self,
        columns: impl IntoIterator<Item = (impl Into<String>, DataType)>,
    ) -> Self {
        self.edge_columns = columns
            .into_iter()
            .map(|(name, data_type)| EdgeColumn {
                name: name.into(),
                data_type,
            })
            .collect();
        self
    }

    /// Columns of the unified edge table, in schema order.
    #[must_use]
    pub fn edge_columns(&self) -> &[EdgeColumn] {
        &self.edge_columns
    }

    /// Look up the `DataType` of an edge table column by name.
    #[must_use]
    pub fn get_edge_column_type(&self, name: &str) -> Option<DataType> {
        self.edge_columns
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.data_type)
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
            if field_name_string == constants::TRAVERSAL_PATH_COLUMN {
                node.has_traversal_path = true;
            }
            node.fields.push(Field {
                name: field_name_string.clone(),
                source: FieldSource::DatabaseColumn(field_name_string),
                data_type,
                nullable,
                ..Default::default()
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

    /// Set `default_columns` for an existing node.
    ///
    /// # Panics
    ///
    /// Panics if the node doesn't exist or if any column name is not a declared field.
    #[must_use]
    pub fn with_default_columns(
        mut self,
        node_name: &str,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let node = self
            .nodes
            .get_mut(node_name)
            .unwrap_or_else(|| panic!("node \"{node_name}\" does not exist"));
        node.default_columns = columns.into_iter().map(Into::into).collect();
        let field_names: std::collections::HashSet<&str> =
            node.fields.iter().map(|f| f.name.as_str()).collect();
        for col in &node.default_columns {
            assert!(
                field_names.contains(col.as_str()),
                "default_columns entry '{col}' is not a declared field of node '{node_name}'"
            );
        }
        self
    }

    /// Mutate a field on a node.
    ///
    /// # Errors
    ///
    /// Returns an error if the node or field doesn't exist.
    pub fn modify_field(
        mut self,
        node_name: &str,
        field_name: &str,
        f: impl FnOnce(&mut Field),
    ) -> Result<Self, OntologyError> {
        let field = self.get_field_mut(node_name, field_name)?;
        f(field);
        Ok(self)
    }

    fn get_field_mut(
        &mut self,
        node_name: &str,
        field_name: &str,
    ) -> Result<&mut Field, OntologyError> {
        let node = self.nodes.get_mut(node_name).ok_or_else(|| {
            OntologyError::Validation(format!("node \"{node_name}\" does not exist"))
        })?;
        node.fields
            .iter_mut()
            .find(|f| f.name == field_name)
            .ok_or_else(|| {
                OntologyError::Validation(format!(
                    "field \"{field_name}\" not found on \"{node_name}\""
                ))
            })
    }

    /// Builder: set redaction config for a node (for testing).
    #[must_use]
    pub fn with_redaction(
        mut self,
        node_name: &str,
        resource_type: impl Into<String>,
        id_column: impl Into<String>,
    ) -> Self {
        let node = self
            .nodes
            .get_mut(node_name)
            .unwrap_or_else(|| panic!("node \"{node_name}\" does not exist"));
        node.redaction = Some(RedactionConfig {
            resource_type: resource_type.into(),
            id_column: id_column.into(),
            ability: "read".to_string(),
        });
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
        loading::load_from_dir(dir.as_ref())
    }

    /// Load ontology from embedded files compiled into the binary.
    ///
    /// This uses the ontology files from `config/ontology/` that were
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
        loading::load_embedded()
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

    /// Get all source node types for an edge relationship.
    ///
    /// Returns unique node types that can be the source of this relationship.
    pub fn get_edge_source_types(&self, relationship_kind: &str) -> Vec<String> {
        let Some(variants) = self.get_edge(relationship_kind) else {
            return Vec::new();
        };

        variants
            .iter()
            .map(|edge| edge.source_kind.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Get all target node types for an edge relationship.
    ///
    /// Returns unique node types that can be the target of this relationship.
    pub fn get_edge_all_target_types(&self, relationship_kind: &str) -> Vec<String> {
        let Some(variants) = self.get_edge(relationship_kind) else {
            return Vec::new();
        };

        variants
            .iter()
            .map(|edge| edge.target_kind.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
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

    /// Get the redaction config for an entity, if it requires redaction.
    #[must_use]
    pub fn get_redaction_config(&self, entity_name: &str) -> Option<&RedactionConfig> {
        self.get_node(entity_name)?.redaction.as_ref()
    }

    /// Check if an entity requires redaction validation.
    #[must_use]
    pub fn requires_redaction(&self, entity_name: &str) -> bool {
        self.get_redaction_config(entity_name).is_some()
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

    #[must_use]
    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    /// Table name prefix for all ClickHouse graph tables.
    #[must_use]
    pub fn table_prefix(&self) -> &str {
        &self.table_prefix
    }

    /// ClickHouse table name for all graph edges.
    #[must_use]
    pub fn edge_table(&self) -> &str {
        &self.edge_table
    }

    /// Default ORDER BY / dedup key columns for node tables.
    #[must_use]
    pub fn default_entity_sort_key(&self) -> &[String] {
        &self.default_entity_sort_key
    }

    /// ORDER BY / dedup key columns for the edge table.
    #[must_use]
    pub fn edge_sort_key(&self) -> &[String] {
        &self.edge_sort_key
    }

    /// Look up the dedup key (ORDER BY columns) for a ClickHouse table name.
    ///
    /// Returns the node's `sort_key` for node tables, the `edge_sort_key` for
    /// the edge table, or `None` if the table is unknown.
    #[must_use]
    pub fn sort_key_for_table(&self, table: &str) -> Option<&[String]> {
        if table == self.edge_table {
            return Some(&self.edge_sort_key);
        }
        self.nodes
            .values()
            .find(|n| n.destination_table == table)
            .map(|n| n.sort_key.as_slice())
    }

    pub fn domains(&self) -> impl Iterator<Item = &DomainInfo> {
        self.domains.values()
    }

    #[must_use]
    pub fn get_edge_description(&self, name: &str) -> Option<&str> {
        self.edge_descriptions.get(name).map(|s| s.as_str())
    }

    /// Get ETL config for an edge by relationship kind.
    ///
    /// Returns `Some` only for edges sourced from join tables.
    pub fn get_edge_etl(&self, relationship_kind: &str) -> Option<&EdgeSourceEtlConfig> {
        self.edge_etl_configs.get(relationship_kind)
    }

    /// Check if an edge has ETL config (i.e., is sourced from a join table).
    pub fn has_edge_etl(&self, relationship_kind: &str) -> bool {
        self.edge_etl_configs.contains_key(relationship_kind)
    }

    /// Iterator over all edge ETL configs (relationship_kind, config).
    pub fn edge_etl_configs(&self) -> impl Iterator<Item = (&str, &EdgeSourceEtlConfig)> {
        self.edge_etl_configs.iter().map(|(k, v)| (k.as_str(), v))
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
        let Some(node) = self.nodes.get(node_name) else {
            return false;
        };

        if NODE_RESERVED_COLUMNS.contains(&field_name) {
            return true;
        }

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

    /// Look up a field's [`DataType`] on a node.
    ///
    /// Returns `None` if the node or field doesn't exist. Reserved columns
    /// listed in [`NODE_RESERVED_COLUMNS`] (currently just `id`) are always
    /// `DataType::Int`.
    #[must_use]
    pub fn get_field_type(&self, node_name: &str, field_name: &str) -> Option<DataType> {
        if NODE_RESERVED_COLUMNS.contains(&field_name) {
            return Some(DataType::Int);
        }
        let node = self.nodes.get(node_name)?;
        node.fields
            .iter()
            .find(|f| f.name == field_name)
            .map(|f| f.data_type)
    }

    /// Check a boolean property on a node field.
    ///
    /// Returns `true` for reserved columns (e.g. `id`). Returns `false` for
    /// unknown fields (fail-closed). Unknown nodes return `true` since edge
    /// filters pass entity names like `"relationship[0]"`.
    #[must_use]
    pub fn check_field_flag(
        &self,
        node_name: &str,
        field_name: &str,
        flag: impl Fn(&Field) -> bool,
    ) -> bool {
        if NODE_RESERVED_COLUMNS.contains(&field_name) {
            return true;
        }
        let Some(node) = self.nodes.get(node_name) else {
            return true;
        };
        node.fields
            .iter()
            .find(|f| f.name == field_name)
            .is_some_and(&flag)
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
    /// Returns the `destination_table` from the node's ontology definition.
    ///
    /// # Errors
    ///
    /// Returns an error if the node label is unknown.
    pub fn table_name(&self, node_label: &str) -> Result<&str, OntologyError> {
        let node = self.nodes.get(node_label).ok_or_else(|| {
            OntologyError::Validation(format!("unknown node label \"{node_label}\""))
        })?;
        Ok(&node.destination_table)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::loading::{EtlSettings, NodeYaml, ReadOntologyFile, load_with};

    fn fixtures_dir() -> std::path::PathBuf {
        Path::new(env!("ONTOLOGY_DIR")).to_path_buf()
    }

    #[test]
    fn test_load_ontology() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

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

        assert_eq!(embedded, from_dir);
    }

    #[test]
    fn test_getters_and_iterators() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let user = ontology.get_node("User").expect("User should exist");
        assert_eq!(user.name, "User");
        let field_names: Vec<_> = user.fields.iter().map(|f| f.name.as_str()).collect();
        assert!(field_names.contains(&"id"));
        assert!(field_names.contains(&"username"));
        assert!(field_names.contains(&"email"));
        assert!(user.primary_keys.contains(&"id".to_string()));

        let authored = ontology
            .get_edge("AUTHORED")
            .expect("AUTHORED should exist");
        assert!(!authored.is_empty());
        assert_eq!(authored[0].relationship_kind, "AUTHORED");

        let names: Vec<_> = ontology.node_names().collect();
        assert!(names.contains(&"User"));
        assert!(names.contains(&"Project"));
        let nodes: Vec<_> = ontology.nodes().collect();
        assert_eq!(nodes.len(), ontology.node_count());

        let edge_names: Vec<_> = ontology.edge_names().collect();
        assert!(edge_names.contains(&"AUTHORED"));
        let edges: Vec<_> = ontology.edges().collect();
        assert!(!edges.is_empty(), "edges should return at least one edge");
    }

    #[test]
    fn test_display() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let display = format!("{ontology}");
        assert!(display.contains("nodes"));
        assert!(display.contains("edge types"));

        let user = ontology.get_node("User").expect("User should exist");
        assert!(format!("{user}").contains("User"));

        assert_eq!(format!("{}", DataType::String), "String");
        assert_eq!(format!("{}", DataType::Int), "Int");
        assert_eq!(format!("{}", DataType::Date), "Date");
        assert_eq!(format!("{}", DataType::DateTime), "DateTime");
        assert_eq!(format!("{}", DataType::Uuid), "Uuid");

        let field = Field {
            name: "email".into(),
            source: FieldSource::DatabaseColumn("email".into()),
            data_type: DataType::String,
            nullable: true,
            ..Default::default()
        };
        assert_eq!(format!("{field}"), "email: String?");
        let field = Field {
            name: "id".into(),
            source: FieldSource::DatabaseColumn("id".into()),
            data_type: DataType::Int,
            ..Default::default()
        };
        assert_eq!(format!("{field}"), "id: Int");
    }

    #[test]
    fn test_determinism_and_equality() {
        let ontology1 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let ontology2 = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_eq!(ontology1, ontology2);

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

        assert_eq!(ontology.node_count(), 3);
        assert!(ontology.has_node("User"));
        assert!(ontology.has_node("Project"));
        assert!(ontology.has_node("Note"));
        assert!(!ontology.has_node("Group"));

        assert_eq!(ontology.edge_count(), 2);
        assert!(ontology.has_edge("AUTHORED"));
        assert!(ontology.has_edge("CONTAINS"));
        assert!(!ontology.has_edge("MEMBER_OF"));

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

        assert!(ontology.has_field("User", "id"));
        assert!(ontology.has_field("User", "username"));
        assert!(!ontology.has_field("User", "nonexistent"));
        assert!(!ontology.has_field("Unknown", "id"));
        assert!(!ontology.has_field("Unknown", "field"));
    }

    #[test]
    fn test_validate_field() {
        let ontology = Ontology::new()
            .with_nodes(["User"])
            .with_fields("User", [("username", DataType::String)]);

        assert!(ontology.validate_field("User", "id").is_ok());
        assert!(ontology.validate_field("User", "username").is_ok());

        let err = ontology.validate_field("Unknown", "id").unwrap_err();
        assert!(err.to_string().contains("unknown node type"));
        let err = ontology.validate_field("Unknown", "field").unwrap_err();
        assert!(err.to_string().contains("unknown node type"));

        let err = ontology.validate_field("User", "nonexistent").unwrap_err();
        assert!(err.to_string().contains("does not exist"));

        let err = Ontology::new().validate_field("", "field").unwrap_err();
        assert!(err.to_string().contains("without an entity type"));
    }

    #[test]
    fn test_validate_type() {
        let ontology = Ontology::new()
            .with_nodes(["User"])
            .with_edges(["AUTHORED"]);

        assert!(ontology.validate_type("User").is_ok());
        assert!(ontology.validate_type("AUTHORED").is_ok());

        let err = ontology.validate_type("Invalid").unwrap_err();
        assert!(err.to_string().contains("not a valid node label"));
    }

    #[test]
    fn test_table_name() {
        let ontology = Ontology::new().with_nodes(["User", "Project"]);

        assert_eq!(ontology.table_name("User").unwrap(), "gl_user");
        assert_eq!(ontology.table_name("Project").unwrap(), "gl_project");

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
        assert_eq!(DataType::Uuid.to_json_schema_type(), "string");
    }

    fn base_schema() -> &'static str {
        include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"))
    }

    #[test]
    fn test_derive_json_schema() {
        let ontology = Ontology::new()
            .with_nodes(["User", "Project"])
            .with_edges(["AUTHORED"])
            .with_fields("User", [("username", DataType::String)]);

        let result = ontology.derive_json_schema(base_schema()).unwrap();

        let labels = result["$defs"]["EntityType"]["enum"].as_array().unwrap();
        let label_strs: Vec<_> = labels.iter().filter_map(|v| v.as_str()).collect();
        assert!(label_strs.contains(&"User"));
        assert!(label_strs.contains(&"Project"));

        let types = result["$defs"]["RelationshipTypeName"]["enum"]
            .as_array()
            .unwrap();
        let type_strs: Vec<_> = types.iter().filter_map(|v| v.as_str()).collect();
        assert!(type_strs.contains(&"AUTHORED"));

        let user_props = &result["$defs"]["NodeProperties"]["User"];
        assert!(user_props.is_object());
        assert_eq!(user_props["username"]["type"], "string");
    }

    #[test]
    fn test_derive_json_schema_errors() {
        let ontology = Ontology::new();

        let err = ontology.derive_json_schema("not valid json").unwrap_err();
        assert!(err.to_string().contains("failed to parse base schema"));

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
            domain: "core".to_string(),
            label: "username".to_string(),
            fields: vec![Field {
                name: "status".to_string(),
                source: FieldSource::DatabaseColumn("status".to_string()),
                data_type: DataType::Enum,
                nullable: false,
                enum_values: Some(enum_values),
                enum_type: EnumType::Int,
                ..Default::default()
            }],
            destination_table: "gl_user".to_string(),
            ..Default::default()
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

    fn assert_redaction(
        ontology: &Ontology,
        entity: &str,
        resource_type: &str,
        id_column: &str,
        ability: &str,
    ) {
        let config = ontology
            .get_redaction_config(entity)
            .unwrap_or_else(|| panic!("{entity} should have redaction config"));
        assert_eq!(
            config.resource_type, resource_type,
            "{entity}: resource_type mismatch"
        );
        assert_eq!(config.id_column, id_column, "{entity}: id_column mismatch");
        assert_eq!(config.ability, ability, "{entity}: ability mismatch");
    }

    #[test]
    fn redaction_config_core_nodes() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_redaction(&ontology, "Project", "project", "id", "read_project");
        assert_redaction(&ontology, "Group", "group", "id", "read_group");
        assert_redaction(&ontology, "User", "user", "id", "read_user");
        assert_redaction(&ontology, "Note", "note", "id", "read_note");
    }

    #[test]
    fn redaction_config_plan_nodes() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_redaction(&ontology, "WorkItem", "work_item", "id", "read_work_item");
        assert_redaction(&ontology, "Milestone", "milestone", "id", "read_milestone");
        assert_redaction(&ontology, "Label", "label", "id", "read_label");
    }

    #[test]
    fn redaction_config_code_review_nodes() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_redaction(
            &ontology,
            "MergeRequest",
            "merge_request",
            "id",
            "read_merge_request",
        );
        assert_redaction(
            &ontology,
            "MergeRequestDiff",
            "merge_request",
            "merge_request_id",
            "read_merge_request",
        );
        assert_redaction(
            &ontology,
            "MergeRequestDiffFile",
            "merge_request",
            "merge_request_id",
            "read_merge_request",
        );
    }

    #[test]
    fn redaction_config_ci_nodes() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_redaction(&ontology, "Pipeline", "ci_pipeline", "id", "read_pipeline");
        assert_redaction(&ontology, "Stage", "ci_stage", "id", "read_build");
        assert_redaction(&ontology, "Job", "ci_build", "id", "read_build");
    }

    #[test]
    fn redaction_config_security_nodes() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_redaction(
            &ontology,
            "Vulnerability",
            "vulnerability",
            "id",
            "read_vulnerability",
        );
        assert_redaction(
            &ontology,
            "VulnerabilityOccurrence",
            "vulnerability_occurrence",
            "id",
            "read_vulnerability",
        );
        assert_redaction(
            &ontology,
            "VulnerabilityScanner",
            "vulnerability_scanner",
            "id",
            "read_vulnerability_scanner",
        );
        assert_redaction(
            &ontology,
            "VulnerabilityIdentifier",
            "vulnerability_identifier",
            "id",
            "read_vulnerability",
        );
        assert_redaction(
            &ontology,
            "Finding",
            "security_finding",
            "id",
            "read_security_resource",
        );
        assert_redaction(
            &ontology,
            "SecurityScan",
            "security_scan",
            "id",
            "read_scan",
        );
    }

    #[test]
    fn redaction_config_source_code_nodes() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_redaction(&ontology, "Branch", "project", "project_id", "read_code");
        assert_redaction(&ontology, "File", "project", "project_id", "read_code");
        assert_redaction(&ontology, "Directory", "project", "project_id", "read_code");
        assert_redaction(
            &ontology,
            "Definition",
            "project",
            "project_id",
            "read_code",
        );
        assert_redaction(
            &ontology,
            "ImportedSymbol",
            "project",
            "project_id",
            "read_code",
        );
    }

    // --- sort_key tests ---

    #[test]
    fn sort_key_settings_are_non_empty() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert!(
            !ontology.default_entity_sort_key().is_empty(),
            "default_entity_sort_key should be non-empty"
        );
        assert!(
            !ontology.edge_sort_key().is_empty(),
            "edge_sort_key should be non-empty"
        );
    }

    #[test]
    fn sort_key_every_node_has_non_empty_key() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        for node in ontology.nodes() {
            assert!(
                !node.sort_key.is_empty(),
                "{} should have a non-empty sort_key",
                node.name
            );
        }
    }

    #[test]
    fn sort_key_most_nodes_inherit_default() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let default = ontology.default_entity_sort_key();

        let (inheriting, overriding): (Vec<_>, Vec<_>) = ontology
            .nodes()
            .partition(|n| n.sort_key.as_slice() == default);

        assert!(
            inheriting.len() > overriding.len(),
            "most nodes should inherit the default sort_key, \
             but got {} inheriting vs {} overriding",
            inheriting.len(),
            overriding.len()
        );

        for node in &overriding {
            assert_ne!(
                node.sort_key.as_slice(),
                default,
                "{} is in the override set but equals default",
                node.name
            );
        }
    }

    #[test]
    fn sort_key_for_table_resolves_every_node() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        for node in ontology.nodes() {
            let key = ontology.sort_key_for_table(&node.destination_table);
            assert_eq!(
                key,
                Some(node.sort_key.as_slice()),
                "sort_key_for_table({}) should return the node's sort_key",
                node.destination_table
            );
        }
    }

    #[test]
    fn sort_key_for_table_resolves_edge_table() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_eq!(
            ontology.sort_key_for_table(ontology.edge_table()),
            Some(ontology.edge_sort_key())
        );
    }

    #[test]
    fn sort_key_for_table_returns_none_for_unknown() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        assert_eq!(ontology.sort_key_for_table("nonexistent_table"), None);
    }

    #[test]
    fn sort_key_with_nodes_builder_inherits_default() {
        let ontology = Ontology::new().with_nodes(["Foo", "Bar"]);

        let default_key = ontology.default_entity_sort_key().to_vec();
        for name in &["Foo", "Bar"] {
            let node = ontology
                .get_node(name)
                .unwrap_or_else(|| panic!("{name} should exist"));
            assert_eq!(
                node.sort_key, default_key,
                "builder-created {name} should inherit default_entity_sort_key"
            );
        }
    }

    #[test]
    fn sort_key_node_yaml_explicit_overrides_default() {
        let yaml = r#"
node_type: TestNode
domain: test
description: A test node
label: name
destination_table: gl_test
sort_key: [project_id, branch, id]
properties:
  id:
    type: int64
    source: id
    nullable: false
    description: "ID"
  name:
    type: string
    source: name
    nullable: false
    description: "Name"
"#;
        let node_def: NodeYaml = serde_yaml::from_str(yaml).expect("valid YAML");
        let default_sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let etl_settings = EtlSettings {
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let entity = node_def
            .into_entity("TestNode".to_string(), &default_sort_key, &etl_settings)
            .expect("should succeed");
        assert_eq!(entity.sort_key, vec!["project_id", "branch", "id"]);
    }

    #[test]
    fn sort_key_node_yaml_absent_inherits_default() {
        let yaml = r#"
node_type: TestNode
domain: test
description: A test node
label: name
destination_table: gl_test
properties:
  id:
    type: int64
    source: id
    nullable: false
    description: "ID"
  name:
    type: string
    source: name
    nullable: false
    description: "Name"
"#;
        let node_def: NodeYaml = serde_yaml::from_str(yaml).expect("valid YAML");
        let default_sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let etl_settings = EtlSettings {
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let entity = node_def
            .into_entity("TestNode".to_string(), &default_sort_key, &etl_settings)
            .expect("should succeed");
        assert_eq!(entity.sort_key, default_sort_key);
    }

    #[test]
    fn destination_table_must_match_table_prefix() {
        use std::collections::HashMap;

        struct MockReader(HashMap<String, String>);
        impl ReadOntologyFile for MockReader {
            fn read(&self, path: &str) -> Result<String, OntologyError> {
                self.0.get(path).cloned().ok_or_else(|| OntologyError::Io {
                    path: path.to_string(),
                    source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
                })
            }
        }

        let mut files = HashMap::new();
        files.insert(
            "schema.yaml".to_string(),
            r#"
schema_version: "1.0"
settings:
  table_prefix: "kg_"
  edge_table: "kg_edge"
  default_entity_sort_key: [traversal_path, id]
  edge_sort_key: [traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind]
  edge_columns:
    - {name: traversal_path, type: string}
    - {name: relationship_kind, type: string}
    - {name: source_id, type: int64}
    - {name: source_kind, type: string}
    - {name: target_id, type: int64}
    - {name: target_kind, type: string}
  etl:
    default_watermark: _siphon_replicated_at
    default_deleted: _siphon_deleted
    default_etl_order_by: [traversal_path, id]
domains:
  core:
    nodes:
      User: nodes/core/user.yaml
edges: {}
"#
            .to_string(),
        );
        files.insert(
            "nodes/core/user.yaml".to_string(),
            r##"
node_type: User
domain: core
description: A user
label: username
destination_table: gl_user
properties:
  id:
    type: int64
    source: id
    nullable: false
    description: "ID"
  username:
    type: string
    source: username
    nullable: false
    description: "Username"
primary_keys: [id]
redaction:
  resource_type: user
  id_column: id
  ability: read_user
style:
  size: 30
  color: "#6B7280"
"##
            .to_string(),
        );

        let err = load_with(&MockReader(files)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("gl_user") && msg.contains("kg_"),
            "error should mention both the bad table and expected prefix, got: {msg}"
        );
    }

    #[test]
    fn all_nodes_require_redaction() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let expected = [
            "Project",
            "Group",
            "User",
            "Note",
            "WorkItem",
            "Milestone",
            "Label",
            "MergeRequest",
            "MergeRequestDiff",
            "MergeRequestDiffFile",
            "Pipeline",
            "Stage",
            "Job",
            "Vulnerability",
            "VulnerabilityOccurrence",
            "VulnerabilityScanner",
            "VulnerabilityIdentifier",
            "Finding",
            "SecurityScan",
            "Branch",
            "File",
            "Directory",
            "Definition",
            "ImportedSymbol",
        ];

        for entity in &expected {
            assert!(
                ontology.requires_redaction(entity),
                "{entity} should require redaction"
            );
        }
    }

    // --- default_columns tests ---

    #[test]
    fn default_columns_loaded_from_ontology() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let user = ontology.get_node("User").expect("User should exist");
        assert_eq!(
            user.default_columns,
            vec!["id", "username", "name", "state"]
        );

        let mr = ontology
            .get_node("MergeRequest")
            .expect("MergeRequest should exist");
        assert_eq!(
            mr.default_columns,
            vec![
                "id",
                "iid",
                "title",
                "state",
                "source_branch",
                "target_branch"
            ]
        );

        let label = ontology.get_node("Label").expect("Label should exist");
        assert_eq!(label.default_columns, vec!["id", "title", "color"]);

        let definition = ontology
            .get_node("Definition")
            .expect("Definition should exist");
        assert_eq!(
            definition.default_columns,
            vec!["id", "name", "fqn", "definition_type", "file_path"]
        );
    }

    #[test]
    fn all_nodes_have_default_columns() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        for node in ontology.nodes() {
            assert!(
                !node.default_columns.is_empty(),
                "{} should have default_columns defined",
                node.name
            );
        }
    }

    #[test]
    fn default_columns_rejects_unknown_property() {
        let yaml = r#"
node_type: TestNode
domain: test
description: A test node
label: name
destination_table: gl_test
default_columns: [id, nonexistent_field]
properties:
  id:
    type: int64
    source: id
    nullable: false
    description: "ID"
  name:
    type: string
    source: name
    nullable: false
    description: "Name"
"#;
        let node_def: NodeYaml = serde_yaml::from_str(yaml).expect("valid YAML");
        let default_sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let etl_settings = EtlSettings {
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let err = node_def
            .into_entity("TestNode".to_string(), &default_sort_key, &etl_settings)
            .unwrap_err();
        assert!(
            err.to_string().contains("nonexistent_field"),
            "error should mention the bad column name, got: {err}"
        );
        assert!(
            err.to_string().contains("not a declared property"),
            "error should explain the validation failure, got: {err}"
        );
    }

    #[test]
    fn default_columns_empty_is_valid() {
        let yaml = r#"
node_type: TestNode
domain: test
description: A test node
label: name
destination_table: gl_test
properties:
  id:
    type: int64
    source: id
    nullable: false
    description: "ID"
  name:
    type: string
    source: name
    nullable: false
    description: "Name"
"#;
        let node_def: NodeYaml = serde_yaml::from_str(yaml).expect("valid YAML");
        let default_sort_key = vec!["traversal_path".to_string(), "id".to_string()];
        let etl_settings = EtlSettings {
            watermark: "_siphon_replicated_at".to_string(),
            deleted: "_siphon_deleted".to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let entity = node_def
            .into_entity("TestNode".to_string(), &default_sort_key, &etl_settings)
            .expect("should succeed");
        assert!(entity.default_columns.is_empty());
        assert_eq!(entity.sort_key, default_sort_key);
    }

    // ── check_field_flag / modify_field ────────────────────────────

    fn field_flags_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User"])
            .with_fields(
                "User",
                [("username", DataType::String), ("email", DataType::String)],
            )
            .modify_field("User", "email", |f| {
                f.like_allowed = false;
                f.filterable = false;
            })
            .unwrap()
    }

    #[test]
    fn check_field_flag_returns_true_for_reserved_columns() {
        let ont = field_flags_ontology();
        assert!(ont.check_field_flag("User", "id", |f| f.like_allowed));
        assert!(ont.check_field_flag("User", "id", |f| f.filterable));
    }

    #[test]
    fn check_field_flag_returns_true_for_allowed_field() {
        let ont = field_flags_ontology();
        assert!(ont.check_field_flag("User", "username", |f| f.like_allowed));
        assert!(ont.check_field_flag("User", "username", |f| f.filterable));
    }

    #[test]
    fn check_field_flag_returns_false_for_disallowed_field() {
        let ont = field_flags_ontology();
        assert!(!ont.check_field_flag("User", "email", |f| f.like_allowed));
        assert!(!ont.check_field_flag("User", "email", |f| f.filterable));
    }

    #[test]
    fn check_field_flag_fails_closed_for_unknown_field() {
        let ont = field_flags_ontology();
        assert!(!ont.check_field_flag("User", "nonexistent", |f| f.like_allowed));
        assert!(!ont.check_field_flag("User", "nonexistent", |f| f.filterable));
    }

    #[test]
    fn check_field_flag_returns_true_for_unknown_node() {
        let ont = field_flags_ontology();
        assert!(ont.check_field_flag("Unknown", "whatever", |f| f.like_allowed));
    }

    #[test]
    fn modify_field_errors_for_unknown_node() {
        let result = Ontology::new()
            .with_nodes(["User"])
            .modify_field("Bogus", "field", |_| {});
        assert!(result.is_err());
    }

    #[test]
    fn modify_field_errors_for_unknown_field() {
        let result = Ontology::new()
            .with_nodes(["User"])
            .with_fields("User", [("name", DataType::String)])
            .modify_field("User", "bogus", |_| {});
        assert!(result.is_err());
    }
}

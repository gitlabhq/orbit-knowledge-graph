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
pub mod introspection;
mod json_schema;
mod loading;
pub mod query_dsl;

pub use constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, EDGE_RESERVED_COLUMNS, EDGE_TABLE, GL_TABLE_PREFIX,
    NODE_RESERVED_COLUMNS, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN,
};
pub use entities::{
    AuxiliaryColumn, AuxiliaryTable, DataType, DenormDirection, DenormalizedProperty, DomainInfo,
    EdgeColumn, EdgeEndpoint, EdgeEndpointType, EdgeEntity, EdgeSourceEtlConfig, EdgeTableStorage,
    EnumType, Field, FieldSource, NodeEntity, NodeStorage, NodeStyle, RedactionConfig,
    RequiredRole, StorageColumn, StorageIndex, StorageProjection, VirtualSource,
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

/// Configuration for a single edge table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTableConfig {
    pub sort_key: Vec<String>,
    pub columns: Vec<EdgeColumn>,
    /// ClickHouse-specific storage metadata for DDL generation.
    pub storage: EdgeTableStorage,
}

/// A loaded ontology containing all node and edge entities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ontology {
    schema_version: String,
    /// Prefix for all ClickHouse graph table names (e.g., `"gl_"`).
    pub(crate) table_prefix: String,
    /// Default edge table name (e.g., `"gl_edge"`).
    pub(crate) default_edge_table: String,
    /// Default ORDER BY columns for node tables (dedup key for ReplacingMergeTree).
    pub(crate) default_entity_sort_key: Vec<String>,
    /// Edge table configurations keyed by table name.
    pub(crate) edge_table_configs: BTreeMap<String, EdgeTableConfig>,
    pub(crate) domains: BTreeMap<String, DomainInfo>,
    pub(crate) nodes: BTreeMap<String, NodeEntity>,
    pub(crate) edges: BTreeMap<String, Vec<EdgeEntity>>,
    pub(crate) edge_descriptions: BTreeMap<String, String>,
    /// ETL configs for edges sourced from join tables (keyed by relationship kind).
    pub(crate) edge_etl_configs: BTreeMap<String, Vec<EdgeSourceEtlConfig>>,
    pub(crate) etl_settings: EtlSettings,
    pub(crate) internal_column_prefix: String,
    pub(crate) skip_security_filter_for_tables: Vec<String>,
    /// Local entity configs keyed by entity name. Each entry lists
    /// properties to exclude from the local DuckDB table.
    pub(crate) local_entities: BTreeMap<String, Vec<String>>,
    /// Local edge table name, if declared.
    pub(crate) local_edge_table_name: Option<String>,
    /// Local edge table columns, if declared.
    pub(crate) local_edge_columns: Vec<EdgeColumn>,
    /// Non-ontology graph tables (checkpoint, code_indexing_checkpoint, etc.).
    pub(crate) auxiliary_tables: Vec<AuxiliaryTable>,
    /// Node properties denormalized onto edge tables for query optimization.
    pub(crate) denormalized_properties: Vec<DenormalizedProperty>,
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
        let default_sort_key: Vec<String> = EDGE_RESERVED_COLUMNS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let default_config = EdgeTableConfig {
            sort_key: default_sort_key,
            columns: Vec::new(),
            storage: EdgeTableStorage::default(),
        };
        Self {
            schema_version: String::new(),
            table_prefix: GL_TABLE_PREFIX.to_string(),
            default_edge_table: EDGE_TABLE.to_string(),
            default_entity_sort_key: vec![
                TRAVERSAL_PATH_COLUMN.to_string(),
                DEFAULT_PRIMARY_KEY.to_string(),
            ],
            edge_table_configs: BTreeMap::from([(EDGE_TABLE.to_string(), default_config)]),
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
            internal_column_prefix: "_gkg_".to_string(),
            skip_security_filter_for_tables: Vec::new(),
            local_entities: BTreeMap::new(),
            local_edge_table_name: None,
            local_edge_columns: Vec::new(),
            auxiliary_tables: Vec::new(),
            denormalized_properties: Vec::new(),
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
        let cols: Vec<EdgeColumn> = columns
            .into_iter()
            .map(|(name, data_type)| EdgeColumn {
                name: name.into(),
                data_type,
            })
            .collect();
        if let Some(config) = self.edge_table_configs.get_mut(&self.default_edge_table) {
            config.columns = cols;
        }
        self
    }

    /// Builder: declare an additional edge table (for testing multi-table routing).
    #[must_use]
    pub fn with_edge_table(mut self, name: impl Into<String>) -> Self {
        let name = name.into();
        let sort_key: Vec<String> = EDGE_RESERVED_COLUMNS
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        self.edge_table_configs.insert(
            name,
            EdgeTableConfig {
                sort_key,
                columns: Vec::new(),
                storage: EdgeTableStorage::default(),
            },
        );
        self
    }

    /// Builder: assign specific edge types to a named edge table.
    ///
    /// The edge must already be added via `with_edges()` and the table
    /// via `with_edge_table()`. This sets `destination_table` on all
    /// variants of the named edge.
    #[must_use]
    pub fn with_edge_for_table(
        mut self,
        edge_name: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        let edge_name = edge_name.into();
        let table = table.into();
        if let Some(variants) = self.edges.get_mut(&edge_name) {
            for v in variants.iter_mut() {
                v.destination_table = table.clone();
            }
            // If the edge has no variants (common in builder-constructed
            // ontologies), insert a placeholder so edge_table_for_relationship
            // can look it up.
            if variants.is_empty() {
                variants.push(EdgeEntity {
                    relationship_kind: edge_name,
                    destination_table: table,
                    ..Default::default()
                });
            }
        }
        self
    }

    /// Columns of the default edge table, in schema order.
    #[must_use]
    pub fn edge_columns(&self) -> &[EdgeColumn] {
        self.edge_table_configs
            .get(&self.default_edge_table)
            .map(|c| c.columns.as_slice())
            .unwrap_or(&[])
    }

    /// Get the configuration for a specific edge table.
    #[must_use]
    pub fn edge_table_config(&self, table: &str) -> Option<&EdgeTableConfig> {
        self.edge_table_configs.get(table)
    }

    /// Look up the `DataType` of an edge table column by name.
    #[must_use]
    pub fn get_edge_column_type(&self, name: &str) -> Option<DataType> {
        self.edge_columns()
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
            if field_name_string.starts_with(&self.internal_column_prefix) {
                return Err(OntologyError::Validation(format!(
                    "field \"{field_name_string}\" on node \"{node_name}\" uses reserved prefix '{}'",
                    self.internal_column_prefix
                )));
            }
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
            required_role: RequiredRole::Reporter,
        });
        self
    }

    /// Override the `required_role` on a node's existing redaction config.
    /// Panics if the node is missing or has no redaction block. Builder-style
    /// helper for ontologies constructed in tests; production ontologies
    /// load the role directly from YAML.
    #[must_use]
    pub fn with_redaction_role(mut self, node_name: &str, role: RequiredRole) -> Self {
        let redaction = self
            .nodes
            .get_mut(node_name)
            .unwrap_or_else(|| panic!("node \"{node_name}\" does not exist"))
            .redaction
            .as_mut()
            .unwrap_or_else(|| panic!("node \"{node_name}\" has no redaction config"));
        redaction.required_role = role;
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

    /// Returns a clone with all physical table names prefixed for the given
    /// schema version.
    ///
    /// This prepends `prefix` to every `destination_table` on nodes and edges,
    /// the default edge table, edge table config keys, auxiliary table names,
    /// and `skip_security_filter_for_tables`. Local (DuckDB) table names are
    /// not affected.
    ///
    /// An empty prefix returns a clone with table names unchanged (v0 backward
    /// compatibility).
    #[must_use]
    pub fn with_schema_version_prefix(mut self, prefix: &str) -> Self {
        if prefix.is_empty() {
            return self;
        }

        self.default_edge_table = format!("{prefix}{}", self.default_edge_table);

        self.edge_table_configs = self
            .edge_table_configs
            .into_iter()
            .map(|(k, v)| (format!("{prefix}{k}"), v))
            .collect();

        for node in self.nodes.values_mut() {
            node.destination_table = format!("{prefix}{}", node.destination_table);
        }

        for edge_list in self.edges.values_mut() {
            for edge in edge_list {
                edge.destination_table = format!("{prefix}{}", edge.destination_table);
            }
        }

        self.skip_security_filter_for_tables = self
            .skip_security_filter_for_tables
            .into_iter()
            .map(|t| format!("{prefix}{t}"))
            .collect();

        for aux in &mut self.auxiliary_tables {
            aux.name = format!("{prefix}{}", aux.name);
        }

        self
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

    /// Minimum access level required to include rows of the node backing
    /// `table` in a query result. Returns `None` if no node owns this
    /// physical table.
    ///
    /// A `v{N}_` schema-version prefix on the input is stripped before the
    /// lookup because the compiler may receive either the base
    /// `destination_table` or the version-prefixed form produced by
    /// [`Ontology::with_schema_version_prefix`]. Callers that pass edge
    /// tables or CTE names get `None` and should fall back to a default
    /// role.
    #[must_use]
    pub fn min_access_level_for_table(&self, table: &str) -> Option<u32> {
        let normalized = strip_schema_version_prefix(table);
        self.nodes
            .values()
            .find(|n| strip_schema_version_prefix(&n.destination_table) == normalized)
            .and_then(|n| n.redaction.as_ref())
            .map(|r| r.required_role.as_access_level())
    }

    /// Iterator over names of `admin_only` fields on the given entity.
    /// Returns an empty iterator if the entity does not exist.
    pub fn admin_only_properties(&self, entity_name: &str) -> impl Iterator<Item = &str> {
        self.get_node(entity_name).into_iter().flat_map(|node| {
            node.fields
                .iter()
                .filter(|f| f.admin_only)
                .map(|f| f.name.as_str())
        })
    }

    /// Whether `field_name` on `entity_name` is marked `admin_only`.
    /// Returns false if either the entity or the field does not exist.
    #[must_use]
    pub fn is_admin_only(&self, entity_name: &str, field_name: &str) -> bool {
        self.get_node(entity_name)
            .and_then(|node| node.fields.iter().find(|f| f.name == field_name))
            .is_some_and(|f| f.admin_only)
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

    /// Default ClickHouse table name for graph edges.
    #[must_use]
    pub fn edge_table(&self) -> &str {
        &self.default_edge_table
    }

    /// All edge table names defined in settings.
    #[must_use]
    pub fn edge_tables(&self) -> Vec<&str> {
        self.edge_table_configs.keys().map(|s| s.as_str()).collect()
    }

    /// Check if a table name is an edge table.
    #[must_use]
    pub fn is_edge_table(&self, table: &str) -> bool {
        self.edge_table_configs.contains_key(table)
    }

    /// Collapse all edge routing to a single table. Used by the local/DuckDB
    /// pipeline where only one edge table exists.
    pub fn collapse_edge_tables(&mut self, table: &str) {
        let table = table.to_string();
        self.default_edge_table = table.clone();
        for variants in self.edges.values_mut() {
            for edge in variants {
                edge.destination_table = table.clone();
            }
        }
    }

    /// Returns the destination table for a given relationship kind.
    ///
    /// Uses the first variant's `destination_table`. This is correct because
    /// `EdgeYaml::to_entities()` assigns the same table to all variants of a
    /// relationship kind — per-variant table routing is not supported.
    ///
    /// Falls back to the default edge table if the relationship kind is
    /// unknown (e.g. wildcard queries).
    #[must_use]
    pub fn edge_table_for_relationship(&self, relationship_kind: &str) -> &str {
        self.edges
            .get(relationship_kind)
            .and_then(|variants| variants.first())
            .map(|e| e.destination_table.as_str())
            .unwrap_or(&self.default_edge_table)
    }

    /// Prefix for internal columns injected by the compiler.
    #[must_use]
    pub fn internal_column_prefix(&self) -> &str {
        &self.internal_column_prefix
    }

    /// Tables excluded from traversal-path security filters.
    #[must_use]
    pub fn skip_security_filter_tables(&self) -> &[String] {
        &self.skip_security_filter_for_tables
    }

    /// Entity names that participate in the local DuckDB graph.
    #[must_use]
    pub fn local_entity_names(&self) -> Vec<&str> {
        self.local_entities.keys().map(|s| s.as_str()).collect()
    }

    /// Returns the exclude list for a local entity.
    ///
    /// Returns `None` if the entity is not in `local_entities`.
    #[must_use]
    pub fn local_entity_excludes(&self, entity_name: &str) -> Option<&[String]> {
        self.local_entities.get(entity_name).map(|v| v.as_slice())
    }

    /// Returns the fields for a local entity, filtered to exclude virtual
    /// fields and properties listed in the entity's `exclude_properties`.
    ///
    /// Returns `None` if the entity is not in `local_entities`.
    #[must_use]
    pub fn local_entity_fields(&self, entity_name: &str) -> Option<Vec<&Field>> {
        let exclude = self.local_entities.get(entity_name)?;
        let node = self
            .nodes
            .get(entity_name)
            .expect("local entity must exist in nodes");
        Some(
            node.fields
                .iter()
                .filter(|f| !matches!(f.source, FieldSource::Virtual(_)))
                .filter(|f| !exclude.iter().any(|p| p == &f.name))
                .collect(),
        )
    }

    /// Name of the local edge table, if declared.
    #[must_use]
    pub fn local_edge_table_name(&self) -> Option<&str> {
        self.local_edge_table_name.as_deref()
    }

    /// Column definitions for the local edge table, if declared.
    #[must_use]
    pub fn local_edge_columns(&self) -> &[EdgeColumn] {
        &self.local_edge_columns
    }

    /// Non-ontology graph tables (checkpoint, code_indexing_checkpoint, etc.).
    #[must_use]
    pub fn auxiliary_tables(&self) -> &[AuxiliaryTable] {
        &self.auxiliary_tables
    }

    /// Returns all denormalized property declarations.
    #[must_use]
    pub fn denormalized_properties(&self) -> &[DenormalizedProperty] {
        &self.denormalized_properties
    }

    /// Default ORDER BY / dedup key columns for node tables.
    #[must_use]
    pub fn default_entity_sort_key(&self) -> &[String] {
        &self.default_entity_sort_key
    }

    /// ORDER BY / dedup key columns for the default edge table.
    #[must_use]
    pub fn edge_sort_key(&self) -> &[String] {
        self.edge_table_configs
            .get(&self.default_edge_table)
            .map(|c| c.sort_key.as_slice())
            .unwrap_or(&[])
    }

    /// Look up the dedup key (ORDER BY columns) for a ClickHouse table name.
    ///
    /// Returns the node's `sort_key` for node tables, the edge table's
    /// `sort_key` for edge tables, or `None` if the table is unknown.
    #[must_use]
    pub fn sort_key_for_table(&self, table: &str) -> Option<&[String]> {
        if let Some(config) = self.edge_table_configs.get(table) {
            return Some(&config.sort_key);
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

    /// Get ETL configs for an edge by relationship kind.
    ///
    /// Returns `Some` only for edges sourced from join tables.
    pub fn get_edge_etl(&self, relationship_kind: &str) -> Option<&[EdgeSourceEtlConfig]> {
        self.edge_etl_configs
            .get(relationship_kind)
            .map(|v| v.as_slice())
    }

    /// Check if an edge has ETL config (i.e., is sourced from a join table).
    pub fn has_edge_etl(&self, relationship_kind: &str) -> bool {
        self.edge_etl_configs.contains_key(relationship_kind)
    }

    /// Iterator over all edge ETL configs, flattened to (relationship_kind, config) pairs.
    pub fn edge_etl_configs(&self) -> impl Iterator<Item = (&str, &EdgeSourceEtlConfig)> {
        self.edge_etl_configs
            .iter()
            .flat_map(|(k, configs)| configs.iter().map(move |c| (k.as_str(), c)))
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

/// Strip a `v{N}_` schema-version prefix from a physical table name, if
/// present. `N` must be one or more ASCII digits. Used to normalize table
/// names when an ontology built with
/// [`Ontology::with_schema_version_prefix`] is queried with a lookup key
/// that may or may not carry the prefix (and vice versa).
fn strip_schema_version_prefix(table: &str) -> &str {
    let Some(rest) = table.strip_prefix('v') else {
        return table;
    };
    let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
    if digits == 0 {
        return table;
    }
    rest[digits..].strip_prefix('_').unwrap_or(table)
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
    fn code_graph_edges_are_registered() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        // Code-graph emits CALLS, EXTENDS, DEFINES, IMPORTS via the linker
        // resolver. All four must be registered for queries to compile, and
        // they must all route to the same edge table so traversals can JOIN
        // them with edges from other code-graph relationships.
        for kind in ["CALLS", "EXTENDS", "DEFINES", "IMPORTS"] {
            let entries = ontology
                .get_edge(kind)
                .unwrap_or_else(|| panic!("{kind} should be registered"));
            assert!(!entries.is_empty(), "{kind} should have variants");
            assert!(
                entries.iter().any(|e| e.relationship_kind == kind),
                "{kind} relationship_kind mismatch"
            );
            for entry in entries {
                assert_eq!(
                    entry.destination_table, "gl_code_edge",
                    "{kind} variant {:?} -> {:?} should route to gl_code_edge",
                    entry.source_kind, entry.target_kind,
                );
            }
        }
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

    #[test]
    fn strip_schema_version_prefix_matches_vn_underscore() {
        assert_eq!(strip_schema_version_prefix("gl_user"), "gl_user");
        assert_eq!(strip_schema_version_prefix("v1_gl_user"), "gl_user");
        assert_eq!(
            strip_schema_version_prefix("v42_gl_vulnerability"),
            "gl_vulnerability"
        );
        // No digits after `v` — leave untouched.
        assert_eq!(strip_schema_version_prefix("v_gl_user"), "v_gl_user");
        assert_eq!(strip_schema_version_prefix("version_gl_x"), "version_gl_x");
        // Missing underscore after digits — leave untouched.
        assert_eq!(strip_schema_version_prefix("v1gl_user"), "v1gl_user");
    }

    fn ontology_with_role(node: &str, role: RequiredRole) -> Ontology {
        let mut ontology = Ontology::new()
            .with_nodes([node])
            .with_redaction(node, "dummy", "id");
        ontology
            .nodes
            .get_mut(node)
            .and_then(|n| n.redaction.as_mut())
            .expect("node has redaction")
            .required_role = role;
        ontology
    }

    #[test]
    fn min_access_level_for_table_reads_redaction_required_role() {
        let ontology = ontology_with_role("Project", RequiredRole::SecurityManager);
        assert_eq!(ontology.min_access_level_for_table("gl_project"), Some(25));
    }

    #[test]
    fn min_access_level_for_table_normalizes_schema_version_prefix() {
        // The ontology carries the unprefixed `destination_table` but the
        // compiler may look up the prefixed form, or vice-versa.
        let ontology = ontology_with_role("Vulnerability", RequiredRole::SecurityManager);
        assert_eq!(
            ontology.min_access_level_for_table("gl_vulnerability"),
            Some(25)
        );
        assert_eq!(
            ontology.min_access_level_for_table("v1_gl_vulnerability"),
            Some(25)
        );
        assert_eq!(
            ontology.min_access_level_for_table("v42_gl_vulnerability"),
            Some(25)
        );

        // Prefixed ontology, prefixed or unprefixed query — both ends
        // normalize.
        let prefixed = ontology_with_role("Vulnerability", RequiredRole::SecurityManager)
            .with_schema_version_prefix("v1_");
        assert_eq!(
            prefixed.min_access_level_for_table("v1_gl_vulnerability"),
            Some(25)
        );
        assert_eq!(
            prefixed.min_access_level_for_table("gl_vulnerability"),
            Some(25)
        );
    }

    #[test]
    fn min_access_level_for_table_is_none_for_unknown_or_unredacted() {
        let ontology = Ontology::new().with_nodes(["Project"]);
        // Edge tables and CTEs aren't known nodes.
        assert!(ontology.min_access_level_for_table("gl_edge").is_none());
        assert!(ontology.min_access_level_for_table("some_cte").is_none());
        // Node without a `redaction` block yields None — caller picks the
        // default role.
        assert!(ontology.min_access_level_for_table("gl_project").is_none());
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

    // ── edge_tables tests ────────────────────────────────────────────

    #[test]
    fn edge_tables_includes_default_table() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let tables = ontology.edge_tables();
        assert!(
            tables.contains(&ontology.edge_table()),
            "edge_tables should include the default edge table"
        );
    }

    #[test]
    fn is_edge_table_matches_default() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        assert!(ontology.is_edge_table(ontology.edge_table()));
        assert!(!ontology.is_edge_table("gl_user"));
        assert!(!ontology.is_edge_table("nonexistent"));
    }

    #[test]
    fn edge_entity_has_destination_table() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        for entity in ontology.edges() {
            assert!(
                !entity.destination_table.is_empty(),
                "edge '{}' should have a destination_table",
                entity.relationship_kind
            );
        }
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
            .into_entity(
                "TestNode".to_string(),
                &default_sort_key,
                &etl_settings,
                "_gkg_",
            )
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
            .into_entity(
                "TestNode".to_string(),
                &default_sort_key,
                &etl_settings,
                "_gkg_",
            )
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
  default_edge_table: kg_edge
  internal_column_prefix: "_gkg_"
  default_entity_sort_key: [traversal_path, id]
  edge_tables:
    kg_edge:
      sort_key: [traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind]
      columns:
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
            .into_entity(
                "TestNode".to_string(),
                &default_sort_key,
                &etl_settings,
                "_gkg_",
            )
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
            .into_entity(
                "TestNode".to_string(),
                &default_sort_key,
                &etl_settings,
                "_gkg_",
            )
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

    // ── admin_only_properties / is_admin_only ──────────────────────

    fn admin_only_ontology() -> Ontology {
        Ontology::new()
            .with_nodes(["User"])
            .with_fields(
                "User",
                [
                    ("username", DataType::String),
                    ("is_admin", DataType::Bool),
                    ("is_auditor", DataType::Bool),
                ],
            )
            .modify_field("User", "is_admin", |f| f.admin_only = true)
            .unwrap()
            .modify_field("User", "is_auditor", |f| f.admin_only = true)
            .unwrap()
    }

    #[test]
    fn admin_only_properties_yields_only_admin_only_fields() {
        let ont = admin_only_ontology();
        let mut got: Vec<&str> = ont.admin_only_properties("User").collect();
        got.sort_unstable();
        assert_eq!(got, vec!["is_admin", "is_auditor"]);
    }

    #[test]
    fn admin_only_properties_empty_for_unknown_entity() {
        let ont = admin_only_ontology();
        assert_eq!(ont.admin_only_properties("Bogus").count(), 0);
    }

    #[test]
    fn admin_only_properties_empty_when_no_admin_only_fields() {
        let ont = Ontology::new()
            .with_nodes(["Project"])
            .with_fields("Project", [("name", DataType::String)]);
        assert_eq!(ont.admin_only_properties("Project").count(), 0);
    }

    #[test]
    fn is_admin_only_matches_flag() {
        let ont = admin_only_ontology();
        assert!(ont.is_admin_only("User", "is_admin"));
        assert!(ont.is_admin_only("User", "is_auditor"));
        assert!(!ont.is_admin_only("User", "username"));
    }

    #[test]
    fn is_admin_only_fails_closed_for_unknowns() {
        let ont = admin_only_ontology();
        assert!(!ont.is_admin_only("User", "bogus_field"));
        assert!(!ont.is_admin_only("Bogus", "is_admin"));
    }

    #[test]
    fn user_node_marks_sensitive_columns_admin_only() {
        // Pin the real ontology so any future edit that drops admin_only on a
        // sensitive User column fails CI. Each column listed here is one that
        // GitLab does not expose to non-admins on its public REST/GraphQL
        // surfaces; see config/ontology/nodes/core/user.yaml for the rationale.
        let ontology = Ontology::load_embedded().expect("embedded ontology loads");
        for field in [
            "email",
            "first_name",
            "last_name",
            "preferred_language",
            "private_profile",
            "is_external",
            "is_admin",
            "is_auditor",
            "updated_at",
        ] {
            assert!(
                ontology.is_admin_only("User", field),
                "User.{field} must be admin_only"
            );
        }
        for field in [
            "id",
            "username",
            "name",
            "state",
            "avatar_url",
            "public_email",
            "user_type",
            "last_activity_on",
            "created_at",
        ] {
            assert!(
                !ontology.is_admin_only("User", field),
                "User.{field} must not be admin_only"
            );
        }
    }

    #[test]
    fn modify_field_errors_for_unknown_field() {
        let result = Ontology::new()
            .with_nodes(["User"])
            .with_fields("User", [("name", DataType::String)])
            .modify_field("User", "bogus", |_| {});
        assert!(result.is_err());
    }

    #[test]
    fn local_entities_loaded_from_ontology() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let local = ontology.local_entity_names();
        assert!(local.contains(&"Directory"));
        assert!(local.contains(&"File"));
        assert!(local.contains(&"Definition"));
        assert!(local.contains(&"ImportedSymbol"));
        assert!(!local.contains(&"User"));
    }

    #[test]
    fn local_entity_fields_excludes_per_entity_properties() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let fields = ontology
            .local_entity_fields("Directory")
            .expect("Directory is a local entity");
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();

        // Included: regular fields and envelope fields (not excluded in YAML)
        assert!(names.contains(&"id"));
        assert!(names.contains(&"project_id"));
        assert!(names.contains(&"branch"));
        assert!(names.contains(&"path"));
        assert!(names.contains(&"name"));
        // Excluded: listed in exclude_properties for this entity
        assert!(!names.contains(&"traversal_path"));
        // commit_sha is now included in local schema
        assert!(names.contains(&"commit_sha"));
    }

    #[test]
    fn local_entity_fields_excludes_virtual_fields() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        let fields = ontology
            .local_entity_fields("Definition")
            .expect("Definition is a local entity");
        let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();

        assert!(names.contains(&"fqn"));
        assert!(!names.contains(&"content"), "virtual field");
    }

    #[test]
    fn local_entity_fields_returns_none_for_non_local() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        assert!(ontology.local_entity_fields("User").is_none());
    }

    #[test]
    fn local_edge_table_loaded_from_ontology() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        assert_eq!(ontology.local_edge_table_name(), Some("gl_edge"));
        let cols = ontology.local_edge_columns();
        assert!(!cols.is_empty());
        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "source_id",
                "source_kind",
                "relationship_kind",
                "target_id",
                "target_kind"
            ]
        );
    }

    #[test]
    fn local_exclude_properties_validated_against_fields() {
        // Verify the real ontology passes validation (all exclude_properties
        // reference actual fields). If someone adds a typo, this catches it.
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        for entity_name in ontology.local_entity_names() {
            let fields = ontology
                .local_entity_fields(entity_name)
                .expect("local entity should have fields");
            assert!(
                !fields.is_empty(),
                "local entity '{entity_name}' should have at least one field after exclusions"
            );
        }
    }

    #[test]
    fn with_schema_version_prefix_empty_is_identity() {
        let ontology = Ontology::load_embedded().expect("should load");
        let original_edge = ontology.edge_table().to_string();
        let original_user = ontology.table_name("User").unwrap().to_string();

        let prefixed = ontology.with_schema_version_prefix("");

        assert_eq!(prefixed.edge_table(), original_edge);
        assert_eq!(prefixed.table_name("User").unwrap(), original_user);
    }

    #[test]
    fn with_schema_version_prefix_applies_to_all_tables() {
        let ontology = Ontology::load_embedded().expect("should load");
        let prefixed = ontology.with_schema_version_prefix("v1_");

        assert_eq!(prefixed.edge_table(), "v1_gl_edge");
        assert!(prefixed.table_name("User").unwrap().starts_with("v1_"));

        for node in prefixed.nodes() {
            assert!(
                node.destination_table.starts_with("v1_"),
                "node table '{}' should be prefixed",
                node.destination_table
            );
        }

        for edge_table in prefixed.edge_tables() {
            assert!(
                edge_table.starts_with("v1_"),
                "edge table '{edge_table}' should be prefixed",
            );
        }

        for aux in prefixed.auxiliary_tables() {
            assert!(
                aux.name.starts_with("v1_"),
                "auxiliary table '{}' should be prefixed",
                aux.name
            );
        }
    }

    #[test]
    fn with_schema_version_prefix_does_not_affect_local_tables() {
        let ontology = Ontology::load_embedded().expect("should load");
        let local_edge_before = ontology.local_edge_table_name().map(String::from);

        let prefixed = ontology.with_schema_version_prefix("v2_");

        assert_eq!(
            prefixed.local_edge_table_name().map(String::from),
            local_edge_before,
            "local edge table should not be prefixed"
        );
    }
}

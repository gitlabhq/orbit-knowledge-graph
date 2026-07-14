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
pub mod errors;
pub mod etl;
pub mod etl_sql;
pub mod introspection;
mod json_schema;
mod loading;
pub mod migrations;
pub mod pipelines;
pub mod query_dsl;
pub mod sql_template;

pub use constants::{
    DEFAULT_PRIMARY_KEY, DELETED_COLUMN, EDGE_RESERVED_COLUMNS, EDGE_TABLE, GL_TABLE_PREFIX,
    NODE_RESERVED_COLUMNS, TRAVERSAL_PATH_COLUMN, VERSION_COLUMN, siphon_deleted_column,
    siphon_watermark_column,
};
pub use entities::{
    AuxiliaryColumn, AuxiliaryDictionary, AuxiliaryTable, DataType, DenormDirection,
    DenormalizedProperty, DerivedEntity, DictionaryLayout, DictionaryLifetime, DomainInfo,
    EdgeColumn, EdgeEntity, EdgeTableStorage, EdgeVariantScope, EnumType, Field, FieldSelectivity,
    FieldSource, MaterializedViewDefinition, NodeEntity, NodeStorage, NodeStyle, PartitionConfig,
    PartitionStrategy, RedactionConfig, RefreshableMaterializedViewDefinition, RequiredRole,
    StatisticsConfig, StatisticsExclude, StorageColumn, StorageIndex, StorageProjection,
    TraversalPathKind, TraversalPathLookup, TraversalPathLookupSpec, VirtualSource,
};
pub use etl::{
    ClickHouseExtract, DEFAULT_TRANSFORM, EdgeMapping, EnrichSource, EtlScope, Extract,
    ExtractQuery, NodeRef, NodeRefKind, PathResolution, Pipeline, ReindexSource, Transform,
};

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use loading::EtlSettings;

/// A query-graph edge for [`Ontology::propagate_scope_prefixes`]. Abstracts
/// over compiler-specific types so the taint walk lives in the ontology crate.
#[derive(Debug)]
pub struct ScopeEdge<'a> {
    pub from: &'a str,
    pub to: &'a str,
    pub types: &'a [String],
    pub source_kind: &'a str,
    pub target_kind: &'a str,
}

#[derive(Debug)]
pub enum OntologyError {
    Io {
        path: String,
        source: std::io::Error,
    },
    Yaml {
        path: String,
        source: serde_yaml::Error,
    },
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeTableConfig {
    pub sort_key: Vec<String>,
    pub columns: Vec<EdgeColumn>,
    pub storage: EdgeTableStorage,
}

impl EdgeTableConfig {
    #[must_use]
    pub fn has_traversal_path(&self) -> bool {
        self.storage
            .columns
            .iter()
            .any(|column| column.name == TRAVERSAL_PATH_COLUMN)
            || self
                .columns
                .iter()
                .any(|column| column.name == TRAVERSAL_PATH_COLUMN)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ontology {
    schema_version: String,
    pub(crate) table_prefix: String,
    pub(crate) default_edge_table: String,
    /// Default ORDER BY columns for node tables (dedup key for ReplacingMergeTree).
    pub(crate) default_entity_sort_key: Vec<String>,
    pub(crate) edge_table_configs: BTreeMap<String, EdgeTableConfig>,
    pub(crate) domains: BTreeMap<String, DomainInfo>,
    pub(crate) nodes: BTreeMap<String, NodeEntity>,
    pub(crate) edges: BTreeMap<String, Vec<EdgeEntity>>,
    pub(crate) edge_descriptions: BTreeMap<String, String>,
    pub(crate) edge_pipelines: BTreeMap<String, Vec<Pipeline>>,
    /// Reindex trigger tables per edge relationship kind, resolved from each
    /// edge's `indexer` block. Parallels `edge_pipelines`; the pipeline model
    /// itself carries no reindex information.
    pub(crate) edge_reindex_sources: BTreeMap<String, Vec<ReindexSource>>,
    pub(crate) etl_settings: EtlSettings,
    pub(crate) internal_column_prefix: String,
    /// Local entity configs keyed by entity name. Each entry lists
    /// properties to exclude from the local DuckDB table.
    pub(crate) local_entities: BTreeMap<String, Vec<String>>,
    pub(crate) local_edge_table_name: Option<String>,
    pub(crate) local_edge_columns: Vec<EdgeColumn>,
    pub(crate) auxiliary_tables: Vec<AuxiliaryTable>,
    pub(crate) auxiliary_dictionaries: Vec<AuxiliaryDictionary>,
    /// Node properties denormalized onto edge tables for query optimization.
    pub(crate) denormalized_properties: Vec<DenormalizedProperty>,
    /// Edge-producing entities derived by a Rust transform (keyed by name).
    /// These have no node table; they extract from the datalake and emit edges.
    pub(crate) derived_entities: BTreeMap<String, DerivedEntity>,
    pub(crate) materialized_views: Vec<MaterializedViewDefinition>,
    pub(crate) refreshable_materialized_views: Vec<RefreshableMaterializedViewDefinition>,
    pub(crate) statistics: Option<StatisticsConfig>,
    pub(crate) partition: Option<PartitionConfig>,
    pub(crate) traversal_path_lookups: Vec<TraversalPathLookup>,
    /// Regex patterns for `v<N>_*` objects that the GC sweep must never drop.
    /// Matched against the base name (after stripping the `v<N>_` prefix).
    /// Empty by default: everything outside the keep-set is dropped.
    pub(crate) gc_preserve_patterns: Vec<String>,
}

impl Default for Ontology {
    fn default() -> Self {
        Self::new()
    }
}

impl Ontology {
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
            edge_pipelines: BTreeMap::new(),
            edge_reindex_sources: BTreeMap::new(),
            etl_settings: EtlSettings {
                watermark: String::new(),
                deleted: String::new(),
                order_by: vec![
                    TRAVERSAL_PATH_COLUMN.to_string(),
                    DEFAULT_PRIMARY_KEY.to_string(),
                ],
            },
            internal_column_prefix: "_gkg_".to_string(),
            local_entities: BTreeMap::new(),
            local_edge_table_name: None,
            local_edge_columns: Vec::new(),
            auxiliary_tables: Vec::new(),
            auxiliary_dictionaries: Vec::new(),
            denormalized_properties: Vec::new(),
            derived_entities: BTreeMap::new(),
            materialized_views: Vec::new(),
            refreshable_materialized_views: Vec::new(),
            statistics: None,
            partition: None,
            traversal_path_lookups: Vec::new(),
            gc_preserve_patterns: Vec::new(),
        }
    }

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

    #[must_use]
    pub fn with_path_scopable_nodes(
        mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let leads_with_tp =
            self.default_entity_sort_key.first().map(String::as_str) == Some(TRAVERSAL_PATH_COLUMN);
        for name in names {
            let name = name.into();
            self.nodes.insert(
                name.clone(),
                NodeEntity {
                    name: name.clone(),
                    destination_table: format!("{}{}", self.table_prefix, name.to_lowercase()),
                    sort_key: self.default_entity_sort_key.clone(),
                    has_traversal_path: leads_with_tp,
                    ..Default::default()
                },
            );
        }
        self
    }

    #[must_use]
    pub fn with_edges(mut self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        for name in names {
            self.edges.insert(name.into(), vec![]);
        }
        self
    }

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

    #[must_use]
    pub fn with_edge_variant(mut self, variant: EdgeEntity) -> Self {
        let kind = variant.relationship_kind.clone();
        let entry = self.edges.entry(kind).or_default();
        entry.push(variant);
        self
    }

    /// The edge must already be added via `with_edges()` and the table
    /// via `with_edge_table()`. Sets `destination_table` on all variants.
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

    #[must_use]
    pub fn edge_columns(&self) -> &[EdgeColumn] {
        self.edge_table_configs
            .get(&self.default_edge_table)
            .map(|c| c.columns.as_slice())
            .unwrap_or(&[])
    }

    #[must_use]
    pub fn edge_table_config(&self, table: &str) -> Option<&EdgeTableConfig> {
        self.edge_table_configs.get(table)
    }

    #[must_use]
    pub fn get_edge_column_type(&self, name: &str) -> Option<DataType> {
        // Search across all edge tables, not just the default, so
        // table-specific columns (e.g. project_id on gl_code_edge) are
        // recognized by the validation pass.
        for config in self.edge_table_configs.values() {
            if let Some(col) = config.columns.iter().find(|c| c.name == name) {
                return Some(col.data_type);
            }
        }
        None
    }

    pub fn get_edge_table_column_type(&self, table: &str, name: &str) -> Option<DataType> {
        self.edge_table_configs
            .get(table)
            .and_then(|config| config.columns.iter().find(|c| c.name == name))
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

    #[must_use]
    pub fn with_partition(mut self, config: PartitionConfig) -> Self {
        self.partition = Some(config);
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
    /// the default edge table, edge table config keys, and auxiliary table
    /// names. Local (DuckDB) table names are not affected.
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

        for aux in &mut self.auxiliary_tables {
            aux.name = format!("{prefix}{}", aux.name);
        }

        for dict in &mut self.auxiliary_dictionaries {
            dict.name = format!("{prefix}{}", dict.name);
            dict.source_table = format!("{prefix}{}", dict.source_table);
        }

        for lookup in &mut self.traversal_path_lookups {
            lookup.source_table = format!("{prefix}{}", lookup.source_table);
            if let Some(dict) = lookup.dictionary.as_mut() {
                *dict = format!("{prefix}{dict}");
            }
        }

        if let Some(ref mut stats) = self.statistics {
            stats.stats_table = format!("{prefix}{}", stats.stats_table);
            stats.histogram_table = format!("{prefix}{}", stats.histogram_table);
            stats.token_table = format!("{prefix}{}", stats.token_table);
            stats.dictionary = format!("{prefix}{}", stats.dictionary);
        }

        self
    }

    #[must_use]
    pub fn get_node(&self, name: &str) -> Option<&NodeEntity> {
        self.nodes.get(name)
    }

    #[must_use]
    pub fn get_edge(&self, name: &str) -> Option<&[EdgeEntity]> {
        self.edges.get(name).map(|v| v.as_slice())
    }

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

    /// Get relationship kinds whose variants match any of the supplied endpoint pairs.
    ///
    /// Passing `None` for an endpoint leaves that side unconstrained.
    pub fn relationship_kinds_matching<'a>(
        &self,
        endpoints: impl IntoIterator<Item = (Option<&'a str>, Option<&'a str>)>,
    ) -> Vec<String> {
        let endpoints: Vec<_> = endpoints.into_iter().collect();
        self.edges()
            .filter(|edge| {
                endpoints.iter().any(|(source_kind, target_kind)| {
                    source_kind
                        .as_ref()
                        .is_none_or(|kind| edge.source_kind == *kind)
                        && target_kind
                            .as_ref()
                            .is_none_or(|kind| edge.target_kind == *kind)
                })
            })
            .map(|edge| edge.relationship_kind.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[must_use]
    pub fn has_node(&self, name: &str) -> bool {
        self.nodes.contains_key(name)
    }

    #[must_use]
    pub fn has_edge(&self, name: &str) -> bool {
        self.edges.contains_key(name)
    }

    #[must_use]
    pub fn get_redaction_config(&self, entity_name: &str) -> Option<&RedactionConfig> {
        self.get_node(entity_name)?.redaction.as_ref()
    }

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

    pub fn nodes(&self) -> impl Iterator<Item = &NodeEntity> {
        self.nodes.values()
    }

    pub fn node_names(&self) -> impl Iterator<Item = &str> {
        self.nodes.keys().map(|s| s.as_str())
    }

    pub fn derived_entities(&self) -> impl Iterator<Item = &DerivedEntity> {
        self.derived_entities.values()
    }

    pub fn reindex_sources(&self) -> BTreeSet<ReindexSource> {
        let node_sources = self
            .nodes()
            .flat_map(|node| node.reindex_on.iter().cloned());
        let edge_sources = self.edge_reindex_sources.values().flatten().cloned();
        let derived_sources = self
            .derived_entities()
            .flat_map(|derived| derived.reindex_on.iter().cloned());

        node_sources
            .chain(edge_sources)
            .chain(derived_sources)
            .collect()
    }

    pub fn edges(&self) -> impl Iterator<Item = &EdgeEntity> {
        self.edges.values().flatten()
    }

    pub fn edge_names(&self) -> impl Iterator<Item = &str> {
        self.edges.keys().map(|s| s.as_str())
    }

    #[must_use]
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    #[must_use]
    pub fn edge_count(&self) -> usize {
        self.edges.len()
    }

    #[must_use]
    pub fn schema_version(&self) -> &str {
        &self.schema_version
    }

    #[must_use]
    pub fn table_prefix(&self) -> &str {
        &self.table_prefix
    }

    #[must_use]
    pub fn edge_table(&self) -> &str {
        &self.default_edge_table
    }

    #[must_use]
    pub fn edge_tables(&self) -> Vec<&str> {
        self.edge_table_configs.keys().map(|s| s.as_str()).collect()
    }

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

    #[must_use]
    pub fn relationship_kinds_emitted_by(&self, entity_name: &str) -> BTreeSet<String> {
        if self.has_edge(entity_name) {
            return std::iter::once(entity_name.to_string()).collect();
        }
        if let Some(node) = self.get_node(entity_name) {
            return node
                .pipelines
                .iter()
                .flat_map(|pipeline| pipeline.transform.edges())
                .map(|mapping| mapping.label.clone())
                .collect();
        }
        self.derived_entities()
            .find(|derived| derived.name == entity_name)
            .map(|derived| derived.emits.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Returns `true` when **at least one** variant of `relationship_kind` is
    /// scope-preserving. This is a coarse pre-filter — callers that need
    /// per-variant accuracy must inspect [`EdgeEntity::scope`] directly,
    /// because a relationship kind may have both scope-preserving and
    /// non-scope-preserving variants (e.g. `CONTAINS` has `Group→Project`
    /// but also `User→Project` and `WorkItem→WorkItem` which are not).
    /// Fail-closed: unknown or unannotated edges return false.
    #[must_use]
    pub fn has_scope_preserving_variant(&self, relationship_kind: &str) -> bool {
        self.edges.get(relationship_kind).is_some_and(|variants| {
            variants
                .iter()
                .any(|v| v.scope.is_some_and(|s| s.is_scope_preserving()))
        })
    }

    /// True when `entity` declares a `traversal_path_lookup`, i.e. its
    /// `traversal_path` can be resolved from an `id`/`full_path` filter so a
    /// query anchored on it can be scope-pruned. `Project`, `Group`, and
    /// `MergeRequest` today.
    #[must_use]
    pub fn is_anchor(&self, entity: &str) -> bool {
        self.traversal_path_lookups
            .iter()
            .any(|l| l.entity == entity)
    }

    /// True when a `startsWith(traversal_path, P)` predicate prunes granules
    /// on this node's table: the table has a `traversal_path` column, its
    /// dedup key (ORDER BY / `sort_key`) leads with it, and the node is not
    /// global-scoped. Uses `sort_key` rather than `storage.primary_key`
    /// because PRIMARY KEY is an index prefix that may omit leading columns
    /// while `sort_key` is the actual ReplacingMergeTree dedup key.
    #[must_use]
    pub fn is_path_scopable(&self, entity: &str) -> bool {
        let Some(node) = self.nodes.get(entity) else {
            return false;
        };
        if !node.has_traversal_path || node.global {
            return false;
        }
        node.sort_key.first().map(String::as_str) == Some(TRAVERSAL_PATH_COLUMN)
    }

    #[must_use]
    pub fn is_table_path_scopable(&self, table: &str) -> bool {
        let normalized = strip_schema_version_prefix(table);
        self.nodes
            .iter()
            .find(|(_, n)| strip_schema_version_prefix(&n.destination_table) == normalized)
            .is_some_and(|(name, _)| self.is_path_scopable(name))
    }

    /// Returns `(fk_column, anchor_entity)` pairs derived from
    /// `namespace_anchor` edge variants. When a query filters an entity by
    /// one of these FK columns, the compiler can resolve the anchor's
    /// `traversal_path` and scope the query.
    ///
    /// Example: `MergeRequest.project_id` → `("project_id", "Project")`.
    ///
    /// Deduplicated by FK column name. The load-time validator
    /// (`validate_edge_scope_annotations`) guarantees that the same FK
    /// column never maps to two different anchor entities.
    #[must_use]
    pub fn anchor_fk_mappings(&self) -> Vec<(&str, &str)> {
        let mut seen = std::collections::HashMap::new();
        let mut result = Vec::new();
        for variants in self.edges.values() {
            for v in variants {
                if v.scope == Some(EdgeVariantScope::NamespaceAnchor)
                    && let Some(fk) = v.fk_column.as_deref()
                {
                    if let Some(&existing_anchor) = seen.get(fk) {
                        debug_assert_eq!(
                            existing_anchor,
                            v.target_kind.as_str(),
                            "FK column '{fk}' maps to two different anchors"
                        );
                    } else {
                        seen.insert(fk, v.target_kind.as_str());
                        result.push((fk, v.target_kind.as_str()));
                    }
                }
            }
        }
        result
    }

    /// Whether the specific `(relationship_kind, source_kind, target_kind)`
    /// triple is scope-preserving. Unlike [`has_scope_preserving_variant`],
    /// this resolves to the exact variant and is safe for mixed-variant edges
    /// like `CONTAINS`.
    #[must_use]
    pub fn is_scope_preserving_triple(&self, kind: &str, source: &str, target: &str) -> bool {
        self.edges.get(kind).is_some_and(|variants| {
            variants.iter().any(|v| {
                v.source_kind == source
                    && v.target_kind == target
                    && v.scope.is_some_and(|s| s.is_scope_preserving())
            })
        })
    }

    /// The exact `scope` annotation for a `(kind, source, target)` variant.
    #[must_use]
    pub fn edge_scope_for(
        &self,
        kind: &str,
        source_kind: &str,
        target_kind: &str,
    ) -> Option<EdgeVariantScope> {
        self.edges.get(kind).and_then(|variants| {
            variants
                .iter()
                .find(|v| v.source_kind == source_kind && v.target_kind == target_kind)
                .and_then(|v| v.scope)
        })
    }

    /// Flood resolved `traversal_path` prefixes across scope-preserving edges
    /// using a two-pass taint walk.
    ///
    /// **Pass A (taint):** marks every alias reachable from a seed node only
    /// through a non-scope-preserving edge — these must never receive a prefix.
    ///
    /// **Pass B (BFS):** from the seed aliases, walks scope-preserving edges
    /// and copies the prefix to untainted neighbours.
    ///
    /// Pure: no DB calls, no widening past the seed.
    #[must_use]
    pub fn propagate_scope_prefixes(
        &self,
        edges: &[ScopeEdge<'_>],
        seed: &std::collections::HashMap<String, String>,
    ) -> std::collections::HashMap<String, String> {
        use std::collections::{HashMap, HashSet};

        if seed.is_empty() {
            return HashMap::new();
        }

        let scope_preserving: Vec<bool> = edges
            .iter()
            .map(|e| {
                e.types
                    .iter()
                    .all(|t| self.is_scope_preserving_triple(t, e.source_kind, e.target_kind))
            })
            .collect();

        // Pass A: taint aliases reachable only through non-scope-preserving edges.
        let mut tainted: HashSet<&str> = HashSet::new();
        loop {
            let mut changed = false;
            for (i, e) in edges.iter().enumerate() {
                if scope_preserving[i] {
                    continue;
                }
                for (from, to) in [(e.from, e.to), (e.to, e.from)] {
                    if (seed.contains_key(from) || tainted.contains(from)) && tainted.insert(to) {
                        changed = true;
                    }
                }
            }
            if !changed {
                break;
            }
        }

        // Pass B: flood prefixes across scope-preserving edges, skipping tainted.
        let mut result = seed.clone();
        loop {
            let mut changed = false;
            for (i, e) in edges.iter().enumerate() {
                if !scope_preserving[i] {
                    continue;
                }
                let propagation = match (result.get(e.from).cloned(), result.get(e.to).cloned()) {
                    (Some(p), None) if !tainted.contains(e.to) => Some((e.to.to_string(), p)),
                    (None, Some(p)) if !tainted.contains(e.from) => Some((e.from.to_string(), p)),
                    _ => None,
                };
                if let Some((alias, prefix)) = propagation {
                    result.insert(alias, prefix);
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
        result
    }

    #[must_use]
    pub fn internal_column_prefix(&self) -> &str {
        &self.internal_column_prefix
    }

    /// Default datalake watermark column for `argMax` deduplication and
    /// incremental-pull windowing. Loaded from `schema.yaml`'s
    /// `default_watermark`.
    #[must_use]
    pub fn default_watermark_column(&self) -> &str {
        &self.etl_settings.watermark
    }

    /// Default datalake soft-delete flag column. Loaded from `schema.yaml`'s
    /// `default_deleted`.
    #[must_use]
    pub fn default_deleted_column(&self) -> &str {
        &self.etl_settings.deleted
    }

    /// Physical tables of `global` nodes — non-namespaced hubs (User, Runner).
    /// These are excluded from traversal-path security filters because they
    /// carry no `traversal_path` to scope on.
    #[must_use]
    pub fn global_tables(&self) -> Vec<&str> {
        self.nodes
            .values()
            .filter(|n| n.global)
            .map(|n| n.destination_table.as_str())
            .collect()
    }

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

    #[must_use]
    pub fn local_edge_table_name(&self) -> Option<&str> {
        self.local_edge_table_name.as_deref()
    }

    #[must_use]
    pub fn local_edge_columns(&self) -> &[EdgeColumn] {
        &self.local_edge_columns
    }

    #[must_use]
    pub fn auxiliary_tables(&self) -> &[AuxiliaryTable] {
        &self.auxiliary_tables
    }

    #[must_use]
    pub fn materialized_views(&self) -> &[MaterializedViewDefinition] {
        &self.materialized_views
    }

    #[must_use]
    pub fn refreshable_materialized_views(&self) -> &[RefreshableMaterializedViewDefinition] {
        &self.refreshable_materialized_views
    }

    #[must_use]
    pub fn statistics(&self) -> Option<&StatisticsConfig> {
        self.statistics.as_ref()
    }

    #[must_use]
    pub fn partition(&self) -> Option<&PartitionConfig> {
        self.partition.as_ref()
    }

    /// Returns the partition key column for a given entity's statistics MV,
    /// or `None` if the entity lacks the configured partition column (global
    /// entities like User, Runner get an empty partition key).
    #[must_use]
    pub fn stats_partition_key_for(&self, entity: &str) -> Option<&str> {
        let config = self.statistics.as_ref()?;
        let node = self.nodes.get(entity)?;
        if node.fields.iter().any(|f| f.name == config.partition_key) {
            Some(&config.partition_key)
        } else {
            None
        }
    }

    /// Categorize a node entity's filterable fields into stat types.
    /// Returns (categorical, token, histogram) column name lists.
    /// Skips: uuid, virtual, filterable:false, and excluded columns.
    #[must_use]
    pub fn stats_columns_for(&self, entity: &str) -> (Vec<&str>, Vec<&str>, Vec<&str>) {
        if self.statistics.is_none() {
            return (vec![], vec![], vec![]);
        }
        let excluded: std::collections::HashSet<&str> = self
            .statistics
            .as_ref()
            .map(|s| {
                s.exclude
                    .iter()
                    .filter(|e| e.node == entity)
                    .flat_map(|e| e.columns.iter().map(String::as_str))
                    .collect()
            })
            .unwrap_or_default();

        let node = match self.nodes.get(entity) {
            Some(n) => n,
            None => return (vec![], vec![], vec![]),
        };

        let mut categorical = vec![];
        let mut token = vec![];
        let mut histogram = vec![];

        for field in &node.fields {
            if field.is_virtual() || !field.filterable || excluded.contains(field.name.as_str()) {
                continue;
            }
            match field.data_type {
                DataType::Bool | DataType::Enum => categorical.push(field.name.as_str()),
                DataType::String => {
                    if field.selectivity == FieldSelectivity::Low {
                        categorical.push(field.name.as_str());
                    } else {
                        token.push(field.name.as_str());
                    }
                }
                DataType::Int | DataType::Float | DataType::Date | DataType::DateTime => {
                    histogram.push(field.name.as_str());
                }
                DataType::Uuid => {} // skip: selectivity is always 1/row_count
            }
        }

        (categorical, token, histogram)
    }

    #[must_use]
    pub fn auxiliary_dictionaries(&self) -> &[AuxiliaryDictionary] {
        &self.auxiliary_dictionaries
    }

    #[must_use]
    pub fn traversal_path_lookups(&self) -> &[TraversalPathLookup] {
        &self.traversal_path_lookups
    }

    #[must_use]
    pub fn gc_preserve_patterns(&self) -> &[String] {
        &self.gc_preserve_patterns
    }

    #[must_use]
    pub fn traversal_path_lookup(
        &self,
        entity: &str,
        kind: TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.traversal_path_lookups
            .iter()
            .find(|l| l.entity == entity && l.kind == kind)
    }

    #[must_use]
    pub fn denormalized_properties(&self) -> &[DenormalizedProperty] {
        &self.denormalized_properties
    }

    /// Returns the text index tokenizer for a column on a node entity, if one exists.
    ///
    /// Looks up `StorageIndex` entries whose `index_type` starts with `text(`.
    /// Returns the full tokenizer parameter string (e.g. `"tokenizer = splitByNonAlpha"`).
    #[must_use]
    pub fn text_index_tokenizer(&self, entity_name: &str, column_name: &str) -> Option<&str> {
        let node = self.nodes.get(entity_name)?;
        node.storage
            .indexes
            .iter()
            .find(|idx| idx.column == column_name && idx.index_type.starts_with("text("))
            .map(|idx| {
                // Extract the inner params: "text(tokenizer = splitByNonAlpha)" -> "tokenizer = splitByNonAlpha"
                let s = idx.index_type.as_str();
                &s[5..s.len() - 1]
            })
    }

    /// Returns the sorted, deduplicated list of columns on a node entity that
    /// carry a `text(...)` storage index, and therefore support the
    /// `token_match`, `all_tokens`, and `any_tokens` query operators.
    ///
    /// This is the same `text(`-index signal that [`Ontology::text_index_tokenizer`]
    /// keys off and that the compiler's token-operator validation enforces, so
    /// the returned set is exactly the set of properties for which token
    /// operators are accepted.
    #[must_use]
    pub fn text_indexed_columns(&self, entity_name: &str) -> Vec<&str> {
        let Some(node) = self.nodes.get(entity_name) else {
            return vec![];
        };
        let mut columns: Vec<&str> = node
            .storage
            .indexes
            .iter()
            .filter(|idx| idx.index_type.starts_with("text("))
            .map(|idx| idx.column.as_str())
            .collect();
        columns.sort_unstable();
        columns.dedup();
        columns
    }

    #[must_use]
    pub fn default_entity_sort_key(&self) -> &[String] {
        &self.default_entity_sort_key
    }

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

    /// Returns `Some` only for edges sourced from standalone pipelines.
    pub fn get_edge_etl(&self, relationship_kind: &str) -> Option<&[Pipeline]> {
        self.edge_pipelines
            .get(relationship_kind)
            .map(|v| v.as_slice())
    }

    pub fn has_edge_etl(&self, relationship_kind: &str) -> bool {
        self.edge_pipelines.contains_key(relationship_kind)
    }

    /// Whether this relationship's edge materializes `column` on its
    /// `direction` side — i.e. whether a node property in that column can be
    /// denormalized onto the edge row. The single source of truth for which
    /// edges carry which denorm tags; both the read path (compiler) and the
    /// write path (indexer) derive from it.
    ///
    /// - FK edges (no standalone ETL config) project every column of their
    ///   node, so they always carry it.
    /// - A standalone edge projects `column` only when the matching endpoint
    ///   is a fixed `Literal` node type whose `enrich` list includes it;
    ///   polymorphic (`Column`-typed) endpoints materialize no node columns.
    pub fn edge_projects_column(
        &self,
        relationship_kind: &str,
        direction: DenormDirection,
        column: &str,
    ) -> bool {
        if let Some(pipelines) = self.get_edge_etl(relationship_kind) {
            return pipelines.iter().any(|pipeline| {
                pipeline.transform.edges().iter().any(|edge| {
                    if edge.label != relationship_kind {
                        return false;
                    }
                    let node_ref = match direction {
                        DenormDirection::Source => &edge.source,
                        DenormDirection::Target => &edge.target,
                    };
                    matches!(node_ref.kind, NodeRefKind::Literal(_))
                        && node_ref.enrich.iter().any(|c| c == column)
                })
            });
        }

        // An FK edge is indexed from the node holding the key, so only that side
        // is enriched; the other side's tags stay empty. The `any` guard treats
        // the whole kind as FK-projecting if a single variant declares an
        // `fk_column`; this is sound only because every FK-bearing edge declares
        // it on all variants, except the one mixed edge (`has_identifier`), which
        // carries an ETL block and already returned above. A future mixed edge
        // without an ETL block would need per-variant projection instead.
        if let Some(variants) = self.edges.get(relationship_kind)
            && variants.iter().any(|v| v.fk_column.is_some())
        {
            return variants.iter().any(|v| {
                let Some(fk) = v.fk_column.as_deref() else {
                    return false;
                };
                let holder = match direction {
                    DenormDirection::Source => &v.source_kind,
                    DenormDirection::Target => &v.target_kind,
                };
                self.node_has_column(holder, fk)
            });
        }

        true
    }

    fn node_has_column(&self, node_kind: &str, column: &str) -> bool {
        self.get_node(node_kind)
            .is_some_and(|n| n.fields.iter().any(|f| f.column_name() == Some(column)))
    }

    pub fn edge_etl_configs(&self) -> impl Iterator<Item = (&str, &Pipeline)> {
        self.edge_pipelines
            .iter()
            .flat_map(|(k, configs)| configs.iter().map(move |c| (k.as_str(), c)))
    }

    /// Returns `true` when the node exists and `field_name` is either a
    /// reserved column (`id`) or a declared field; `false` if the node is
    /// unknown.
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
            "field \"{field_name}\" does not exist on node type \"{node_name}\"{}",
            crate::errors::describe_valid_fields(node)
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
pub(crate) fn strip_schema_version_prefix(table: &str) -> &str {
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

    fn collect_sql_contents(
        dir: &Path,
        out: &mut std::collections::HashMap<String, std::path::PathBuf>,
    ) {
        for entry in std::fs::read_dir(dir).expect("ontology directory should read") {
            let path = entry.expect("ontology entry should read").path();
            if path.is_dir() {
                // `sql/` holds refreshable-view select templates, not extract pipelines.
                if path.file_name().and_then(|name| name.to_str()) != Some("sql") {
                    collect_sql_contents(&path, out);
                }
            } else if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".sql.j2"))
            {
                let contents = std::fs::read_to_string(&path).expect("query file should read");
                out.insert(contents, path);
            }
        }
    }

    struct EmptyReader;

    impl ReadOntologyFile for EmptyReader {
        fn read(&self, path: &str) -> Result<String, OntologyError> {
            Err(OntologyError::Io {
                path: path.to_string(),
                source: std::io::Error::new(std::io::ErrorKind::NotFound, path.to_string()),
            })
        }
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
    fn embedded_and_dir_load_match_and_carry_authored_sql_verbatim() {
        let embedded = Ontology::load_embedded().expect("should load embedded ontology");
        let from_dir = Ontology::load_from_dir(fixtures_dir()).expect("should load from dir");

        assert_eq!(embedded, from_dir);
        // Markers are the indexer's concern now, so authored SQL is carried raw:
        // at least one authored file's `{{...}}` markers survive load unresolved.
        let markers_preserved = all_pipelines(&embedded).iter().any(|pipeline| {
            let Extract::ClickHouse(extract) = &pipeline.extract;
            matches!(&extract.query, ExtractQuery::Sql(sql)
                if sql.contains("{{watermark_column}}") || sql.contains("{{deleted_column}}"))
        });
        assert!(
            markers_preserved,
            "authored SQL should be carried verbatim with its markers intact"
        );
    }

    #[test]
    fn authored_sql_uses_lifecycle_markers_and_aliases() {
        let embedded = Ontology::load_embedded().expect("should load embedded ontology");

        for pipeline in all_pipelines(&embedded) {
            let Extract::ClickHouse(extract) = &pipeline.extract;
            let ExtractQuery::Sql(sql) = &extract.query else {
                continue;
            };

            assert!(
                !sql.contains(siphon_watermark_column()),
                "{} hardcodes the watermark column {}; use {{{{watermark_column}}}} instead",
                pipeline.name,
                siphon_watermark_column()
            );
            assert!(
                !sql.contains(siphon_deleted_column()),
                "{} hardcodes the deleted column {}; use {{{{deleted_column}}}} instead",
                pipeline.name,
                siphon_deleted_column()
            );

            let version_alias = format!("AS {VERSION_COLUMN}");
            let deleted_alias = format!("AS {DELETED_COLUMN}");
            assert!(
                sql.contains(&version_alias),
                "{} must select a column `{version_alias}`",
                pipeline.name
            );
            assert!(
                sql.contains(&deleted_alias),
                "{} must select a column `{deleted_alias}`",
                pipeline.name
            );
        }
    }

    /// The ontology is declarative: it carries authored `.sql.j2` verbatim and marks
    /// everything else `Generated` (no inline SQL). The generated SQL itself is the
    /// indexer's concern, checked by its golden snapshot — not here.
    #[test]
    fn ontology_carries_authored_sql_and_marks_the_rest_generated() {
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        let mut sql_files = std::collections::HashMap::new();
        collect_sql_contents(&fixtures_dir(), &mut sql_files);

        for pipeline in all_pipelines(&ontology) {
            let Extract::ClickHouse(extract) = &pipeline.extract;
            match &extract.query {
                ExtractQuery::Sql(sql) => {
                    assert!(
                        sql_files.remove(sql).is_some(),
                        "{} authored SQL does not match any committed .sql.j2 file",
                        pipeline.name
                    );
                }
                ExtractQuery::Generated { .. } => {}
            }
        }

        assert!(
            sql_files.is_empty(),
            "committed .sql.j2 files are not loaded by any pipeline: {:?}",
            sql_files.values().collect::<Vec<_>>()
        );
    }

    fn all_pipelines(ontology: &Ontology) -> Vec<&Pipeline> {
        ontology
            .nodes
            .values()
            .flat_map(|node| node.pipelines.iter())
            .chain(
                ontology
                    .edge_pipelines
                    .values()
                    .flat_map(|pipelines| pipelines.iter()),
            )
            .chain(
                ontology
                    .derived_entities
                    .values()
                    .flat_map(|derived| derived.pipelines.iter()),
            )
            .collect()
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

        // They must all route to the same edge table so traversals can JOIN
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
    fn security_edges_route_to_gl_sec_edge() {
        let ontology = Ontology::load_embedded().expect("embedded ontology should load");

        for kind in [
            "HAS_FINDING",
            "DETECTED_BY",
            "HAS_IDENTIFIER",
            "OCCURRENCE_OF",
            "SCANS",
        ] {
            let entries = ontology
                .get_edge(kind)
                .unwrap_or_else(|| panic!("{kind} should be registered"));
            assert!(!entries.is_empty(), "{kind} should have variants");
            for entry in entries {
                assert_eq!(
                    entry.destination_table, "gl_sec_edge",
                    "{kind} variant {:?} -> {:?} should route to gl_sec_edge",
                    entry.source_kind, entry.target_kind,
                );
            }
        }
    }

    #[test]
    fn diff_edges_route_to_gl_diff_edge() {
        let ontology = Ontology::load_embedded().expect("embedded ontology should load");

        for kind in ["HAS_FILE", "HAS_DIFF", "HAS_LATEST_DIFF"] {
            let entries = ontology
                .get_edge(kind)
                .unwrap_or_else(|| panic!("{kind} should be registered"));
            assert!(!entries.is_empty(), "{kind} should have variants");
            for entry in entries {
                assert_eq!(
                    entry.destination_table, "gl_diff_edge",
                    "{kind} variant {:?} -> {:?} should route to gl_diff_edge",
                    entry.source_kind, entry.target_kind,
                );
            }
        }
    }

    #[test]
    fn denorm_declared_only_on_fk_holding_side() {
        use crate::entities::DenormDirection;
        let ontology = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");

        // HAS_DIFF's foreign key (merge_request_id) lives on the diff, so only the
        // target side projects. Projecting onto the always-empty source side pushes
        // has(source_tags,'state:merged') and silently drops every merged-MR diff
        // row (gitlab-org/gitlab#601941). Call edge_projects_column directly so the
        // guard would fail if that fix were reverted.
        assert!(
            !ontology.edge_projects_column("HAS_DIFF", DenormDirection::Source, "state"),
            "HAS_DIFF must not project MergeRequest.state onto source_tags"
        );
        assert!(
            ontology.edge_projects_column("HAS_DIFF", DenormDirection::Target, "state"),
            "HAS_DIFF (diff holds merge_request_id) projects on the target side"
        );
        assert!(
            ontology.edge_projects_column("IN_PROJECT", DenormDirection::Source, "state"),
            "IN_PROJECT (project_id on the MR) projects MergeRequest.state onto source_tags"
        );
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
    fn validate_field_lists_long_field_lists_untrimmed() {
        let fields: Vec<(String, DataType)> = (0..20)
            .map(|i| (format!("field_{i}"), DataType::Int))
            .collect();
        let ontology = Ontology::new()
            .with_nodes(["Wide"])
            .with_fields("Wide", fields);

        let msg = ontology
            .validate_field("Wide", "missing")
            .unwrap_err()
            .to_string();
        assert!(msg.contains("Valid fields:"), "got: {msg}");
        for i in 0..20 {
            assert!(msg.contains(&format!("field_{i}")), "got: {msg}");
        }
        assert!(!msg.contains("more"), "got: {msg}");
        assert!(!msg.contains("get_graph_schema"), "got: {msg}");
        // Reserved "id" must not be duplicated even when a field is also named differently.
        assert_eq!(msg.matches("field_0").count(), 1, "got: {msg}");
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

        // Temporary: aligned with source-code entities to avoid duplicate
        // resource_type entries in the redaction callback. See #570.
        assert_redaction(&ontology, "Project", "project", "id", "read_code");
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
            deleted: constants::siphon_deleted_column().to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let entity = node_def
            .into_entity(
                "TestNode".to_string(),
                "nodes/test/test_node.yaml",
                &default_sort_key,
                &etl_settings,
                "_gkg_",
                &EmptyReader,
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
            deleted: constants::siphon_deleted_column().to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let entity = node_def
            .into_entity(
                "TestNode".to_string(),
                "nodes/test/test_node.yaml",
                &default_sort_key,
                &etl_settings,
                "_gkg_",
                &EmptyReader,
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
            vec![
                "id",
                "name",
                "fqn",
                "definition_type",
                "file_path",
                "commit_sha",
                "start_line",
                "end_line"
            ]
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
            deleted: constants::siphon_deleted_column().to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let err = node_def
            .into_entity(
                "TestNode".to_string(),
                "nodes/test/test_node.yaml",
                &default_sort_key,
                &etl_settings,
                "_gkg_",
                &EmptyReader,
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
            deleted: constants::siphon_deleted_column().to_string(),
            order_by: vec!["traversal_path".to_string(), "id".to_string()],
        };
        let entity = node_def
            .into_entity(
                "TestNode".to_string(),
                "nodes/test/test_node.yaml",
                &default_sort_key,
                &etl_settings,
                "_gkg_",
                &EmptyReader,
            )
            .expect("should succeed");
        assert!(entity.default_columns.is_empty());
        assert_eq!(entity.sort_key, default_sort_key);
    }

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

        assert!(names.contains(&"id"));
        assert!(names.contains(&"project_id"));
        assert!(names.contains(&"branch"));
        assert!(names.contains(&"path"));
        assert!(names.contains(&"name"));
        // traversal_path is included for hydration traversal-path narrowing.
        assert!(names.contains(&"traversal_path"));
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
                "target_kind",
                "traversal_path",
            ]
        );
    }

    #[test]
    fn local_exclude_properties_validated_against_fields() {
        // Guards against a typo'd exclude_properties entry referencing a missing field.
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

        for dict in prefixed.auxiliary_dictionaries() {
            assert!(
                dict.name.starts_with("v1_") && dict.source_table.starts_with("v1_"),
                "dictionary '{}' over '{}' should be prefixed",
                dict.name,
                dict.source_table
            );
        }

        for lookup in prefixed.traversal_path_lookups() {
            assert!(
                lookup.source_table.starts_with("v1_"),
                "lookup source_table '{}' should be prefixed",
                lookup.source_table
            );
            if let Some(dict) = &lookup.dictionary {
                assert!(
                    dict.starts_with("v1_"),
                    "lookup dictionary '{dict}' should be prefixed"
                );
            }
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

    #[test]
    fn fk_column_loaded_from_edge_yaml() {
        let ontology = Ontology::load_embedded().expect("should load");
        let in_project_mr = ontology
            .edges()
            .find(|e| {
                e.relationship_kind == "IN_PROJECT"
                    && e.source_kind == "MergeRequest"
                    && e.target_kind == "Project"
            })
            .expect("IN_PROJECT MR→Project should exist");
        assert_eq!(
            in_project_mr.fk_column.as_deref(),
            Some("project_id"),
            "IN_PROJECT MR→Project should declare fk_column = project_id"
        );
    }

    #[test]
    fn fk_column_none_when_not_declared() {
        let ontology = Ontology::load_embedded().expect("should load");
        let reviewer = ontology
            .edges()
            .find(|e| e.relationship_kind == "REVIEWER")
            .expect("REVIEWER should exist");
        assert_eq!(
            reviewer.fk_column, None,
            "REVIEWER should not declare fk_column (many-to-many)"
        );
    }

    #[test]
    fn system_note_derived_entity_loaded_from_domain() {
        let ontology = Ontology::load_embedded().expect("should load");
        let derived = ontology
            .derived_entities()
            .find(|d| d.name == "SystemNote")
            .expect("SystemNote derived entity should load from the core domain");

        let pipeline = derived
            .pipelines
            .first()
            .expect("SystemNote should declare a pipeline");
        assert!(matches!(
            pipeline.transform,
            crate::Transform::Rust(ref name) if name == "system_notes"
        ));
        assert_eq!(pipeline.scope, crate::EtlScope::Namespaced);
        let crate::Extract::ClickHouse(extract) = &pipeline.extract;
        assert!(matches!(
            extract.query,
            crate::ExtractQuery::Sql(ref sql) if sql.contains("FROM siphon_notes")
        ));
        assert!(derived.emits.contains(&"MENTIONS".to_string()));
    }

    #[test]
    fn relationship_kinds_emitted_by_edge_kind_is_itself() {
        let o = Ontology::load_embedded().unwrap();
        assert_eq!(
            o.relationship_kinds_emitted_by("MENTIONS"),
            std::iter::once("MENTIONS".to_string()).collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn relationship_kinds_emitted_by_derived_entity_are_its_emits() {
        let o = Ontology::load_embedded().unwrap();
        assert!(
            o.relationship_kinds_emitted_by("SystemNote")
                .contains("MENTIONS")
        );
    }

    #[test]
    fn relationship_kinds_emitted_by_node_are_its_edge_mappings() {
        let o = Ontology::load_embedded().unwrap();
        let note = o.get_node("Note").expect("Note node must load");
        let expected: BTreeSet<String> = note
            .pipelines
            .iter()
            .flat_map(|pipeline| pipeline.transform.edges())
            .map(|mapping| mapping.label.clone())
            .collect();
        assert_eq!(o.relationship_kinds_emitted_by("Note"), expected);
    }

    #[test]
    fn relationship_kinds_emitted_by_unknown_entity_is_empty() {
        let o = Ontology::load_embedded().unwrap();
        assert!(o.relationship_kinds_emitted_by("Ghost").is_empty());
    }

    #[test]
    fn derived_emits_are_registered_and_visible_in_schema() {
        let o = Ontology::load_embedded().expect("should load");
        let edge_names: Vec<&str> = o.edge_names().collect();

        for derived in o.derived_entities() {
            for emit in &derived.emits {
                assert!(
                    edge_names.contains(&emit.as_str()),
                    "derived '{}' emits '{emit}' but it is missing from the schema edge list",
                    derived.name
                );
            }
        }

        assert!(edge_names.contains(&"MENTIONS"));
    }

    #[test]
    fn is_anchor_tracks_traversal_path_lookups() {
        let o = Ontology::load_embedded().unwrap();
        for entity in [
            "Project",
            "Group",
            "MergeRequest",
            "Definition",
            "File",
            "Directory",
        ] {
            assert!(o.is_anchor(entity), "{entity} declares a lookup");
        }
        for entity in ["WorkItem", "User", "Nonexistent"] {
            assert!(!o.is_anchor(entity), "{entity} declares no lookup");
        }
        assert!(
            o.traversal_path_lookups()
                .iter()
                .all(|l| o.is_anchor(&l.entity)),
            "is_anchor must accept every lookup entity"
        );
    }

    #[test]
    fn is_path_scopable_accepts_namespaced_traversal_path_entities() {
        let o = Ontology::load_embedded().unwrap();
        for entity in [
            "WorkItem",
            "MergeRequest",
            "Note",
            "Vulnerability",
            "Pipeline",
            "Job",
            "Definition",
            "File",
            "Project",
            "Group",
        ] {
            assert!(
                o.is_path_scopable(entity),
                "{entity} leads its key with traversal_path"
            );
        }
        for entity in ["User", "Runner", "Nonexistent"] {
            assert!(
                !o.is_path_scopable(entity),
                "{entity} is global / has no traversal_path"
            );
        }
    }

    #[test]
    fn scope_preserving_edges_loaded_from_yaml() {
        let o = Ontology::load_embedded().unwrap();
        for kind in [
            "HAS_DIFF",
            "HAS_FILE",
            "HAS_LATEST_DIFF",
            "CONTAINS",
            "DEFINES",
        ] {
            assert!(
                o.has_scope_preserving_variant(kind),
                "{kind} should be same-namespace"
            );
        }
    }

    #[test]
    fn cross_namespace_edges_have_no_scope_preserving_variant() {
        let o = Ontology::load_embedded().unwrap();
        for kind in [
            "CLOSES",
            "RELATED_TO",
            "FIXES",
            "MENTIONS",
            "SOURCE_PROJECT",
            "IMPORTS",
        ] {
            assert!(
                !o.has_scope_preserving_variant(kind),
                "{kind} should NOT be same-namespace"
            );
        }
    }

    #[test]
    fn namespace_anchor_edges_are_in_project_and_in_group() {
        let o = Ontology::load_embedded().unwrap();
        for kind in ["IN_PROJECT", "IN_GROUP"] {
            let variants = o.edges.get(kind).expect(kind);
            assert!(
                variants
                    .iter()
                    .all(|v| v.scope == Some(EdgeVariantScope::NamespaceAnchor)),
                "all {kind} variants must be namespace_anchor"
            );
        }
    }

    #[test]
    fn user_lifecycle_edges_to_issuables_are_prune_to_target() {
        // REOPENED is a global User source into a namespaced issuable target,
        // identical in shape to CLOSED/MERGED. All must carry prune_to_target so
        // the query-restrict pass scopes them from the resolved target prefix;
        // a missing scope falls through restrict's `_ => continue` arm and
        // scopes the edge differently from its siblings.
        let o = Ontology::load_embedded().unwrap();
        for kind in ["REOPENED", "CLOSED", "MERGED"] {
            let variants = o.edges.get(kind).expect(kind);
            assert!(
                variants
                    .iter()
                    .all(|v| v.scope == Some(EdgeVariantScope::PruneToTarget)),
                "all {kind} variants must be prune_to_target"
            );
        }
    }

    #[test]
    fn contains_user_project_and_workitem_workitem_not_scope_preserving() {
        let o = Ontology::load_embedded().unwrap();
        let contains = o.edges.get("CONTAINS").expect("CONTAINS");
        let user_project = contains
            .iter()
            .find(|v| v.source_kind == "User" && v.target_kind == "Project");
        assert_eq!(
            user_project.and_then(|v| v.scope),
            None,
            "User→Project crosses global/namespaced boundary"
        );
        let wi_wi = contains
            .iter()
            .find(|v| v.source_kind == "WorkItem" && v.target_kind == "WorkItem");
        assert_eq!(
            wi_wi.and_then(|v| v.scope),
            None,
            "WorkItem→WorkItem (epic→issue) may cross namespaces"
        );
    }

    #[test]
    fn calls_imported_symbol_variant_not_scope_preserving() {
        let o = Ontology::load_embedded().unwrap();
        let calls = o.edges.get("CALLS").expect("CALLS");
        let def_import = calls
            .iter()
            .find(|v| v.target_kind == "ImportedSymbol")
            .expect("Definition→ImportedSymbol variant");
        assert_eq!(
            def_import.scope, None,
            "ImportedSymbol is the cross-repo resolution boundary"
        );
    }

    #[test]
    fn anchor_fk_mappings_includes_project_id_and_group_id() {
        let o = Ontology::load_embedded().unwrap();
        let mappings = o.anchor_fk_mappings();
        assert!(
            mappings.contains(&("project_id", "Project")),
            "project_id → Project must be in anchor_fk_mappings: {mappings:?}"
        );
        // IN_GROUP uses group_id on Milestone/Label and namespace_id on WorkItem.
        assert!(
            mappings.iter().any(|(_, anchor)| *anchor == "Group"),
            "at least one Group anchor FK must be present: {mappings:?}"
        );
    }

    #[test]
    fn ci_domain_edges_have_scope_preserving_variants() {
        let o = Ontology::load_embedded().unwrap();
        for kind in ["HAS_STAGE", "HAS_JOB", "HAS_HEAD_PIPELINE", "IN_PIPELINE"] {
            assert!(
                o.has_scope_preserving_variant(kind),
                "{kind} should be same-namespace (CI entities share project scope)"
            );
        }
    }

    #[test]
    fn is_scope_preserving_triple_resolves_mixed_variants() {
        let o = Ontology::load_embedded().unwrap();
        assert!(o.is_scope_preserving_triple("CONTAINS", "Group", "Project"));
        assert!(!o.is_scope_preserving_triple("CONTAINS", "User", "Project"));
        assert!(!o.is_scope_preserving_triple("CONTAINS", "WorkItem", "WorkItem"));
        assert!(o.is_scope_preserving_triple("CALLS", "Definition", "Definition"));
        assert!(!o.is_scope_preserving_triple("CALLS", "Definition", "ImportedSymbol"));
    }

    #[test]
    fn propagate_floods_across_same_namespace_edges() {
        let o = Ontology::load_embedded().unwrap();
        let types_diff = vec!["HAS_DIFF".to_string()];
        let types_file = vec!["HAS_FILE".to_string()];
        let edges = vec![
            ScopeEdge {
                from: "mr",
                to: "diff",
                types: &types_diff,
                source_kind: "MergeRequest",
                target_kind: "MergeRequestDiff",
            },
            ScopeEdge {
                from: "diff",
                to: "df",
                types: &types_file,
                source_kind: "MergeRequestDiff",
                target_kind: "MergeRequestDiffFile",
            },
        ];
        let seed =
            std::collections::HashMap::from([("mr".to_string(), "1/9970/15846663/".to_string())]);
        let got = o.propagate_scope_prefixes(&edges, &seed);
        assert_eq!(
            got.get("diff").map(String::as_str),
            Some("1/9970/15846663/")
        );
        assert_eq!(got.get("df").map(String::as_str), Some("1/9970/15846663/"));
    }

    #[test]
    fn propagate_stops_at_cross_namespace_edge() {
        let o = Ontology::load_embedded().unwrap();
        let types_closes = vec!["CLOSES".to_string()];
        let types_label = vec!["HAS_LABEL".to_string()];
        let edges = vec![
            ScopeEdge {
                from: "mr",
                to: "wi",
                types: &types_closes,
                source_kind: "MergeRequest",
                target_kind: "WorkItem",
            },
            ScopeEdge {
                from: "wi",
                to: "lab",
                types: &types_label,
                source_kind: "WorkItem",
                target_kind: "Label",
            },
        ];
        let seed =
            std::collections::HashMap::from([("mr".to_string(), "1/9970/15846663/".to_string())]);
        let got = o.propagate_scope_prefixes(&edges, &seed);
        assert!(!got.contains_key("wi"));
        assert!(!got.contains_key("lab"));
    }

    // Diamond: node X reachable via both a scope-preserving and a
    // cross-namespace path. Taint from pass A must block propagation.
    #[test]
    fn propagate_taints_diamond_reachable_node() {
        let o = Ontology::load_embedded().unwrap();
        let types_diff = vec!["HAS_DIFF".to_string()];
        let types_closes = vec!["CLOSES".to_string()];
        let edges = vec![
            ScopeEdge {
                from: "mr",
                to: "diff",
                types: &types_diff,
                source_kind: "MergeRequest",
                target_kind: "MergeRequestDiff",
            },
            ScopeEdge {
                from: "mr",
                to: "wi",
                types: &types_closes,
                source_kind: "MergeRequest",
                target_kind: "WorkItem",
            },
        ];
        let seed =
            std::collections::HashMap::from([("mr".to_string(), "1/9970/15846663/".to_string())]);
        let got = o.propagate_scope_prefixes(&edges, &seed);
        assert_eq!(
            got.get("diff").map(String::as_str),
            Some("1/9970/15846663/")
        );
        assert!(!got.contains_key("wi"), "tainted via CLOSES");
    }

    fn assert_text_indexed_columns_consistent(ontology: &Ontology) {
        for node in ontology.nodes() {
            let columns = ontology.text_indexed_columns(&node.name);

            for column in &columns {
                assert!(
                    ontology.text_index_tokenizer(&node.name, column).is_some(),
                    "{}.{column} is reported text-indexed but has no tokenizer",
                    node.name
                );
            }

            // Reverse direction: every `text(...)` storage index on the node
            // must surface through the accessor, so the generated doc table can
            // never omit a column for which the validator accepts token ops.
            for idx in &node.storage.indexes {
                if idx.index_type.starts_with("text(") {
                    assert!(
                        columns.contains(&idx.column.as_str()),
                        "{}.{} carries a text() index but is missing from text_indexed_columns",
                        node.name,
                        idx.column
                    );
                }
            }

            assert!(
                columns.windows(2).all(|w| w[0] < w[1]),
                "{} text-indexed columns must be sorted and deduplicated: {columns:?}",
                node.name
            );
        }

        assert!(ontology.text_indexed_columns("Nonexistent").is_empty());
    }

    #[test]
    fn text_indexed_columns_match_tokenizer_lookups() {
        let fixture = Ontology::load_from_dir(fixtures_dir()).expect("should load ontology");
        assert_text_indexed_columns_consistent(&fixture);

        // The generator renders `load_embedded()`, so lock the guarantee against
        // the real shipped ontology too, not just the test fixtures.
        let embedded = Ontology::load_embedded().expect("should load embedded ontology");
        assert_text_indexed_columns_consistent(&embedded);

        let mr = embedded.text_indexed_columns("MergeRequest");
        assert!(mr.contains(&"title"));
        assert!(mr.contains(&"description"));
    }
}

use std::{collections::BTreeMap, fmt};

use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::etl::{EdgeMapping, NodeRef, NodeRefKind, Pipeline, ReindexSource};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageColumn {
    pub name: String,
    pub ch_type: String,
    pub default: Option<String>,
    pub codec: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeStorage {
    pub version_only_engine: bool,
    pub primary_key: Option<Vec<String>>,
    pub columns: Vec<StorageColumn>,
    pub indexes: Vec<StorageIndex>,
    pub projections: Vec<StorageProjection>,
    pub settings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageIndex {
    pub name: String,
    pub column: String,
    pub index_type: String,
    pub granularity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageProjection {
    Reorder {
        name: String,
        order_by: Vec<String>,
    },
    Lightweight {
        name: String,
        order_by: Vec<String>,
    },
    Aggregate {
        name: String,
        select: Vec<String>,
        group_by: Vec<String>,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeTableStorage {
    pub index_granularity: Option<u32>,
    pub primary_key: Option<Vec<String>>,
    pub columns: Vec<StorageColumn>,
    pub indexes: Vec<StorageIndex>,
    pub projections: Vec<StorageProjection>,
    pub denormalized_columns: Vec<StorageColumn>,
    pub denormalized_indexes: Vec<StorageIndex>,
    pub settings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    pub name: String,
    pub versioned: bool,
    pub to_table: Option<String>,
    pub select_query: String,
    pub engine: Option<String>,
    pub engine_args: Vec<String>,
    pub order_by: Vec<String>,
    pub populate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RefreshableMaterializedViewDefinition {
    pub name: String,
    pub versioned: bool,
    pub select_query: String,
    pub append_to: String,
    pub refresh: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsConfig {
    pub stats_table: String,
    pub histogram_table: String,
    pub token_table: String,
    pub dictionary: String,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
    pub histogram_buckets: u16,
    pub top_k_tokens: u16,
    pub partition_key: String,
    pub exclude: Vec<StatisticsExclude>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsExclude {
    pub node: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionConfig {
    pub strategy: PartitionStrategy,
    pub partitioned_tables: std::collections::BTreeSet<String>,
}

impl PartitionConfig {
    #[must_use]
    pub fn is_partitioned(&self, table: &str) -> bool {
        self.partitioned_tables
            .contains(crate::strip_schema_version_prefix(table))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStrategy {
    HashBucket { buckets: u16, column: String },
}

impl PartitionStrategy {
    #[must_use]
    pub fn column(&self) -> &str {
        match self {
            Self::HashBucket { column, .. } => column,
        }
    }
}

impl PartitionConfig {
    #[must_use]
    pub fn column(&self) -> &str {
        self.strategy.column()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryTable {
    pub name: String,
    pub versioned: bool,
    pub columns: Vec<AuxiliaryColumn>,
    pub order_by: Vec<String>,
    pub version_only_engine: bool,
    pub version_type: Option<String>,
    pub projections: Vec<StorageProjection>,
    pub include_system_columns: bool,
    pub engine: Option<String>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub codec: Option<Vec<String>>,
    pub default: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuxiliaryDictionary {
    pub name: String,
    pub source_table: String,
    pub key: String,
    pub key_type: Option<DataType>,
    pub attributes: Vec<AuxiliaryColumn>,
    pub layout: DictionaryLayout,
    pub lifetime: DictionaryLifetime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryLayout {
    pub kind: String,
    pub size_in_cells: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DictionaryLifetime {
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainInfo {
    pub name: String,
    pub description: String,
    pub node_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct NodeStyle {
    #[serde(default = "NodeStyle::default_size")]
    pub size: i32,
    #[serde(default = "NodeStyle::default_color")]
    pub color: String,
}

impl Default for NodeStyle {
    fn default() -> Self {
        Self {
            size: Self::default_size(),
            color: Self::default_color(),
        }
    }
}

impl NodeStyle {
    fn default_size() -> i32 {
        30
    }

    fn default_color() -> String {
        "#6B7280".to_string()
    }
}

/// GitLab access levels. Discriminants match `Gitlab::Access` in Rails so
/// that YAML strings, JWT claim integers, and compiler-side comparisons all
/// agree. `SecurityManager` intentionally sits between `Reporter` and
/// `Developer` (25) to match the role's hybrid scope: broader than reporter
/// for security resources, narrower than developer for code changes.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, strum::FromRepr)]
#[serde(rename_all = "snake_case")]
pub enum RequiredRole {
    Guest = 10,
    Reporter = 20,
    SecurityManager = 25,
    Developer = 30,
    Maintainer = 40,
    Owner = 50,
}

impl RequiredRole {
    pub fn as_access_level(self) -> u32 {
        self as u32
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RedactionConfig {
    /// Rails resource type (e.g., "projects", "merge_requests", "groups", "users").
    /// This maps to the key used in `Authz::RedactionService::RESOURCE_CLASSES`.
    pub resource_type: String,
    pub id_column: String,
    pub ability: String,
    #[serde(default = "RedactionConfig::default_required_role")]
    pub required_role: RequiredRole,
}

impl RedactionConfig {
    fn default_required_role() -> RequiredRole {
        RequiredRole::Reporter
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeEntity {
    pub name: String,
    pub domain: String,
    pub description: String,
    pub label: String,
    pub fields: Vec<Field>,
    pub primary_keys: Vec<String>,
    pub destination_table: String,
    pub enrichment_props: Vec<String>,
    pub default_columns: Vec<String>,
    pub sort_key: Vec<String>,
    pub pipelines: Vec<Pipeline>,
    pub reindex_on: Vec<ReindexSource>,
    pub redaction: Option<RedactionConfig>,
    pub style: NodeStyle,
    pub has_traversal_path: bool,
    pub global: bool,
    pub storage: NodeStorage,
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
            enrichment_props: vec![],
            default_columns: vec![],
            sort_key: vec![],
            destination_table: String::new(),
            pipelines: vec![],
            reindex_on: vec![],
            redaction: None,
            style: NodeStyle::default(),
            has_traversal_path: false,
            global: false,
            storage: NodeStorage::default(),
        }
    }
}

impl NodeEntity {
    pub fn reference_properties(&self) -> impl Iterator<Item = &str> {
        self.pipelines
            .iter()
            .flat_map(|pipeline| pipeline.transform.edges())
            .flat_map(|edge| [&edge.source, &edge.target])
            .filter(|end| !matches!(&end.kind, NodeRefKind::Literal(kind) if *kind == self.name))
            .filter_map(|end| {
                self.fields
                    .iter()
                    .find(|field| field.column_name() == Some(end.field.as_str()))
            })
            .map(|field| field.name.as_str())
    }

    /// Column-backed fields marked `mutable: false` outside the sort key. A tombstone must carry
    /// them, or a pre-pick filter on one rejects the tombstone and resurrects an older live row.
    pub fn immutable_columns(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|field| !field.mutable && field.column_name().is_some())
            .map(|field| field.name.as_str())
            .filter(|name| !self.sort_key.iter().any(|key| key == name))
            .collect()
    }

    pub fn listed_fields(&self) -> impl Iterator<Item = &Field> {
        self.fields
            .iter()
            .filter(|field| !field.hidden && !field.is_disabled_virtual())
    }

    pub fn is_default_column(&self, field: &Field) -> bool {
        !field.admin_only
            && (self.default_columns.is_empty()
                || self
                    .default_columns
                    .iter()
                    .any(|column| column == &field.name))
    }
}

impl fmt::Display for NodeEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeVariantScope {
    NamespaceAnchor,
    SameNamespace,
    PruneToSource,
    PruneToTarget,
}

impl EdgeVariantScope {
    #[must_use]
    pub fn is_scope_preserving(self) -> bool {
        matches!(self, Self::NamespaceAnchor | Self::SameNamespace)
    }

    #[must_use]
    pub fn propagates_to_source(self, relationship: &str, source: &str) -> bool {
        self.is_scope_preserving() && !(relationship == "CONTAINS" && source == "Group")
    }

    #[must_use]
    pub fn edge_path_is_target_path(self) -> bool {
        matches!(self, Self::SameNamespace | Self::PruneToTarget)
    }

    #[must_use]
    pub fn edge_path_is_source_path(self) -> bool {
        matches!(self, Self::PruneToSource)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeEntity {
    pub relationship_kind: String,
    pub source: String,
    pub source_kind: String,
    pub target: String,
    pub target_kind: String,
    pub destination_table: String,
    pub fk_column: Option<String>,
    pub scope: Option<EdgeVariantScope>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedEntity {
    pub name: String,
    pub emits: Vec<String>,
    pub pipelines: Vec<Pipeline>,
    pub reindex_on: Vec<ReindexSource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DenormDirection {
    Source,
    Target,
}

impl DenormDirection {
    pub fn endpoint<'a>(&self, mapping: &'a EdgeMapping) -> &'a NodeRef {
        match self {
            Self::Source => &mapping.source,
            Self::Target => &mapping.target,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenormalizedProperty {
    pub relationship_kind: String,
    pub node_kind: String,
    pub property_name: String,
    pub direction: DenormDirection,
    pub edge_column: String,
    pub tag_key: String,
    pub enum_values: Option<BTreeMap<i64, String>>,
    pub nullable: bool,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldSource {
    DatabaseColumn(String),
    Virtual(VirtualSource),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualSource {
    pub service: String,
    pub lookup: String,
    pub disabled: bool,
    pub depends_on: Vec<String>,
    pub allowed_ops: Vec<String>,
}

impl VirtualSource {
    pub const DEFAULT_ALLOWED_OPS: &[&str] = &[
        "eq",
        "contains",
        "starts_with",
        "ends_with",
        "is_null",
        "is_not_null",
    ];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraversalPathKind {
    Id,
    FullPath,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalPathLookupSpec {
    pub kind: TraversalPathKind,
    pub source_table: String,
    pub key_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalPathLookup {
    pub entity: String,
    pub kind: TraversalPathKind,
    pub source_table: String,
    pub key_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub source: FieldSource,
    pub data_type: DataType,
    pub nullable: bool,
    pub enum_values: Option<BTreeMap<i64, String>>,
    pub enum_type: EnumType,
    pub like_allowed: bool,
    pub filterable: bool,
    pub admin_only: bool,
    pub hidden: bool,
    pub selectivity: FieldSelectivity,
    pub unique: bool,
    pub high_fan_out: bool,
    pub description: Option<String>,
    pub traversal_path_lookup: Option<TraversalPathLookupSpec>,
    pub mutable: bool,
    pub terminal_values: Option<Vec<String>>,
    pub binary: bool,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            name: String::new(),
            source: FieldSource::DatabaseColumn(String::new()),
            data_type: DataType::String,
            nullable: false,
            enum_values: None,
            enum_type: EnumType::default(),
            like_allowed: true,
            filterable: true,
            admin_only: false,
            hidden: false,
            selectivity: FieldSelectivity::High,
            unique: false,
            high_fan_out: false,
            description: None,
            traversal_path_lookup: None,
            mutable: true,
            terminal_values: None,
            binary: false,
        }
    }
}

impl Field {
    pub fn column_name(&self) -> Option<&str> {
        match &self.source {
            FieldSource::DatabaseColumn(name) => Some(name),
            FieldSource::Virtual(_) => None,
        }
    }

    pub fn is_virtual(&self) -> bool {
        matches!(self.source, FieldSource::Virtual(_))
    }

    pub fn is_disabled_virtual(&self) -> bool {
        matches!(&self.source, FieldSource::Virtual(v) if v.disabled)
    }

    pub fn is_text_searchable(&self) -> bool {
        self.like_allowed
            && self.filterable
            && !self.is_virtual()
            && !self.admin_only
            && matches!(self.data_type, DataType::String | DataType::Uuid)
    }
}

impl fmt::Display for Field {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nullable = if self.nullable { "?" } else { "" };
        write!(f, "{}: {}{}", self.name, self.data_type, nullable)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    Enum,
    Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSelectivity {
    Low,
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

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <&str>::deserialize(deserializer)?;
        match s {
            "string" => Ok(Self::String),
            "int64" | "int" | "integer" => Ok(Self::Int),
            "float64" | "float" | "double" => Ok(Self::Float),
            "boolean" | "bool" => Ok(Self::Bool),
            "date" => Ok(Self::Date),
            "timestamp" | "datetime" => Ok(Self::DateTime),
            "enum" => Ok(Self::Enum),
            "uuid" => Ok(Self::Uuid),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &[
                    "string",
                    "int64",
                    "int",
                    "integer",
                    "float64",
                    "float",
                    "double",
                    "boolean",
                    "bool",
                    "date",
                    "timestamp",
                    "datetime",
                    "enum",
                    "uuid",
                ],
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EnumType {
    #[default]
    Int,
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

#[cfg(test)]
mod listed_fields_tests {
    use super::*;

    fn field(name: &str, source: FieldSource) -> Field {
        Field {
            name: name.to_string(),
            source,
            nullable: true,
            ..Field::default()
        }
    }

    fn virtual_source(disabled: bool) -> FieldSource {
        FieldSource::Virtual(VirtualSource {
            service: "observability".to_string(),
            lookup: "pipeline_trace_id".to_string(),
            disabled,
            depends_on: Vec::new(),
            allowed_ops: VirtualSource::DEFAULT_ALLOWED_OPS
                .iter()
                .map(|op| op.to_string())
                .collect(),
        })
    }

    fn node_with(fields: Vec<Field>) -> NodeEntity {
        NodeEntity {
            fields,
            ..Default::default()
        }
    }

    #[test]
    fn a_disabled_virtual_field_is_not_listed() {
        let node = node_with(vec![
            field("id", FieldSource::DatabaseColumn("id".to_string())),
            field("observability_trace_id", virtual_source(true)),
        ]);

        let listed: Vec<&str> = node.listed_fields().map(|f| f.name.as_str()).collect();

        assert_eq!(listed, vec!["id"]);
    }

    #[test]
    fn a_resolvable_virtual_field_is_listed() {
        let node = node_with(vec![field("content", virtual_source(false))]);

        assert_eq!(node.listed_fields().count(), 1);
    }
}

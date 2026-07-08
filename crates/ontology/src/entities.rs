use std::{collections::BTreeMap, fmt};

use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::etl::EtlConfig;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeColumn {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageColumn {
    pub name: String,
    /// Exact ClickHouse type string, e.g. `"Int64"`, `"LowCardinality(String)"`,
    /// `"Nullable(DateTime64(6, 'UTC'))"`.
    pub ch_type: String,
    pub default: Option<String>,
    pub codec: Option<Vec<String>>,
}

/// Table-level storage configuration for a node entity. Fully explicit:
/// columns are listed in DDL order, indexes and projections are complete.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeStorage {
    /// When true, engine is `ReplacingMergeTree(_version)` instead of
    /// `ReplacingMergeTree(_version, _deleted)`.
    pub version_only_engine: bool,
    /// When set, emit `PRIMARY KEY (...)` in the DDL. When absent, ClickHouse
    /// defaults PRIMARY KEY to ORDER BY.
    pub primary_key: Option<Vec<String>>,
    /// Columns in exact DDL order. Does NOT include `_version`/`_deleted`
    /// (system columns are appended automatically).
    pub columns: Vec<StorageColumn>,
    /// Complete list of indexes (no auto-generation).
    pub indexes: Vec<StorageIndex>,
    /// Complete list of projections (no auto-generation).
    pub projections: Vec<StorageProjection>,
    /// Raw ClickHouse MergeTree table settings emitted into CREATE TABLE.
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
    /// Lightweight projection: stores only the key columns + `_part_offset`,
    /// acting as a secondary index without duplicating full rows.
    /// Requires ClickHouse 26.1+.
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

/// A materialized view definition from the ontology settings.
///
/// Materialized views act as ClickHouse insert triggers that transform
/// incoming data and write it to a destination. The `select_query` uses
/// `{table_name}` placeholders for table references so that schema-version
/// prefixes can be resolved at DDL generation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaterializedViewDefinition {
    pub name: String,
    /// Target table for the `TO` clause. When set, the view writes into this
    /// pre-existing table. Table name uses the logical name (without prefix).
    pub to_table: Option<String>,
    /// The `SELECT ...` query. Table references use `{table_name}` placeholders.
    pub select_query: String,
    /// Ignored when `to_table` is set.
    pub engine: Option<String>,
    pub engine_args: Vec<String>,
    /// Ignored when `to_table` is set.
    pub order_by: Vec<String>,
    pub populate: bool,
}

/// Configuration for automated column statistics collection.
///
/// Column categorization is auto-derived from ontology property types:
///   boolean / enum / selectivity:low  →  stats_table  (value frequencies)
///   string                            →  token_table  (token frequencies)
///   int64 / timestamp / date / float  →  histogram_table (equi-depth buckets)
///   uuid                              →  skipped
///   filterable: false / virtual        →  skipped
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
    /// Entities without this column get global stats (empty partition key).
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
    pub columns: Vec<AuxiliaryColumn>,
    pub order_by: Vec<String>,
    /// When true, engine is `ReplacingMergeTree(_version)` without `_deleted`.
    pub version_only_engine: bool,
    /// Override version column type (e.g. `"uint64"` for code_indexing_checkpoint).
    pub version_type: Option<String>,
    pub projections: Vec<StorageProjection>,
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

/// Redaction configuration for an entity.
///
/// Defines how this entity should be validated against Rails' RedactionService
/// to ensure users have permission to view the entity.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RedactionConfig {
    /// Rails resource type (e.g., "projects", "merge_requests", "groups", "users").
    /// This maps to the key used in `Authz::RedactionService::RESOURCE_CLASSES`.
    pub resource_type: String,
    /// Column containing the ID for redaction (defaults to "id").
    #[serde(default = "RedactionConfig::default_id_column")]
    pub id_column: String,
    /// The ability to check for this resource (e.g., "read_project", "read_group").
    /// Defaults to "read".
    #[serde(default = "RedactionConfig::default_ability")]
    pub ability: String,
    /// Minimum GitLab role required on a traversal path for rows of this
    /// entity to survive the compiler security pass. Defaults to `Reporter`
    /// to preserve the pre-role-scoping behavior for entities that did not
    /// opt in. Set to `SecurityManager` (or stricter) for entities whose
    /// ability is only granted at that level, e.g. `read_vulnerability`.
    #[serde(default = "RedactionConfig::default_required_role")]
    pub required_role: RequiredRole,
}

impl RedactionConfig {
    fn default_id_column() -> String {
        "id".to_string()
    }

    fn default_ability() -> String {
        "read".to_string()
    }

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
    /// If empty, all columns are returned.
    pub default_columns: Vec<String>,
    /// ClickHouse ORDER BY columns; used as the deduplication key for ReplacingMergeTree.
    pub sort_key: Vec<String>,
    pub etl: Option<EtlConfig>,
    /// If `None`, this entity does not require redaction validation.
    pub redaction: Option<RedactionConfig>,
    pub style: NodeStyle,
    /// Derived from the declared fields during ontology loading.
    pub has_traversal_path: bool,
    /// Non-namespaced global hub (e.g. User, Runner), declared `global: true`.
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
            default_columns: vec![],
            sort_key: vec![],
            destination_table: String::new(),
            etl: None,
            redaction: None,
            style: NodeStyle::default(),
            has_traversal_path: false,
            global: false,
            storage: NodeStorage::default(),
        }
    }
}

impl fmt::Display for NodeEntity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.name)
    }
}

/// How a resolved `traversal_path` prefix relates to an edge variant's two
/// endpoints. `namespace_anchor` and `same_namespace` propagate the prefix
/// across the edge; `prune_to_source`/`prune_to_target` only confine the
/// edge's own scan to the named endpoint's namespace, without propagating, so
/// the other endpoint may be a global hub (User/Runner/Label).
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
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EdgeEntity {
    pub relationship_kind: String,
    pub source: String,
    pub source_kind: String,
    pub target: String,
    pub target_kind: String,
    /// Defaults to the global edge table.
    pub destination_table: String,
    /// Foreign key column on one of the two node tables that encodes this
    /// relationship (e.g. "project_id", "author_id"). When present, the
    /// compiler can join node tables directly instead of scanning the edge table.
    pub fk_column: Option<String>,
    /// Namespace scope relationship; see [`EdgeVariantScope`].
    pub scope: Option<EdgeVariantScope>,
}

/// ETL configuration for edges sourced from join tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeSourceEtlConfig {
    pub scope: crate::etl::EtlScope,
    pub source: String,
    pub watermark: String,
    pub deleted: String,
    /// Columns for ORDER BY in extract queries and cursor-based pagination.
    pub order_by: Vec<String>,
    /// Optional extra `WHERE` predicate pushed into the extraction SQL, ANDed
    /// with the watermark/deleted/IsNotNull filters. Lets a standalone edge
    /// select a row subset of a shared source table (e.g. one Rails enum value)
    /// without a dedicated source table or a Rust transform.
    pub filter: Option<String>,
    pub reindex_on: Vec<crate::etl::ReindexSource>,
    pub from: EdgeEndpoint,
    pub to: EdgeEndpoint,
}

/// An entity extracted from the datalake and turned into edges by a named
/// transform, with no node table of its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedEntity {
    pub name: String,
    pub emits: Vec<String>,
    pub transform: String,
    pub etl: EtlConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEndpoint {
    pub id_column: String,
    pub node_type: EdgeEndpointType,
    /// Columns to enrich from this endpoint's node datalake table via LEFT
    /// JOIN at extract time. Makes node properties available in the MemTable
    /// for the denormalization system to project onto edge rows.
    pub enrich: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeEndpointType {
    Literal(String),
    Column {
        column: String,
        type_mapping: std::collections::BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DenormDirection {
    Source,
    Target,
}

/// A node property denormalized onto an edge table for query optimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DenormalizedProperty {
    pub relationship_kind: String,
    pub node_kind: String,
    pub property_name: String,
    pub direction: DenormDirection,
    /// Array column on the edge table: `"source_tags"` or `"target_tags"`.
    pub edge_column: String,
    /// Values are stored as `"key:value"` tokens in the array.
    pub tag_key: String,
    pub enum_values: Option<BTreeMap<i64, String>>,
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
    /// When true, the field is declared in the ontology but not yet resolvable.
    /// The compiler will exclude it from hydration plans.
    pub disabled: bool,
    /// Column-backed properties this virtual field needs in the property map
    /// for resolution. The compiler ensures these are fetched during hydration
    /// even if the user didn't request them.
    pub depends_on: Vec<String>,
    /// Filter operators allowed on this virtual column. Populated by the
    /// ontology loader — always non-empty at runtime (defaults applied during
    /// loading when the YAML omits `allowed_ops`).
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
    pub dictionary: Option<String>,
    pub source_table: String,
    pub key_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalPathLookup {
    pub entity: String,
    pub kind: TraversalPathKind,
    pub dictionary: Option<String>,
    pub source_table: String,
    pub key_column: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub source: FieldSource,
    pub data_type: DataType,
    pub nullable: bool,
    /// Integer value to string label mapping for int-based enum types.
    pub enum_values: Option<BTreeMap<i64, String>>,
    /// Defaults to Int.
    pub enum_type: EnumType,
    /// Whether LIKE-based filter operators (contains, starts_with, ends_with)
    /// are allowed on this field. Defaults to true. Set to false for sensitive
    /// columns (e.g. emails, vulnerability titles) to prevent probing.
    pub like_allowed: bool,
    /// Whether users can filter on this field. Defaults to true. Set to false
    /// for internal columns (e.g. traversal_path) that are system-controlled.
    pub filterable: bool,
    /// Whether this field is only visible to instance administrators.
    /// Defaults to false. When true, non-admin users cannot select or
    /// filter on this field.
    pub admin_only: bool,
    /// Filter selectivity hint for the query planner. Low-selectivity columns
    /// (enums, booleans) match most rows and should not trigger narrowing CTEs.
    /// High-selectivity columns (IDs, paths, names) narrow effectively.
    /// Derived automatically from `data_type` unless overridden in YAML.
    pub selectivity: FieldSelectivity,
    pub description: Option<String>,
    pub traversal_path_lookup: Option<TraversalPathLookupSpec>,
    pub mutable: bool,
    /// For enum fields, values that once reached never change (absorbing states).
    pub terminal_values: Option<Vec<String>>,
    /// Source column is a Postgres `bytea` that may hold non-UTF8 bytes. Extraction
    /// hex-encodes invalid values so a single binary row cannot poison the Arrow batch.
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
            selectivity: FieldSelectivity::High,
            description: None,
            traversal_path_lookup: None,
            mutable: true,
            terminal_values: None,
            binary: false,
        }
    }
}

impl Field {
    /// Returns the source column name if this field is column-backed, or `None`
    /// if the field is virtual.
    pub fn column_name(&self) -> Option<&str> {
        match &self.source {
            FieldSource::DatabaseColumn(name) => Some(name),
            FieldSource::Virtual(_) => None,
        }
    }

    pub fn is_virtual(&self) -> bool {
        matches!(self.source, FieldSource::Virtual(_))
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

/// Filter selectivity hint for the query planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
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

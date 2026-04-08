//! Configuration for synthetic data generation and evaluation.

use anyhow::{Context, Result, ensure};
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Root configuration for the simulator.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// ClickHouse connection and schema settings.
    pub clickhouse: ClickHouseConfig,
    /// Data generation settings.
    pub generation: GenerationConfig,
    /// Evaluation settings. Required when running the evaluator.
    #[serde(default)]
    pub evaluation: EvaluationConfig,
}

impl Config {
    /// Load configuration from a YAML file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let config: Self = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        Ok(config)
    }

    /// Save configuration to a YAML file.
    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let contents = serde_yaml::to_string(self)?;
        std::fs::write(path, contents)?;
        Ok(())
    }
}

/// ClickHouse connection and schema settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    /// Connection URL.
    #[serde(default = "default_clickhouse_url")]
    pub url: String,
    /// Database name.
    #[serde(default = "default_database")]
    pub database: String,
    /// Username for authentication.
    #[serde(default = "default_username")]
    pub username: String,
    /// Password for authentication (optional).
    #[serde(default)]
    pub password: Option<String>,
    /// Client connection settings.
    #[serde(default)]
    pub client: ClientConfig,
    /// Schema settings.
    #[serde(default)]
    pub schema: SchemaConfig,
}

fn default_clickhouse_url() -> String {
    "http://localhost:8123".to_string()
}

fn default_database() -> String {
    "default".to_string()
}

fn default_username() -> String {
    "default".to_string()
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_database(),
            username: default_username(),
            password: None,
            client: ClientConfig::default(),
            schema: SchemaConfig::default(),
        }
    }
}

impl ClickHouseConfig {
    /// Build an ArrowClickHouseClient from this config.
    pub fn build_client(&self) -> clickhouse_client::ArrowClickHouseClient {
        clickhouse_client::ArrowClickHouseClient::new(
            &self.url,
            &self.database,
            &self.username,
            self.password.as_deref(),
            &std::collections::HashMap::new(),
        )
    }
}

/// Client connection settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClientConfig {
    /// Send timeout in seconds.
    #[serde(default = "default_timeout")]
    pub send_timeout: u32,
    /// Receive timeout in seconds.
    #[serde(default = "default_timeout")]
    pub receive_timeout: u32,
    /// Max rows per insert batch.
    #[serde(default = "default_insert_block_size")]
    pub max_insert_block_size: usize,
}

fn default_timeout() -> u32 {
    3600
}

fn default_insert_block_size() -> usize {
    100_000
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            send_timeout: default_timeout(),
            receive_timeout: default_timeout(),
            max_insert_block_size: default_insert_block_size(),
        }
    }
}

/// Schema configuration for tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaConfig {
    /// Table engine type: MergeTree, ReplacingMergeTree, etc.
    #[serde(default = "default_engine_type")]
    pub engine: String,
    /// Use per-node sort_key from the ontology YAML instead of global node_order_by.
    /// When true, each node table gets its own ORDER BY from the ontology
    /// (e.g. gl_user uses [id], code tables use [traversal_path, project_id, branch, id]).
    /// When false (default), all node tables share the same node_order_by.
    #[serde(default)]
    pub use_ontology_sort_keys: bool,
    /// PRIMARY KEY columns for node tables (sparse index, not uniqueness constraint).
    /// If empty, defaults to ORDER BY columns.
    /// Ignored when use_ontology_sort_keys is true.
    #[serde(default)]
    pub node_primary_key: Vec<String>,
    /// ORDER BY columns for node tables (physical sort order on disk).
    /// Ignored when use_ontology_sort_keys is true.
    #[serde(default = "default_node_order_by")]
    pub node_order_by: Vec<String>,
    /// PRIMARY KEY columns for edge table.
    /// If empty, defaults to ORDER BY columns.
    #[serde(default)]
    pub edge_primary_key: Vec<String>,
    /// ORDER BY columns for edge table.
    #[serde(default = "default_edge_order_by")]
    pub edge_order_by: Vec<String>,
    /// Index granularity (rows per granule).
    #[serde(default = "default_index_granularity")]
    pub index_granularity: u32,
    /// Data skipping indexes.
    #[serde(default)]
    pub indexes: Vec<IndexConfig>,
    /// Projections for bidirectional traversal.
    #[serde(default)]
    pub projections: Vec<ProjectionConfig>,
    /// Additional MergeTree SETTINGS options.
    ///
    /// These are appended to the CREATE TABLE SETTINGS clause.
    /// Common options:
    /// - `min_bytes_for_wide_part`: Threshold for wide vs compact parts (default: 10MB)
    /// - `merge_with_ttl_timeout`: TTL merge interval in seconds
    /// - `storage_policy`: Named storage policy for tiered storage
    /// - `ttl_only_drop_parts`: Only drop whole parts on TTL expiry (0 or 1)
    ///
    /// See: https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#settings
    #[serde(default)]
    pub settings: HashMap<String, String>,
}

fn default_engine_type() -> String {
    "MergeTree".to_string()
}

fn default_node_order_by() -> Vec<String> {
    use ontology::constants::{DEFAULT_PRIMARY_KEY, TRAVERSAL_PATH_COLUMN};
    vec![
        TRAVERSAL_PATH_COLUMN.to_string(),
        DEFAULT_PRIMARY_KEY.to_string(),
    ]
}

fn default_edge_order_by() -> Vec<String> {
    use ontology::constants::{EDGE_RESERVED_COLUMNS, TRAVERSAL_PATH_COLUMN};
    // Subset of EDGE_RESERVED_COLUMNS in query-optimal order.
    // All values validated against EDGE_RESERVED_COLUMNS at startup.
    vec![
        TRAVERSAL_PATH_COLUMN.to_string(),
        EDGE_RESERVED_COLUMNS[2].to_string(), // source_id
        EDGE_RESERVED_COLUMNS[3].to_string(), // source_kind
        EDGE_RESERVED_COLUMNS[1].to_string(), // relationship_kind
    ]
}

fn default_index_granularity() -> u32 {
    8192
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            engine: default_engine_type(),
            use_ontology_sort_keys: false,
            node_primary_key: vec![],
            node_order_by: default_node_order_by(),
            edge_primary_key: vec![],
            edge_order_by: default_edge_order_by(),
            index_granularity: default_index_granularity(),
            indexes: vec![],
            projections: vec![],
            settings: HashMap::new(),
        }
    }
}

/// Data skipping index configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexConfig {
    /// Index name.
    pub name: String,
    /// Target table ("*" for all node tables, "edges" for edge table).
    pub table: String,
    /// Column expression to index.
    pub expression: String,
    /// Index type: bloom_filter, minmax, set, tokenbf_v1, ngrambf_v1.
    #[serde(rename = "type")]
    pub index_type: String,
    /// Granularity (typically 1-8 for good selectivity).
    #[serde(default = "default_index_granularity_small")]
    pub granularity: u32,
}

fn default_index_granularity_small() -> u32 {
    4
}

/// Projection configuration for reverse lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionConfig {
    /// Projection name.
    pub name: String,
    /// Target table.
    pub table: String,
    /// Columns to include in projection (SELECT clause).
    pub columns: Vec<String>,
    /// ORDER BY columns for projection.
    pub order_by: Vec<String>,
}

/// Data generation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerationConfig {
    /// Path to ontology fixtures.
    #[serde(default = "default_ontology_path")]
    pub ontology_path: String,
    /// Output directory for Parquet files.
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    /// Skip generation if Parquet files already exist.
    #[serde(default)]
    pub skip_if_present: bool,
    /// Number of organizations to generate.
    #[serde(default = "default_organizations")]
    pub organizations: u32,

    /// Root entities with absolute counts per organization.
    /// These entities have no parent and are generated first.
    /// Example: { "User": 100, "Group": 50 }
    #[serde(default)]
    pub roots: HashMap<String, usize>,

    /// Relationship-based generation configuration.
    /// Defines how child entities are generated based on ontology edges.
    #[serde(default)]
    pub relationships: RelationshipConfig,

    /// Entity type that defines the namespace hierarchy.
    ///
    /// This entity type gets namespace IDs (extending traversal paths)
    /// instead of regular entity IDs. In GitLab's data model this is
    /// "Group" — groups define the `org/ns1/ns2/` hierarchy that scopes
    /// all other entities.
    #[serde(default = "default_namespace_entity")]
    pub namespace_entity: String,

    /// Association edges configuration.
    /// Creates edges between existing entities (e.g., AUTHORED, MEMBER_OF).
    #[serde(default)]
    pub associations: AssociationConfig,

    /// Batch size for Parquet row groups.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Run generation in parallel across organizations.
    #[serde(default)]
    pub parallel: bool,

    /// Random seed for reproducible data generation.
    /// If not set, uses thread-local random source.
    #[serde(default)]
    pub seed: Option<u64>,

    /// Path to fake data YAML file for customizing generated values.
    #[serde(default = "default_fake_data_path")]
    pub fake_data_path: String,
}

fn default_output_dir() -> String {
    "data".to_string()
}

fn default_ontology_path() -> String {
    env!("ONTOLOGY_DIR").to_string()
}

fn default_organizations() -> u32 {
    2
}

fn default_namespace_entity() -> String {
    super::constants::DEFAULT_NAMESPACE_ENTITY.to_string()
}

fn default_fake_data_path() -> String {
    super::constants::DEFAULT_FAKE_DATA_PATH.to_string()
}

fn default_batch_size() -> usize {
    10_000
}

impl Default for GenerationConfig {
    fn default() -> Self {
        Self {
            ontology_path: default_ontology_path(),
            output_dir: default_output_dir(),
            skip_if_present: false,
            organizations: default_organizations(),
            roots: HashMap::new(),
            relationships: RelationshipConfig::default(),
            namespace_entity: default_namespace_entity(),
            associations: AssociationConfig::default(),
            batch_size: default_batch_size(),
            parallel: false,
            seed: None,
            fake_data_path: default_fake_data_path(),
        }
    }
}

/// Edge ratio for relationship-based generation.
///
/// Can be either:
/// - An integer count (e.g., 5 children per parent)
/// - A fractional probability (e.g., 0.3 = 30% chance of creating edge)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeRatio {
    /// Fixed count of children per parent.
    Count(usize),
    /// Probability of creating the relationship (0.0-1.0).
    Probability(f64),
    /// Recursive hierarchy: fixed count per parent, expanded to `max_depth` levels.
    ///
    /// Only valid for self-referential edges (source == target).
    /// The dependency graph expands `A → A` into epsilon depth-level nodes:
    /// `A → A@1 → A@2 → ... → A@max_depth`, each with `count` children per parent.
    ///
    /// ```yaml
    /// CONTAINS:
    ///   "Group -> Group":
    ///     count: 2
    ///     max_depth: 3
    /// ```
    Recursive { count: usize, max_depth: usize },
}

impl EdgeRatio {
    /// Sample a count from this ratio.
    pub fn sample(&self, rng: &mut impl Rng) -> usize {
        match self {
            EdgeRatio::Count(n) | EdgeRatio::Recursive { count: n, .. } => *n,
            EdgeRatio::Probability(p) => {
                if rng.random_bool(*p) {
                    1
                } else {
                    0
                }
            }
        }
    }

    /// Sample a count with variance (for more realistic distributions).
    /// For counts, returns a value in range [count/2, count*1.5].
    pub fn sample_with_variance(&self, rng: &mut impl Rng) -> usize {
        match self {
            EdgeRatio::Count(n) | EdgeRatio::Recursive { count: n, .. } => {
                let min = (*n as f64 * 0.5).ceil() as usize;
                let max = (*n as f64 * 1.5).ceil() as usize;
                rng.random_range(min.max(1)..=max.max(1))
            }
            EdgeRatio::Probability(p) => {
                if rng.random_bool(*p) {
                    1
                } else {
                    0
                }
            }
        }
    }
}

impl Default for EdgeRatio {
    fn default() -> Self {
        EdgeRatio::Count(1)
    }
}

/// Configuration for a single edge relationship variant.
///
/// Format in YAML: `"SourceKind -> TargetKind": ratio`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeVariantConfig {
    /// Source node type (e.g., "Group").
    pub source: String,
    /// Target node type (e.g., "Project").
    pub target: String,
    /// Ratio or probability for this relationship.
    pub ratio: EdgeRatio,
}

/// Relationship-based generation configuration.
///
/// Maps ontology edge types to their generation ratios.
/// Example:
/// ```yaml
/// relationships:
///   CONTAINS:
///     "Group -> Group": 3        # 3 subgroups per group
///     "Group -> Project": 5      # 5 projects per group
///   IN_PROJECT:
///     "MergeRequest -> Project": 30
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationshipConfig {
    /// Map of edge type to variant configurations.
    /// Key: edge relationship kind (e.g., "CONTAINS")
    /// Value: map of "Source -> Target" to ratio
    #[serde(flatten)]
    pub edges: HashMap<String, HashMap<String, EdgeRatio>>,
}

impl RelationshipConfig {
    /// Parse a variant key like "Group -> Project" into (source, target).
    pub fn parse_variant_key(key: &str) -> Option<(String, String)> {
        let parts: Vec<&str> = key.split("->").map(|s| s.trim()).collect();
        if parts.len() == 2 {
            Some((parts[0].to_string(), parts[1].to_string()))
        } else {
            None
        }
    }

    /// Get all configured relationships as a flat list.
    pub fn all_relationships(&self) -> Vec<(String, String, String, EdgeRatio)> {
        let mut result = Vec::new();
        for (edge_type, variants) in &self.edges {
            for (variant_key, ratio) in variants {
                if let Some((source, target)) = Self::parse_variant_key(variant_key) {
                    result.push((edge_type.clone(), source, target, ratio.clone()));
                }
            }
        }
        result
    }
}

/// Iteration direction for association edge generation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IterationDirection {
    /// Iterate over target entities (default).
    /// For each target, sample source entities to link.
    #[default]
    Target,
    /// Iterate over source entities.
    /// For each source, sample target entities to link.
    Source,
}

/// Extended association edge configuration with iteration direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociationEdgeExtended {
    /// Ratio or probability for this relationship.
    pub ratio: EdgeRatio,
    /// Which side to iterate over when generating edges.
    #[serde(default)]
    pub per: IterationDirection,
}

/// Association edge value - can be simple ratio or extended config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AssociationEdgeValue {
    /// Simple ratio (defaults to iterating over targets).
    Simple(EdgeRatio),
    /// Extended config with iteration direction.
    Extended(AssociationEdgeExtended),
}

impl AssociationEdgeValue {
    /// Get the ratio from this config.
    pub fn ratio(&self) -> &EdgeRatio {
        match self {
            AssociationEdgeValue::Simple(r) => r,
            AssociationEdgeValue::Extended(e) => &e.ratio,
        }
    }

    /// Get the iteration direction.
    pub fn iteration_direction(&self) -> IterationDirection {
        match self {
            AssociationEdgeValue::Simple(_) => IterationDirection::Target,
            AssociationEdgeValue::Extended(e) => e.per,
        }
    }
}

/// Association edge configuration.
///
/// Creates edges between existing entities (does not generate new entities).
/// Used for relationships like AUTHORED, MEMBER_OF, ASSIGNED, etc.
///
/// # Simple Format
/// `"Source -> Target": ratio`
/// Iterates over targets, sampling sources to link.
///
/// # Extended Format
/// `"Source -> Target": { ratio: 0.3, per: "source" }`
/// Iterates over the specified side (source or target).
///
/// # Example YAML
/// ```yaml
/// associations:
///   AUTHORED:
///     "User -> MergeRequest": 1           # Each MR has 1 author
///   MERGED_BY:
///     "User -> MergeRequest":             # 30% of MRs have a merger
///       ratio: 0.3
///       per: target                       # Iterate over MRs, not Users
///   ASSIGNED:
///     "User -> WorkItem": 0.7             # 70% of work items have an assignee
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssociationConfig {
    /// Map of edge type to variant configurations.
    #[serde(flatten)]
    pub edges: HashMap<String, HashMap<String, AssociationEdgeValue>>,
}

impl AssociationConfig {
    /// Get all configured associations as a flat list.
    /// Returns (edge_type, source_kind, target_kind, ratio, iteration_direction).
    pub fn all_associations(&self) -> Vec<(String, String, String, EdgeRatio, IterationDirection)> {
        let mut result = Vec::new();
        for (edge_type, variants) in &self.edges {
            for (variant_key, value) in variants {
                if let Some((source, target)) = RelationshipConfig::parse_variant_key(variant_key) {
                    result.push((
                        edge_type.clone(),
                        source,
                        target,
                        value.ratio().clone(),
                        value.iteration_direction(),
                    ));
                }
            }
        }
        result
    }
}

/// Evaluation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationConfig {
    /// Path to queries YAML file.
    pub queries_path: String,
    /// Number of IDs to sample per entity type.
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
    /// Number of iterations to run each query.
    #[serde(default = "default_iterations")]
    pub iterations: usize,
    /// Number of queries to execute concurrently.
    /// 1 = serial (default), >1 = concurrent load testing.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Skip cache warming.
    #[serde(default)]
    pub skip_cache_warm: bool,
    /// Filter pattern for queries.
    #[serde(default)]
    pub filter: Option<String>,
    /// Output settings.
    #[serde(default)]
    pub output: OutputConfig,
    /// Directory to save run metadata (query plans, params, sample data).
    #[serde(default)]
    pub metadata_dir: Option<String>,
    /// ClickHouse query-level SETTINGS appended to every evaluated query.
    /// These override the built-in safe defaults (e.g. `join_algorithm`).
    #[serde(default)]
    pub settings: std::collections::HashMap<String, String>,
}

fn default_sample_size() -> usize {
    100
}

fn default_iterations() -> usize {
    1
}

fn default_concurrency() -> usize {
    1
}

impl Default for EvaluationConfig {
    fn default() -> Self {
        Self {
            queries_path: String::new(),
            sample_size: default_sample_size(),
            iterations: default_iterations(),
            concurrency: default_concurrency(),
            skip_cache_warm: false,
            filter: None,
            output: OutputConfig::default(),
            metadata_dir: None,
            settings: std::collections::HashMap::new(),
        }
    }
}

impl EvaluationConfig {
    /// Validate that required fields are set.
    /// `queries_path` has no serde default so YAML deserialization enforces it,
    /// but `Default` leaves it empty for convenience. Call this before evaluation.
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.queries_path.is_empty(),
            "evaluation.queries_path must be set"
        );
        ensure!(self.concurrency >= 1, "evaluation.concurrency must be >= 1");
        Ok(())
    }
}

/// Output configuration for evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutputConfig {
    /// Output format: text, json, markdown.
    #[serde(default = "default_format")]
    pub format: String,
    /// Output file path (stdout if not specified).
    #[serde(default)]
    pub path: Option<String>,
}

fn default_format() -> String {
    "text".to_string()
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            format: default_format(),
            path: None,
        }
    }
}

/// Fake data configuration loaded from YAML.
///
/// Controls string pools, classification rules, boolean probabilities,
/// and integer ranges used by `FakeValueGenerator`.
/// All fields are mandatory in YAML — no serde defaults.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeDataConfig {
    /// String pools and classification rules.
    pub strings: FakeDataStrings,
    /// Boolean probabilities keyed by field name, with a default fallback.
    pub bools: FakeDataBools,
    /// Integer ranges keyed by field name, with a default fallback.
    pub ints: FakeDataInts,
}

impl FakeDataConfig {
    /// Load from a YAML file. Validates all values after deserialization.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read fake data file: {}", path.display()))?;
        let config: Self = serde_yaml::from_str(&contents)
            .with_context(|| format!("Failed to parse fake data file: {}", path.display()))?;
        config
            .validate()
            .with_context(|| format!("Invalid fake data file: {}", path.display()))?;
        Ok(config)
    }

    /// Validate all values are within expected bounds.
    pub fn validate(&self) -> Result<()> {
        self.validate_string_pools()?;
        self.validate_bools()?;
        self.validate_ints()?;
        Ok(())
    }

    fn validate_string_pools(&self) -> Result<()> {
        let pools = &self.strings.pools;
        ensure!(
            !pools.name_prefixes.is_empty(),
            "strings.pools.name_prefixes must not be empty"
        );
        ensure!(
            !pools.email_domains.is_empty(),
            "strings.pools.email_domains must not be empty"
        );
        ensure!(
            !pools.description_words.is_empty(),
            "strings.pools.description_words must not be empty"
        );
        ensure!(
            !pools.statuses.is_empty(),
            "strings.pools.statuses must not be empty"
        );
        ensure!(
            !pools.states.is_empty(),
            "strings.pools.states must not be empty"
        );
        ensure!(
            !pools.branch_prefixes.is_empty(),
            "strings.pools.branch_prefixes must not be empty"
        );
        Ok(())
    }

    fn validate_bools(&self) -> Result<()> {
        validate_probability(self.bools.default, "bools.default")?;
        for (name, &p) in &self.bools.fields {
            validate_probability(p, &format!("bools.fields.{name}"))?;
        }
        Ok(())
    }

    fn validate_ints(&self) -> Result<()> {
        validate_int_range(self.ints.default, "ints.default")?;
        for (name, &range) in &self.ints.fields {
            validate_int_range(range, &format!("ints.fields.{name}"))?;
        }
        Ok(())
    }
}

fn validate_probability(p: f64, field: &str) -> Result<()> {
    ensure!(
        p.is_finite(),
        "{field}: probability must be finite, got {p}"
    );
    ensure!(
        (0.0..=1.0).contains(&p),
        "{field}: probability must be in [0.0, 1.0], got {p}"
    );
    Ok(())
}

fn validate_int_range(range: [u32; 2], field: &str) -> Result<()> {
    let [min, max] = range;
    ensure!(min <= max, "{field}: min ({min}) must be <= max ({max})");
    ensure!(
        (max as u64 - min as u64 + 1) <= u32::MAX as u64,
        "{field}: range [{min}, {max}] too large (range_size would overflow u32)"
    );
    Ok(())
}

/// String pools and classification rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeDataStrings {
    /// Named string pools referenced by generation strategies.
    pub pools: FakeDataStringPools,
    /// Classification rules for string fields (first match wins).
    pub classify: Vec<StringClassifyRule>,
}

/// Named string pools.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeDataStringPools {
    pub name_prefixes: Vec<String>,
    pub email_domains: Vec<String>,
    pub description_words: Vec<String>,
    pub statuses: Vec<String>,
    pub states: Vec<String>,
    pub branch_prefixes: Vec<String>,
}

/// A classification rule: if the lowercased field name contains any pattern, use this kind.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StringClassifyRule {
    /// Substrings to match against the lowercased field name.
    pub contains: Vec<String>,
    /// The generation strategy to use.
    pub kind: StringKind,
}

/// String generation strategy. Each variant maps to a different formatting template
/// in the generator code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StringKind {
    NameOrTitle,
    Email,
    Url,
    Path,
    ShaOrHash,
    DescriptionOrBody,
    Status,
    State,
    RefOrBranch,
}

/// Boolean probabilities keyed by lowercased field name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeDataBools {
    /// Fallback probability for fields not in `fields`.
    pub default: f64,
    /// Per-field probabilities (0.0–1.0).
    pub fields: HashMap<String, f64>,
}

/// Integer ranges keyed by lowercased field name.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FakeDataInts {
    /// Fallback range [min, max] (inclusive) for fields not in `fields`.
    pub default: [u32; 2],
    /// Per-field ranges.
    pub fields: HashMap<String, [u32; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.clickhouse.url, "http://localhost:8123");
        assert_eq!(config.generation.organizations, 2);
        assert!(config.generation.roots.is_empty());
    }

    #[test]
    fn test_yaml_roundtrip() {
        let config = Config::default();
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.clickhouse.url, config.clickhouse.url);
    }

    #[test]
    fn test_partial_yaml() {
        let yaml = r#"
clickhouse:
  url: http://ch:8123
generation:
  organizations: 5
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.clickhouse.url, "http://ch:8123");
        assert_eq!(config.generation.organizations, 5);
        assert_eq!(config.evaluation.sample_size, 100);
    }

    #[test]
    fn test_edge_ratio_count() {
        let ratio = EdgeRatio::Count(5);
        let mut rng = rand::rng();
        assert_eq!(ratio.sample(&mut rng), 5);
    }

    #[test]
    fn test_edge_ratio_probability() {
        let ratio = EdgeRatio::Probability(1.0);
        let mut rng = rand::rng();
        assert_eq!(ratio.sample(&mut rng), 1);

        let ratio_zero = EdgeRatio::Probability(0.0);
        assert_eq!(ratio_zero.sample(&mut rng), 0);
    }

    #[test]
    fn test_relationship_config_parse_variant_key() {
        let (source, target) = RelationshipConfig::parse_variant_key("Group -> Project").unwrap();
        assert_eq!(source, "Group");
        assert_eq!(target, "Project");

        let (source, target) =
            RelationshipConfig::parse_variant_key("MergeRequest->Pipeline").unwrap();
        assert_eq!(source, "MergeRequest");
        assert_eq!(target, "Pipeline");

        assert!(RelationshipConfig::parse_variant_key("Invalid").is_none());
    }

    #[test]
    fn test_namespace_entity_default() {
        let config = Config::default();
        assert_eq!(config.generation.namespace_entity, "Group");
    }

    #[test]
    fn test_namespace_entity_custom() {
        let yaml = r#"
clickhouse:
  url: http://localhost:8123
generation:
  namespace_entity: Namespace
  organizations: 1
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.generation.namespace_entity, "Namespace");
    }

    #[test]
    fn test_namespace_entity_absent_uses_default() {
        let yaml = r#"
clickhouse:
  url: http://localhost:8123
generation:
  organizations: 3
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.generation.namespace_entity, "Group");
        assert_eq!(config.generation.organizations, 3);
    }

    #[test]
    fn test_relationship_config_yaml() {
        let yaml = r#"
clickhouse:
  url: http://localhost:8123
generation:
  roots:
    User: 100
    Group: 50
  relationships:
    CONTAINS:
      "Group -> Group": 3
      "Group -> Project": 5
    IN_PROJECT:
      "MergeRequest -> Project": 30
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(config.generation.roots.get("User"), Some(&100));
        assert_eq!(config.generation.roots.get("Group"), Some(&50));

        let contains = config
            .generation
            .relationships
            .edges
            .get("CONTAINS")
            .unwrap();
        assert!(matches!(
            contains.get("Group -> Group"),
            Some(EdgeRatio::Count(3))
        ));
        assert!(matches!(
            contains.get("Group -> Project"),
            Some(EdgeRatio::Count(5))
        ));

        let in_project = config
            .generation
            .relationships
            .edges
            .get("IN_PROJECT")
            .unwrap();
        assert!(matches!(
            in_project.get("MergeRequest -> Project"),
            Some(EdgeRatio::Count(30))
        ));
    }

    #[test]
    fn test_fake_data_config_loads_yaml() {
        let config = FakeDataConfig::load(crate::synth::constants::DEFAULT_FAKE_DATA_PATH).unwrap();
        assert!(!config.strings.pools.name_prefixes.is_empty());
        assert!(!config.strings.classify.is_empty());
        assert!(!config.bools.fields.is_empty());
        assert!(!config.ints.fields.is_empty());
    }

    #[test]
    fn test_fake_data_config_rejects_partial_yaml() {
        let yaml = r#"
strings:
  pools:
    statuses:
      - "custom1"
"#;
        let result: Result<FakeDataConfig, _> = serde_yaml::from_str(yaml);
        assert!(
            result.is_err(),
            "Partial YAML should be rejected — all fields are mandatory"
        );
    }

    /// Build a minimal valid FakeDataConfig for testing validation.
    fn minimal_fake_data_config() -> FakeDataConfig {
        FakeDataConfig {
            strings: FakeDataStrings {
                pools: FakeDataStringPools {
                    name_prefixes: vec!["a".into()],
                    email_domains: vec!["@x.co".into()],
                    description_words: vec!["w".into()],
                    statuses: vec!["open".into()],
                    states: vec!["active".into()],
                    branch_prefixes: vec!["fix/".into()],
                },
                classify: vec![],
            },
            bools: FakeDataBools {
                default: 0.5,
                fields: HashMap::new(),
            },
            ints: FakeDataInts {
                default: [1, 100],
                fields: HashMap::new(),
            },
        }
    }

    #[test]
    fn test_fake_data_validate() {
        // Valid config passes.
        minimal_fake_data_config().validate().unwrap();

        // Empty string pool.
        let mut cfg = minimal_fake_data_config();
        cfg.strings.pools.name_prefixes = vec![];
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("name_prefixes must not be empty")
        );

        // Negative probability.
        let mut cfg = minimal_fake_data_config();
        cfg.bools.default = -0.1;
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("bools.default")
        );

        // Probability > 1.
        let mut cfg = minimal_fake_data_config();
        cfg.bools.fields.insert("bad".into(), 1.5);
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("bools.fields.bad")
        );

        // NaN probability.
        let mut cfg = minimal_fake_data_config();
        cfg.bools.default = f64::NAN;
        assert!(cfg.validate().unwrap_err().to_string().contains("finite"));

        // Inverted int range.
        let mut cfg = minimal_fake_data_config();
        cfg.ints.default = [100, 1];
        assert!(
            cfg.validate()
                .unwrap_err()
                .to_string()
                .contains("min (100) must be <= max (1)")
        );

        // Overflowing int range.
        let mut cfg = minimal_fake_data_config();
        cfg.ints.fields.insert("huge".into(), [0, u32::MAX]);
        assert!(cfg.validate().unwrap_err().to_string().contains("overflow"));
    }
}

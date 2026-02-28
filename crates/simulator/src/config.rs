//! Configuration for the simulator.

use anyhow::{Context, Result};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

/// Root configuration for the simulator.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct Config {
    /// ClickHouse connection and schema settings.
    pub clickhouse: ClickHouseConfig,
    /// Data generation settings.
    pub generation: GenerationConfig,
    /// Evaluation settings.
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
    /// PRIMARY KEY columns for node tables (sparse index, not uniqueness constraint).
    /// If empty, defaults to ORDER BY columns.
    #[serde(default)]
    pub node_primary_key: Vec<String>,
    /// ORDER BY columns for node tables (physical sort order on disk).
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
    vec!["traversal_path".to_string(), "id".to_string()]
}

fn default_edge_order_by() -> Vec<String> {
    vec![
        "traversal_path".to_string(),
        "source_id".to_string(),
        "source_kind".to_string(),
        "relationship_kind".to_string(),
    ]
}

fn default_index_granularity() -> u32 {
    8192
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            engine: default_engine_type(),
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

/// Subgroup hierarchy generation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubgroupConfig {
    /// Maximum depth of subgroup hierarchy (0 = no subgroups).
    #[serde(default)]
    pub max_depth: usize,
    /// Number of subgroups per parent group at each level.
    #[serde(default = "default_subgroups_per_group")]
    pub per_group: usize,
}

fn default_subgroups_per_group() -> usize {
    2
}

impl Default for SubgroupConfig {
    fn default() -> Self {
        Self {
            max_depth: 0,
            per_group: default_subgroups_per_group(),
        }
    }
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

    /// Subgroup hierarchy configuration.
    /// Controls recursive Group -> Group generation.
    #[serde(default)]
    pub subgroups: SubgroupConfig,

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
}

fn default_output_dir() -> String {
    "data".to_string()
}

fn default_ontology_path() -> String {
    "fixtures/ontology".to_string()
}

fn default_organizations() -> u32 {
    2
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
            subgroups: SubgroupConfig::default(),
            associations: AssociationConfig::default(),
            batch_size: default_batch_size(),
            parallel: false,
            seed: None,
        }
    }
}

/// Edge ratio for relationship-based generation.
///
/// Can be either:
/// - An integer count (e.g., 5 children per parent)
/// - A fractional probability (e.g., 0.3 = 30% chance of creating edge)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeRatio {
    /// Fixed count of children per parent.
    Count(usize),
    /// Probability of creating the relationship (0.0-1.0).
    Probability(f64),
}

impl EdgeRatio {
    /// Sample a count from this ratio.
    pub fn sample(&self, rng: &mut impl Rng) -> usize {
        match self {
            EdgeRatio::Count(n) => *n,
            EdgeRatio::Probability(p) => {
                if rng.gen_bool(*p) {
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
            EdgeRatio::Count(n) => {
                let min = (*n as f64 * 0.5).ceil() as usize;
                let max = (*n as f64 * 1.5).ceil() as usize;
                rng.gen_range(min.max(1)..=max.max(1))
            }
            EdgeRatio::Probability(p) => {
                if rng.gen_bool(*p) {
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
///     "MergeRequest -> User":             # 30% of MRs have a merger
///       ratio: 0.3
///       per: source                       # Iterate over MRs, not Users
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
    /// Path to queries JSON file.
    #[serde(default = "default_queries_path")]
    pub queries_path: String,
    /// Number of IDs to sample per entity type.
    #[serde(default = "default_sample_size")]
    pub sample_size: usize,
    /// Number of iterations to run each query.
    #[serde(default = "default_iterations")]
    pub iterations: usize,
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
}

fn default_queries_path() -> String {
    "fixtures/queries/sdlc_queries.json".to_string()
}

fn default_sample_size() -> usize {
    100
}

fn default_iterations() -> usize {
    1
}

impl Default for EvaluationConfig {
    fn default() -> Self {
        Self {
            queries_path: default_queries_path(),
            sample_size: default_sample_size(),
            iterations: default_iterations(),
            skip_cache_warm: false,
            filter: None,
            output: OutputConfig::default(),
            metadata_dir: None,
        }
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

impl GenerationConfig {
    /// Convert to synthetic-graph's GenerationConfig.
    pub fn to_synthetic_graph_config(&self) -> synthetic_graph::config::GenerationConfig {
        synthetic_graph::config::GenerationConfig {
            organizations: self.organizations,
            roots: self.roots.clone(),
            relationships: synthetic_graph::config::RelationshipConfig {
                edges: self
                    .relationships
                    .edges
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.iter()
                                .map(|(vk, ratio)| (vk.clone(), convert_edge_ratio(ratio)))
                                .collect(),
                        )
                    })
                    .collect(),
            },
            associations: synthetic_graph::config::AssociationConfig {
                edges: self
                    .associations
                    .edges
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.iter()
                                .map(|(vk, val)| (vk.clone(), convert_association_value(val)))
                                .collect(),
                        )
                    })
                    .collect(),
            },
            subgroups: synthetic_graph::config::SubgroupConfig {
                max_depth: self.subgroups.max_depth,
                per_group: self.subgroups.per_group,
            },
            batch_size: self.batch_size,
            seed: self.seed.unwrap_or(42),
        }
    }
}

fn convert_edge_ratio(ratio: &EdgeRatio) -> synthetic_graph::config::EdgeRatio {
    match ratio {
        EdgeRatio::Count(n) => synthetic_graph::config::EdgeRatio::Count(*n),
        EdgeRatio::Probability(p) => synthetic_graph::config::EdgeRatio::Probability(*p),
    }
}

fn convert_association_value(
    val: &AssociationEdgeValue,
) -> synthetic_graph::config::AssociationEdgeValue {
    match val {
        AssociationEdgeValue::Simple(ratio) => {
            synthetic_graph::config::AssociationEdgeValue::Simple(convert_edge_ratio(ratio))
        }
        AssociationEdgeValue::Extended(ext) => {
            synthetic_graph::config::AssociationEdgeValue::Extended(
                synthetic_graph::config::AssociationEdgeExtended {
                    ratio: convert_edge_ratio(&ext.ratio),
                    iterate: match ext.per {
                        IterationDirection::Target => {
                            synthetic_graph::config::IterationDirection::Target
                        }
                        IterationDirection::Source => {
                            synthetic_graph::config::IterationDirection::Source
                        }
                    },
                },
            )
        }
    }
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
        let mut rng = rand::thread_rng();
        assert_eq!(ratio.sample(&mut rng), 5);
    }

    #[test]
    fn test_edge_ratio_probability() {
        let ratio = EdgeRatio::Probability(1.0);
        let mut rng = rand::thread_rng();
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
}

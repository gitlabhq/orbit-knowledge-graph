//! Configuration for the simulator.

use anyhow::{Context, Result};
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

    /// Get the count for a specific node type.
    pub fn node_count(&self, node_type: &str) -> usize {
        self.generation
            .nodes
            .counts
            .get(node_type)
            .copied()
            .unwrap_or(self.generation.nodes.default_per_type)
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

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_database(),
            client: ClientConfig::default(),
            schema: SchemaConfig::default(),
        }
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
    vec!["organization_id".to_string(), "id".to_string()]
}

fn default_edge_order_by() -> Vec<String> {
    vec![
        "source_kind".to_string(),
        "source".to_string(),
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
    /// Traversal ID settings.
    #[serde(default)]
    pub traversal: TraversalConfig,
    /// Node generation settings.
    #[serde(default)]
    pub nodes: NodeGenerationConfig,
    /// Edge generation settings.
    #[serde(default)]
    pub edges: EdgeGenerationConfig,
    /// Batch size for Parquet row groups.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Run generation in parallel across organizations.
    #[serde(default)]
    pub parallel: bool,
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
            traversal: TraversalConfig::default(),
            nodes: NodeGenerationConfig::default(),
            edges: EdgeGenerationConfig::default(),
            batch_size: default_batch_size(),
            parallel: false,
        }
    }
}

/// Traversal ID generation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TraversalConfig {
    /// Number of traversal IDs per organization.
    #[serde(default = "default_ids_per_org")]
    pub ids_per_org: usize,
    /// Maximum depth of traversal ID hierarchy.
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
}

fn default_ids_per_org() -> usize {
    1000
}

fn default_max_depth() -> usize {
    5
}

impl Default for TraversalConfig {
    fn default() -> Self {
        Self {
            ids_per_org: default_ids_per_org(),
            max_depth: default_max_depth(),
        }
    }
}

/// Node generation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeGenerationConfig {
    /// Default number of nodes per type.
    #[serde(default = "default_per_type")]
    pub default_per_type: usize,
    /// Override counts for specific node types.
    #[serde(default)]
    pub counts: HashMap<String, usize>,
}

fn default_per_type() -> usize {
    100
}

impl Default for NodeGenerationConfig {
    fn default() -> Self {
        Self {
            default_per_type: default_per_type(),
            counts: HashMap::new(),
        }
    }
}

/// Edge generation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeGenerationConfig {
    /// Number of edges per source node.
    #[serde(default = "default_per_source")]
    pub per_source: usize,
}

fn default_per_source() -> usize {
    3
}

impl Default for EdgeGenerationConfig {
    fn default() -> Self {
        Self {
            per_source: default_per_source(),
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.clickhouse.url, "http://localhost:8123");
        assert_eq!(config.generation.organizations, 2);
        assert_eq!(config.generation.nodes.default_per_type, 100);
    }

    #[test]
    fn test_yaml_roundtrip() {
        let config = Config::default();
        let yaml = serde_yaml::to_string(&config).unwrap();
        let parsed: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(parsed.clickhouse.url, config.clickhouse.url);
    }

    #[test]
    fn test_node_count() {
        let config = Config {
            generation: GenerationConfig {
                nodes: NodeGenerationConfig {
                    default_per_type: 200,
                    counts: [("User".to_string(), 500)].into_iter().collect(),
                },
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(config.node_count("User"), 500);
        assert_eq!(config.node_count("Project"), 200);
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
        // Defaults should be filled in
        assert_eq!(config.generation.nodes.default_per_type, 100);
        assert_eq!(config.evaluation.sample_size, 100);
    }
}

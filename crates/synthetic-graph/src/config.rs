use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// Root configuration for synthetic graph generation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphConfig {
    /// Path to the ontology directory (defaults to embedded ontology).
    #[serde(default)]
    pub ontology_path: Option<String>,
    pub generation: GenerationConfig,
    #[serde(default)]
    pub continuous: ContinuousConfig,
    #[serde(default)]
    pub state: StateConfig,
}

impl GraphConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Self = serde_yaml::from_str(&content)?;
        Ok(config)
    }
}

/// Controls how many entities of each type to generate and how they relate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerationConfig {
    /// Number of organizations (each is an independent graph).
    #[serde(default = "default_organizations")]
    pub organizations: u32,
    /// Root entity counts keyed by node type name (e.g., `"User": 100`).
    #[serde(default)]
    pub roots: HashMap<String, usize>,
    /// Relationship edges that drive child entity generation.
    #[serde(default)]
    pub relationships: RelationshipConfig,
    /// Association edges that link existing entities without creating new ones.
    #[serde(default)]
    pub associations: AssociationConfig,
    /// Subgroup hierarchy settings (Group -> Group nesting).
    #[serde(default)]
    pub subgroups: SubgroupConfig,
    /// Rows per Arrow RecordBatch during generation.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// RNG seed for reproducible generation.
    #[serde(default = "default_seed")]
    pub seed: u64,
}

/// Subgroup hierarchy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubgroupConfig {
    /// Maximum nesting depth for subgroups.
    #[serde(default = "default_subgroup_depth")]
    pub max_depth: usize,
    /// Number of subgroups per parent group.
    #[serde(default = "default_subgroup_per_group")]
    pub per_group: usize,
}

impl Default for SubgroupConfig {
    fn default() -> Self {
        Self {
            max_depth: default_subgroup_depth(),
            per_group: default_subgroup_per_group(),
        }
    }
}

/// Relationship-based generation: edges that create new child entities.
///
/// Structure: `edge_type -> "Source -> Target" -> ratio`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RelationshipConfig {
    #[serde(default)]
    pub edges: HashMap<String, HashMap<String, EdgeRatio>>,
}

impl RelationshipConfig {
    /// Parse `"Source -> Target"` into `(source, target)`.
    pub fn parse_variant_key(key: &str) -> Option<(String, String)> {
        let parts: Vec<&str> = key.split("->").map(str::trim).collect();
        if parts.len() == 2 {
            Some((parts[0].to_string(), parts[1].to_string()))
        } else {
            None
        }
    }

    /// Flatten into `(edge_type, source, target, ratio)` tuples.
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

/// Association-based generation: edges linking existing entities.
///
/// Structure: `edge_type -> "Source -> Target" -> value`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AssociationConfig {
    #[serde(default)]
    pub edges: HashMap<String, HashMap<String, AssociationEdgeValue>>,
}

impl AssociationConfig {
    pub fn all_associations(&self) -> Vec<(String, String, String, EdgeRatio, IterationDirection)> {
        let mut result = Vec::new();
        for (edge_type, variants) in &self.edges {
            for (variant_key, value) in variants {
                if let Some((source, target)) = RelationshipConfig::parse_variant_key(variant_key) {
                    let (ratio, direction) = match value {
                        AssociationEdgeValue::Simple(r) => (r.clone(), IterationDirection::Target),
                        AssociationEdgeValue::Extended(ext) => {
                            (ext.ratio.clone(), ext.iterate.clone())
                        }
                    };
                    result.push((edge_type.clone(), source, target, ratio, direction));
                }
            }
        }
        result
    }
}

/// How many children or association edges to create per parent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EdgeRatio {
    Count(usize),
    Probability(f64),
}

impl EdgeRatio {
    pub fn sample(&self, rng: &mut impl Rng) -> usize {
        match self {
            EdgeRatio::Count(n) => *n,
            EdgeRatio::Probability(p) => {
                if rng.r#gen::<f64>() < *p {
                    1
                } else {
                    0
                }
            }
        }
    }

    /// Sample with variance: for counts, returns a value in `[n*0.5, n*1.5]`.
    pub fn sample_with_variance(&self, rng: &mut impl Rng) -> usize {
        match self {
            EdgeRatio::Count(n) => {
                if *n <= 1 {
                    return *n;
                }
                let min = (*n as f64 * 0.5) as usize;
                let max = (*n as f64 * 1.5) as usize;
                rng.gen_range(min..=max)
            }
            EdgeRatio::Probability(p) => {
                if rng.r#gen::<f64>() < *p {
                    1
                } else {
                    0
                }
            }
        }
    }
}

/// Which side of an association edge to iterate over.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IterationDirection {
    #[default]
    Target,
    Source,
}

/// An association edge value can be a simple ratio or extended with direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AssociationEdgeValue {
    Simple(EdgeRatio),
    Extended(AssociationEdgeExtended),
}

/// Extended association edge with iteration direction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociationEdgeExtended {
    pub ratio: EdgeRatio,
    #[serde(default)]
    pub iterate: IterationDirection,
}

/// Continuous mode: cyclic insert/update/delete traffic against an existing graph.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Number of cycles to run (0 = infinite).
    #[serde(default)]
    pub cycles: usize,
    /// Seconds to sleep between cycles.
    #[serde(default = "default_cycle_interval")]
    pub cycle_interval_secs: u64,
    /// Entity type -> count of inserts per cycle.
    #[serde(default)]
    pub inserts_per_cycle: HashMap<String, usize>,
    /// Entity type -> count of updates per cycle.
    #[serde(default)]
    pub updates_per_cycle: HashMap<String, usize>,
    /// Entity type -> count of deletes per cycle.
    #[serde(default)]
    pub deletes_per_cycle: HashMap<String, usize>,
    /// Field value overrides per entity type.
    #[serde(default)]
    pub field_overrides: HashMap<String, HashMap<String, Vec<serde_json::Value>>>,
}

impl Default for ContinuousConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cycles: 0,
            cycle_interval_secs: default_cycle_interval(),
            inserts_per_cycle: HashMap::new(),
            updates_per_cycle: HashMap::new(),
            deletes_per_cycle: HashMap::new(),
            field_overrides: HashMap::new(),
        }
    }
}

/// State persistence configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    /// Directory for state files.
    #[serde(default = "default_state_dir")]
    pub dir: String,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            dir: default_state_dir(),
        }
    }
}

fn default_organizations() -> u32 {
    1
}
fn default_batch_size() -> usize {
    10_000
}
fn default_seed() -> u64 {
    42
}
fn default_subgroup_depth() -> usize {
    2
}
fn default_subgroup_per_group() -> usize {
    2
}
fn default_cycle_interval() -> u64 {
    5
}
fn default_state_dir() -> String {
    "synthetic-graph-state".to_string()
}

/// Per-entity-type field overrides for the datalake generator's table-based approach.
/// Maps entity type name -> field name -> pool of JSON values.
pub type FieldOverrides = HashMap<String, HashMap<String, Vec<serde_json::Value>>>;

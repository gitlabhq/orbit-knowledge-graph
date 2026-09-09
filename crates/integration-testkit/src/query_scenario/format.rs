use std::collections::BTreeMap;

use serde::Deserialize;

use crate::scenario::Seed;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryScenario {
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub config: ScenarioConfig,
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub expect: QueryExpect,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioConfig {
    #[serde(default)]
    pub extra_seed: Seed,
    #[serde(default)]
    pub security: Option<PresetOr<SecurityOverride>>,
    #[serde(default)]
    pub redaction: Option<PresetOr<RedactionConfig>>,
    #[serde(default)]
    pub max_response_bytes: Option<usize>,
}

/// Either a preset name (string) or an inline value.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PresetOr<T> {
    Preset(String),
    Inline(T),
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityOverride {
    #[serde(default)]
    pub admin: Option<bool>,
    #[serde(default)]
    pub paths: Option<Vec<String>>,
    #[serde(default)]
    pub org_id: Option<i64>,
    #[serde(default)]
    pub access_level: Option<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedactionConfig {
    #[serde(default)]
    pub allow: BTreeMap<String, Vec<i64>>,
    #[serde(default)]
    pub deny: BTreeMap<String, Vec<i64>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryExpect {
    #[serde(default)]
    pub compile_error: Option<CompileErrorExpect>,
    #[serde(default)]
    pub node_count: Option<usize>,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeExpect>,
    #[serde(default)]
    pub edges: BTreeMap<String, Vec<[i64; 2]>>,
    #[serde(default)]
    pub edge_count: BTreeMap<String, usize>,
    #[serde(default)]
    pub referential_integrity: bool,
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub skip_requirements: Vec<String>,
    #[serde(default)]
    pub pages: Vec<BTreeMap<String, Vec<i64>>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExpect {
    #[serde(default)]
    pub count: Option<usize>,
    #[serde(default)]
    pub order: Option<Vec<i64>>,
    #[serde(default)]
    pub ids: Option<Vec<i64>>,
    #[serde(default)]
    pub absent: Option<Vec<i64>>,
    /// Per-field filter assertions: `{ state: blocked }` verifies every
    /// returned node of this entity has `state == "blocked"`.
    #[serde(default)]
    pub filters: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
}

impl QueryExpect {
    /// Derive a total node count from per-entity specs when `node_count` is
    /// not set explicitly. Returns `None` when no entity carries a countable
    /// spec (count, order, or ids).
    pub fn derived_node_count(&self) -> Option<usize> {
        let total: usize = self
            .nodes
            .values()
            .filter_map(|ne| {
                ne.count
                    .or(ne.order.as_ref().map(Vec::len))
                    .or(ne.ids.as_ref().map(Vec::len))
            })
            .sum();
        (total > 0).then_some(total)
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CompileErrorExpect {
    Flag(bool),
    Substring(String),
}

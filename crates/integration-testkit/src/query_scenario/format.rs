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
    /// Uniform paths (all share the same access_level).
    #[serde(default)]
    pub paths: Option<Vec<String>>,
    /// Per-path access levels: `[{path: "1/100/", access_level: 20}]`
    #[serde(default)]
    pub authorized_paths: Option<Vec<AuthorizedPathSpec>>,
    #[serde(default)]
    pub org_id: Option<i64>,
    #[serde(default)]
    pub access_level: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedPathSpec {
    pub path: String,
    pub access_level: u32,
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
    pub compile_only: bool,
    #[serde(default)]
    pub compile_error: Option<CompileErrorExpect>,
    #[serde(default)]
    pub node_count: Option<usize>,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeExpect>,
    /// Exact edge set: `{ MEMBER_OF: [[1,100], [2,100]] }`
    #[serde(default)]
    pub edges: BTreeMap<String, Vec<[i64; 2]>>,
    /// Spot-check edges exist (subset): `{ AUTHORED: [[1,2000], [2,2002]] }`
    #[serde(default)]
    pub edge_exists: BTreeMap<String, Vec<[i64; 2]>>,
    /// Assert edges do NOT exist: `{ MEMBER_OF: [[1,102]] }`
    #[serde(default)]
    pub edge_absent: BTreeMap<String, Vec<[i64; 2]>>,
    #[serde(default)]
    pub edge_count: BTreeMap<String, usize>,
    #[serde(default)]
    pub groups: BTreeMap<String, GroupExpect>,
    #[serde(default)]
    pub empty_aggregation: bool,
    /// Assert row count for ungrouped/property-grouped aggregation results.
    #[serde(default)]
    pub row_count: Option<usize>,
    /// Assert values on rows by index: `[{index: 0, col: val}]`
    #[serde(default)]
    pub row_values: Vec<BTreeMap<String, serde_json::Value>>,
    #[serde(default)]
    pub sql_contains: Vec<String>,
    #[serde(default)]
    pub sql_not_contains: Vec<String>,
    /// Assert the number of paths returned by a path_finding query.
    #[serde(default)]
    pub path_count: Option<usize>,
    #[serde(default)]
    pub referential_integrity: bool,
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub skip_requirements: Vec<String>,
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
    /// Assert these properties exist on every node of this entity.
    #[serde(default)]
    pub prop_present: Vec<String>,
    /// Assert these properties are absent on every node of this entity.
    #[serde(default)]
    pub prop_absent: Vec<String>,
    #[serde(default)]
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupExpect {
    #[serde(default)]
    pub entity: Option<String>,
    #[serde(default)]
    pub ids: Option<Vec<i64>>,
    #[serde(default)]
    pub rows: Vec<GroupRowExpect>,
    #[serde(default)]
    pub absent: Vec<GroupAbsentExpect>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupAbsentExpect {
    pub entity: String,
    pub id: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GroupRowExpect {
    pub entity: String,
    pub id: i64,
    #[serde(default)]
    pub values: BTreeMap<String, serde_json::Value>,
    #[serde(default)]
    pub properties: BTreeMap<String, serde_json::Value>,
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

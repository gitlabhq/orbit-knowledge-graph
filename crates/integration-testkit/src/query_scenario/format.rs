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
    /// Per-alias scope prefixes: `{ g: "1/700/", p: "1/700/" }`
    #[serde(default)]
    pub scope_prefixes: std::collections::BTreeMap<String, String>,
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
    /// Assert error message does NOT contain these substrings.
    #[serde(default)]
    pub compile_error_not_contains: Vec<String>,
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
    /// Assert total edge count across all types.
    #[serde(default)]
    pub total_edge_count: Option<usize>,
    /// Assert the number of paths returned by a path_finding query.
    #[serde(default)]
    pub path_count: Option<usize>,
    /// Assert path destinations: `{ Project: [1000, 1004] }`.
    /// Collects the `to_id` of the last edge in each path, grouped by
    /// `to` entity type. Compared as sorted sets.
    #[serde(default)]
    pub path_destinations: BTreeMap<String, Vec<i64>>,
    /// Assert per-path edge structure: each entry is one path's edges
    /// in step order. `{ from: User, from_id: 1, type: MEMBER_OF, to: Group, to_id: 100 }`
    #[serde(default)]
    pub path_edges: Vec<Vec<PathEdgeExpect>>,
    /// Assert entities that must NOT appear as path edge endpoints.
    #[serde(default)]
    pub path_endpoint_absent: Vec<String>,
    #[serde(default)]
    pub referential_integrity: bool,
    #[serde(default)]
    pub has_more: Option<bool>,
    /// Assertions across ALL pages combined. The runner collects node IDs
    /// and edge tuples from every page and asserts at the end.
    #[serde(default)]
    pub all_pages: Option<AllPagesExpect>,
    /// Multi-page pagination: the runner chains cursors automatically.
    #[serde(default)]
    pub pages: Vec<QueryExpect>,
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
    pub count: Option<usize>,
    #[serde(default)]
    pub order: Option<Vec<i64>>,
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

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AllPagesExpect {
    /// Expected node IDs collected across all pages, sorted.
    #[serde(default)]
    pub node_ids: BTreeMap<String, Vec<i64>>,
    /// Expected group node IDs collected across all pages, sorted.
    /// Key is "group_key:EntityType", e.g. "u:User".
    #[serde(default)]
    pub group_node_ids: BTreeMap<String, Vec<i64>>,
    /// Expected total edge count across all pages (after dedup).
    #[serde(default)]
    pub edge_count: Option<usize>,
    /// Expected number of pages.
    #[serde(default)]
    pub page_count: Option<usize>,
    /// Assert no node ID appears on multiple pages.
    #[serde(default)]
    pub no_duplicate_ids: bool,
}

impl QueryExpect {
    /// Panics if `pages` is set alongside result-level assertions that would
    /// be silently ignored.
    pub fn validate_pages_exclusive(&self, scenario: &str) {
        if self.pages.is_empty() {
            return;
        }
        let has_result_fields = self.node_count.is_some()
            || !self.nodes.is_empty()
            || !self.edges.is_empty()
            || !self.edge_exists.is_empty()
            || !self.edge_absent.is_empty()
            || !self.edge_count.is_empty()
            || !self.groups.is_empty()
            || self.empty_aggregation
            || self.row_count.is_some()
            || !self.row_values.is_empty()
            || self.total_edge_count.is_some()
            || self.path_count.is_some()
            || !self.path_destinations.is_empty()
            || !self.path_edges.is_empty()
            || !self.path_endpoint_absent.is_empty()
            || self.referential_integrity
            || self.has_more.is_some();
        assert!(
            !has_result_fields,
            "{scenario}: pages is set alongside top-level result assertions; \
             move them into the per-page expect or remove them"
        );
    }

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

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathEdgeExpect {
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub from_id: Option<i64>,
    #[serde(rename = "type")]
    #[serde(default)]
    pub edge_type: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub to_id: Option<i64>,
}

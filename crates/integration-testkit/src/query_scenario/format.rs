use std::collections::BTreeMap;

use query_engine::compiler::QueryType;
use serde::Deserialize;

use crate::scenario::Seed;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryScenario {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub config: ScenarioConfig,
    #[serde(default)]
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub expect: Option<QueryExpect>,
    #[serde(default)]
    pub steps: Vec<QueryStep>,
}

/// One query of a multi-step scenario. `seed` rows are inserted before the
/// query runs, so a later step can observe a write made after an earlier one.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryStep {
    #[serde(default)]
    pub seed: Seed,
    pub query: BTreeMap<String, String>,
    pub expect: QueryExpect,
}

impl QueryScenario {
    pub fn into_steps(self) -> (ScenarioConfig, Vec<QueryStep>) {
        let steps = match (self.query.is_empty(), self.steps.is_empty()) {
            (false, true) => vec![QueryStep {
                seed: Seed::default(),
                query: self.query,
                expect: self.expect.expect("scenario declares query without expect"),
            }],
            (true, false) => self.steps,
            (false, false) => panic!("scenario declares both query/expect and steps:"),
            (true, true) => panic!("scenario declares neither query nor steps:"),
        };
        (self.config, steps)
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioConfig {
    pub ontology_overlay: Option<String>,
    #[serde(default)]
    pub extra_seed: Seed,
    #[serde(default)]
    pub unmerged_seed: bool,
    #[serde(default)]
    pub security: Option<PresetOr<SecurityOverride>>,
    #[serde(default)]
    pub redaction: Option<PresetOr<RedactionConfig>>,
    #[serde(default)]
    pub duckdb: bool,
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
    /// Run the query N times and assert all responses are identical.
    #[serde(default)]
    pub repeat_count: Option<usize>,
    #[serde(default)]
    pub compile_error: Option<CompileErrorExpect>,
    #[serde(default)]
    pub compile_error_contains: MessageExpect,
    #[serde(default)]
    pub compile_error_not_contains: MessageExpect,
    #[serde(default)]
    pub node_count: Option<usize>,
    #[serde(default)]
    pub nodes: BTreeMap<String, NodeExpect>,
    /// Exact edge set: `{ MEMBER_OF: [[1,100], [2,100]] }`
    #[serde(default)]
    pub edges: BTreeMap<String, Vec<[i64; 2]>>,
    #[serde(default)]
    pub edge_exists: BTreeMap<String, Vec<EdgeExpect>>,
    #[serde(default)]
    pub edge_absent: BTreeMap<String, Vec<EdgeExpect>>,
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
    /// Assert group column metadata: `{ v_severity: "v.severity" }` verifies
    /// the response declares a group column named `v_severity` backed by
    /// node `v`, property `severity`.
    #[serde(default)]
    pub group_columns: BTreeMap<String, String>,
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
    #[serde(default)]
    pub gql_columns: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub gql_column_names: Vec<String>,
    /// Assertions across ALL pages combined. The runner collects node IDs
    /// and edge tuples from every page and asserts at the end.
    #[serde(default)]
    pub all_pages: Option<AllPagesExpect>,
    /// Multi-page pagination: the runner chains cursors automatically.
    #[serde(default)]
    pub pages: Vec<QueryExpect>,
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
    #[serde(default)]
    pub node_order: BTreeMap<String, Vec<i64>>,
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
    #[serde(default)]
    pub no_duplicate_edges: bool,
    #[serde(default)]
    pub matches_unpaginated: bool,
}

impl QueryExpect {
    pub fn validate_pages_exclusive(&self, scenario: &str) {
        let has_result_fields = self.has_result_assertions();
        if self.pages.is_empty() {
            assert!(
                self.all_pages.is_none(),
                "{scenario}: all_pages requires pages"
            );
            return;
        }
        assert!(
            !has_result_fields && self.repeat_count.is_none(),
            "{scenario}: pages is set alongside top-level result assertions; \
             move them into the per-page expect or remove them"
        );
        for (index, page) in self.pages.iter().enumerate() {
            assert!(
                page.repeat_count.is_none()
                    && page.compile_error.is_none()
                    && !page.compile_error_contains.is_configured()
                    && !page.compile_error_not_contains.is_configured()
                    && page.pages.is_empty()
                    && page.all_pages.is_none(),
                "{scenario}: page {} supports only result assertions",
                index + 1
            );
        }
    }

    pub(super) fn validate_response_type(&self, query_type: QueryType, label: &str) {
        assert!(
            query_type == QueryType::Aggregation || !self.has_aggregation_assertions(),
            "{label}: aggregation assertions require an aggregation query"
        );
        assert!(
            query_type == QueryType::PathFinding || !self.has_path_assertions(),
            "{label}: path assertions require a path_finding query"
        );
    }

    fn has_aggregation_assertions(&self) -> bool {
        !self.groups.is_empty()
            || !self.group_columns.is_empty()
            || self.empty_aggregation
            || self.row_count.is_some()
            || !self.row_values.is_empty()
    }

    fn has_path_assertions(&self) -> bool {
        self.path_count.is_some()
            || !self.path_destinations.is_empty()
            || !self.path_edges.is_empty()
            || !self.path_endpoint_absent.is_empty()
    }

    fn has_result_assertions(&self) -> bool {
        self.node_count.is_some()
            || !self.nodes.is_empty()
            || !self.edges.is_empty()
            || !self.edge_exists.is_empty()
            || !self.edge_absent.is_empty()
            || !self.edge_count.is_empty()
            || self.has_aggregation_assertions()
            || self.total_edge_count.is_some()
            || self.has_path_assertions()
            || self.referential_integrity
            || self.has_more.is_some()
            || !self.gql_columns.is_empty()
            || !self.gql_column_names.is_empty()
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
    PerFrontend(BTreeMap<String, String>),
}

impl CompileErrorExpect {
    pub fn substring_for(&self, frontend_key: &str) -> Option<&str> {
        match self {
            Self::Flag(_) => None,
            Self::Substring(sub) => Some(sub),
            Self::PerFrontend(by_frontend) => Some(
                by_frontend
                    .get(frontend_key)
                    .unwrap_or_else(|| panic!("compile_error missing frontend '{frontend_key}'"))
                    .as_str(),
            ),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MessageExpect {
    All(Vec<String>),
    PerFrontend(BTreeMap<String, Vec<String>>),
}

impl Default for MessageExpect {
    fn default() -> Self {
        Self::All(Vec::new())
    }
}

impl MessageExpect {
    fn is_configured(&self) -> bool {
        !matches!(self, Self::All(fragments) if fragments.is_empty())
    }

    pub fn for_frontend(&self, frontend_key: &str) -> &[String] {
        match self {
            Self::All(fragments) => fragments,
            Self::PerFrontend(by_frontend) => by_frontend
                .get(frontend_key)
                .unwrap_or_else(|| {
                    panic!("compile error message expectation missing frontend '{frontend_key}'")
                })
                .as_slice(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum EdgeExpect {
    Pair([i64; 2]),
    Typed(TypedEdgeExpect),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TypedEdgeExpect {
    pub from: String,
    pub from_id: i64,
    pub to: String,
    pub to_id: i64,
}

impl EdgeExpect {
    pub fn matches(&self, edge: &query_engine::formatters::GraphEdge) -> bool {
        match self {
            Self::Pair([from, to]) => edge.from_id == *from && edge.to_id == *to,
            Self::Typed(expected) => {
                edge.from == expected.from
                    && edge.from_id == expected.from_id
                    && edge.to == expected.to
                    && edge.to_id == expected.to_id
            }
        }
    }
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
    #[serde(default)]
    pub step: Option<usize>,
}

impl PathEdgeExpect {
    pub fn has_assertions(&self) -> bool {
        self.from.is_some()
            || self.from_id.is_some()
            || self.edge_type.is_some()
            || self.to.is_some()
            || self.to_id.is_some()
            || self.step.is_some()
    }
}

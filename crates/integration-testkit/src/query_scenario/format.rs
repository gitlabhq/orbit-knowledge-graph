use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryScenario {
    #[serde(default)]
    pub description: Option<String>,
    pub query: BTreeMap<String, String>,
    #[serde(default)]
    pub security: Option<SecurityOverride>,
    #[serde(default)]
    pub redaction: Option<RedactionConfig>,
    #[serde(default)]
    pub expect: QueryExpect,
}

#[derive(Debug, Default, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
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
    #[serde(default)]
    pub rows: Vec<BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CompileErrorExpect {
    Flag(bool),
    Substring(String),
}

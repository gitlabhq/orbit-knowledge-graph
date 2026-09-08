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
    pub node_order: BTreeMap<String, Vec<i64>>,
    #[serde(default)]
    pub node_ids: BTreeMap<String, Vec<i64>>,
    #[serde(default)]
    pub nodes: Vec<NodeExpect>,
    #[serde(default)]
    pub node_absent: BTreeMap<String, Vec<i64>>,
    #[serde(default)]
    pub edge_set: BTreeMap<String, Vec<[i64; 2]>>,
    #[serde(default)]
    pub edge_count: BTreeMap<String, usize>,
    #[serde(default)]
    pub referential_integrity: bool,
    #[serde(default)]
    pub has_more: Option<bool>,
    #[serde(default)]
    pub skip_requirements: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum CompileErrorExpect {
    Flag(bool),
    Substring(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeExpect {
    pub entity: String,
    pub id: i64,
    #[serde(default)]
    pub properties: BTreeMap<String, serde_json::Value>,
}

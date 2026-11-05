use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::{explicit_null, is_false};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageFragment {
    pub package: String,
    pub store: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<EngineSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, SourceSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, RouteSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub lookups: IndexMap<String, LookupSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineSpec {
    pub kind: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub trusted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_column: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub edge_tables: IndexMap<String, EdgeTableSpec>,
}

impl EngineSpec {
    pub fn edge_table_for(
        &self,
        source_domain: Option<&str>,
        target_domain: Option<&str>,
    ) -> Option<&str> {
        let in_domain = |domain: &str| {
            self.edge_tables
                .iter()
                .find(|(_, table)| table.domains.iter().any(|d| d == domain))
        };
        let shared = source_domain
            .filter(|domain| Some(*domain) == target_domain)
            .and_then(in_domain);
        shared
            .or_else(|| self.edge_tables.iter().find(|(_, table)| table.default))
            .map(|(name, _)| name.as_str())
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTableSpec {
    #[serde(default, skip_serializing_if = "is_false")]
    pub default: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(
        default,
        deserialize_with = "explicit_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub path_column: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub only: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub indexes: IndexMap<String, IndexKinds>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub low_selectivity: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub high_fan_out: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexKinds {
    One(String),
    Many(Vec<String>),
}

impl IndexKinds {
    pub fn kinds(&self) -> &[String] {
        match self {
            Self::One(kind) => std::slice::from_ref(kind),
            Self::Many(kinds) => kinds,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fk: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub variants: IndexMap<String, RouteSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    pub entity: String,
    pub provides: Vec<String>,
    pub keys: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
}

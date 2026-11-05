use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{explicit_null, is_false};

pub type BackendOptions = IndexMap<String, OptionValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OptionValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    List(Vec<OptionValue>),
    Map(IndexMap<String, OptionValue>),
}

pub fn encode_options(value: impl Serialize) -> Result<BackendOptions, serde_json::Error> {
    serde_json::from_str(&serde_json::to_string(&value)?)
}

pub fn decode_options<T: DeserializeOwned>(
    options: &BackendOptions,
) -> Result<T, serde_json::Error> {
    serde_json::from_str(&serde_json::to_string(options)?)
}

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
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineSpec {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub trusted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_column: Option<String>,
    #[serde(default, skip_serializing_if = "BackendOptions::is_empty")]
    pub backend: BackendOptions,
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
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub only: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "BackendOptions::is_empty")]
    pub backend: BackendOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fk: Option<String>,
    #[serde(default, skip_serializing_if = "BackendOptions::is_empty")]
    pub backend: BackendOptions,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub variants: IndexMap<String, RouteSpec>,
}

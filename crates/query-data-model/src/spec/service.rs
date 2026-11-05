use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::is_false;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceFragment {
    pub package: String,
    pub service: String,
    pub lookups: IndexMap<String, LookupSpec>,
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

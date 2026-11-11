use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::{Naming, Raw, Resolved, is_false};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceFragment {
    pub package: String,
    pub service: String,
    pub lookups: IndexMap<String, Lookup<Raw>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Lookup<S: Naming = Resolved> {
    #[serde(skip)]
    pub service: S::Loaded<String>,
    #[serde(skip)]
    pub name: S::Loaded<String>,
    pub operation: S::Defaulted<String>,
    pub entity: S::Entity,
    pub provides: Vec<S::Property>,
    pub keys: Vec<S::Property>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operators: Vec<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub disabled: bool,
}

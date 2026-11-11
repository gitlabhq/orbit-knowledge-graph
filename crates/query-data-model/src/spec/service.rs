use indexmap::IndexMap;
use serde::Deserialize;

use super::{Naming, Raw, Resolved};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceFragment {
    pub package: String,
    pub service: String,
    pub lookups: IndexMap<String, Lookup<Raw>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
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
    #[serde(default)]
    pub operators: Vec<String>,
    #[serde(default)]
    pub disabled: bool,
}

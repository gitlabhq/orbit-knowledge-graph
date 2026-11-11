use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::{is_false, is_true, yes};
use crate::{EdgeVariantScope, RequiredRole, Scope};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationFragment {
    pub package: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, EntityAccessSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, ScopeSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub properties: IndexMap<String, PropertyAccess>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityAccessSpec {
    pub scope: Scope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<RequiredRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redaction: Option<RedactionSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<AnchorSpec>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub delegated: bool,
}

/// A property that holds the id of the namespace entity a row belongs to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnchorSpec {
    pub property: String,
    pub entity: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedactionSpec {
    pub resource: String,
    pub ability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScopeSpec {
    All(EdgeVariantScope),
    PerVariant(ScopeVariants),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeVariants {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<EdgeVariantScope>,
    pub variants: IndexMap<String, EdgeVariantScope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyAccess {
    #[serde(default, skip_serializing_if = "is_false")]
    pub hidden: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub admin_only: bool,
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub filterable: bool,
    #[serde(default = "yes", skip_serializing_if = "is_true")]
    pub like: bool,
}

impl PropertyAccess {
    pub const OPEN: Self = Self {
        hidden: false,
        admin_only: false,
        filterable: true,
        like: true,
    };
}

impl Default for PropertyAccess {
    fn default() -> Self {
        Self::OPEN
    }
}

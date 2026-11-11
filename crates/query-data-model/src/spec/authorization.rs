use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::{Naming, Raw, Resolved, is_false, is_true, yes};
use crate::{EdgeVariantScope, EntityId, RequiredRole, Scope};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationFragment {
    pub package: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, EntityAccess<Raw>>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, ScopeSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub properties: IndexMap<String, PropertyAccess>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct EntityAccess<S: Naming = Resolved> {
    pub scope: Scope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<RequiredRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redaction: Option<Redaction<S>>,
    /// A foreign key to the namespace entity that holds the row; a filter on it bounds the entity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub anchor: Option<Anchor<S>>,
}

/// A property that holds the id of the namespace entity a row belongs to.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Anchor<S: Naming = Resolved> {
    pub property: S::Property,
    pub entity: S::Entity,
}

impl Copy for Anchor {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Redaction<S: Naming = Resolved> {
    pub resource: String,
    pub ability: String,
    pub id: S::Defaulted<S::Property>,
    #[serde(skip)]
    pub owner: S::Loaded<Option<EntityId>>,
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

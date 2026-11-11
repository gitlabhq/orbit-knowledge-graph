use indexmap::IndexMap;
use serde::Deserialize;

use super::{Naming, Raw, Resolved, yes};
use crate::{EdgeVariantScope, EntityId, RequiredRole, Scope};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationFragment {
    pub package: String,
    #[serde(default)]
    pub entities: IndexMap<String, EntityAccess<Raw>>,
    #[serde(default)]
    pub relationships: IndexMap<String, ScopeSpec>,
    #[serde(default)]
    pub properties: IndexMap<String, PropertyAccess>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct EntityAccess<S: Naming = Resolved> {
    pub scope: Scope,
    #[serde(default)]
    pub role: Option<RequiredRole>,
    #[serde(default)]
    pub redaction: Option<Redaction<S>>,
    /// A foreign key to the namespace entity that holds the row; a filter on it bounds the entity.
    #[serde(default)]
    pub anchor: Option<Anchor<S>>,
}

/// A property that holds the id of the namespace entity a row belongs to.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Anchor<S: Naming = Resolved> {
    pub property: S::Property,
    pub entity: S::Entity,
}

impl Copy for Anchor {}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Redaction<S: Naming = Resolved> {
    pub resource: String,
    pub ability: String,
    pub id: S::Defaulted<S::Property>,
    #[serde(skip)]
    pub owner: S::Loaded<Option<EntityId>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ScopeSpec {
    All(EdgeVariantScope),
    PerVariant(ScopeVariants),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeVariants {
    #[serde(default)]
    pub scope: Option<EdgeVariantScope>,
    pub variants: IndexMap<String, EdgeVariantScope>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyAccess {
    #[serde(default)]
    pub hidden: bool,
    #[serde(default)]
    pub admin_only: bool,
    #[serde(default = "yes")]
    pub filterable: bool,
    #[serde(default = "yes")]
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

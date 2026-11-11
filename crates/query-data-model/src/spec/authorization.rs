use indexmap::IndexMap;
use serde::Deserialize;

use super::OneOrMany;

/// A package's authorization: the platform vocabulary it declares and its entities' policies.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizationFragment {
    pub package: String,
    #[serde(default)]
    pub permissions: Vec<String>,
    #[serde(default)]
    pub scopes: IndexMap<String, ScopeDeclaration>,
    #[serde(default)]
    pub entities: IndexMap<String, EntityAccess>,
    #[serde(default)]
    pub relationships: IndexMap<String, RelationshipAccess>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeDeclaration {
    pub kind: ScopeKind,
    pub provider: String,
    /// The entity an `owner` scope's rows belong to.
    pub entity: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    Hierarchy,
    Flat,
    Owner,
    Global,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityAccess {
    pub scope: String,
    pub permission: String,
    /// One policy per backend kind that stores the entity; a global entity declares none.
    #[serde(default)]
    pub enforce: IndexMap<String, PolicySpec>,
    #[serde(default)]
    pub checks: Vec<CheckSpec>,
    pub properties: PropertyClasses,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum PolicySpec {
    Word(PolicyWord),
    ScopeKey { scope_key: ScopeKeySpec },
    Owner { owner: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyWord {
    None,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ScopeKeySpec {
    Property(String),
    Via(Via),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Via {
    pub via: OneOrMany,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum CheckSpec {
    Delegated(DelegatedCheck),
    Rows(RowCheck),
}

/// The provider answers `Credentials`, and the backend runs the read with the caller's sign-in.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegatedCheck {
    pub provider: String,
    pub kind: DelegatedWord,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelegatedWord {
    Delegated,
}

/// The provider answers `Check` for the returned rows, or for the `object` each row names.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowCheck {
    pub provider: String,
    pub permission: String,
    pub resource: String,
    pub object: Option<String>,
    pub id: Option<String>,
}

/// Every property of an entity, or attribute of a relationship, in exactly one class: `open`
/// reads and filters freely, `exact` filters without pattern matches, `unfiltered` is read but
/// never filtered, `hidden` is neither, and `admin` and `admin_exact` are `open` and `exact`
/// for administrators only.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PropertyClasses {
    #[serde(default)]
    pub open: Vec<String>,
    #[serde(default)]
    pub exact: Vec<String>,
    #[serde(default)]
    pub unfiltered: Vec<String>,
    #[serde(default)]
    pub hidden: Vec<String>,
    #[serde(default)]
    pub admin: Vec<String>,
    #[serde(default)]
    pub admin_exact: Vec<String>,
}

impl PropertyClasses {
    pub fn classes(&self) -> [(&[String], PropertyAccess); 6] {
        let class = |hidden, admin_only, filterable, like| PropertyAccess {
            hidden,
            admin_only,
            filterable,
            like,
        };
        [
            (&self.open, class(false, false, true, true)),
            (&self.exact, class(false, false, true, false)),
            (&self.unfiltered, class(false, false, false, false)),
            (&self.hidden, class(true, false, false, false)),
            (&self.admin, class(false, true, true, true)),
            (&self.admin_exact, class(false, true, true, false)),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipAccess {
    pub scope: String,
    pub permission: String,
    pub enforce: IndexMap<String, EdgePolicy>,
    /// Policies for single variants, by backend kind, where they differ from `enforce`.
    #[serde(default)]
    pub variants: IndexMap<String, IndexMap<String, EdgePolicy>>,
    #[serde(default)]
    pub attributes: PropertyClasses,
}

/// An edge row's scope key: the copy of `scope_key` it holds, and which endpoints share it.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgePolicy<P = String> {
    pub scope_key: P,
    pub of: EdgeScope,
}

/// Which endpoints an edge row's scope key belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeScope {
    /// The target's key; the source may sit in another scope.
    Target,
    /// The source's key; the target may sit in another scope.
    Source,
    /// The target's key, which the source shares.
    Both,
    /// The target's key, which lies within the source's scope.
    Nested,
    /// The target is the scope entity that holds the source.
    Holder,
    /// The edge's own key, which neither endpoint is known to share.
    Own,
}

impl EdgeScope {
    /// A bound on one endpoint's scope key also bounds the other.
    #[must_use]
    pub fn preserves_scope(self) -> bool {
        matches!(self, Self::Both | Self::Nested | Self::Holder)
    }

    /// A bound on the target's scope key also bounds the source.
    #[must_use]
    pub fn propagates_to_source(self) -> bool {
        matches!(self, Self::Both | Self::Holder)
    }

    #[must_use]
    pub fn edge_path_is_target_path(self) -> bool {
        matches!(self, Self::Both | Self::Nested | Self::Target)
    }

    #[must_use]
    pub fn edge_path_is_source_path(self) -> bool {
        matches!(self, Self::Source)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyAccess {
    pub hidden: bool,
    pub admin_only: bool,
    pub filterable: bool,
    pub like: bool,
}

impl PropertyAccess {
    /// A value the plan computes rather than reads, which no package declares.
    pub const COMPUTED: Self = Self {
        hidden: false,
        admin_only: false,
        filterable: true,
        like: true,
    };
}

/// A package's manifest.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PackageManifest {
    pub name: String,
    #[serde(default)]
    pub requires: Vec<String>,
}

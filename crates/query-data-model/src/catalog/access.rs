mod entities;
mod policies;
mod relationships;
mod vocabulary;

use std::collections::BTreeSet;

use super::Diagnostics;
use crate::generic::AttributeId;
use crate::spec::AuthorizationFragment;
pub use crate::spec::{EdgeScope, PropertyAccess, ScopeKind};
use crate::{EntityId, GraphCatalog, Hop, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ScopeId(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PermissionId(pub usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub name: String,
    pub package: String,
    pub kind: ScopeKind,
    pub provider: String,
    /// The entity an owner scope's rows belong to.
    pub owner: Option<EntityId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permission {
    pub name: String,
    pub package: String,
}

/// How a backend restricts an entity's reads to the caller's scope keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Policy {
    ScopeKey(PropertyId),
    /// Relationships from the entity to the entity whose scope key bounds it.
    Via(Vec<Hop>),
    /// The relationship to the entity that owns the row.
    Owner(Hop),
    /// Only the entity's checks restrict its rows.
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// The provider answers `Check` for each row's `id`, a key of `object` when one is named.
    Rows {
        provider: String,
        permission: PermissionId,
        resource: String,
        id: PropertyId,
        object: Option<EntityId>,
    },
    /// The provider answers `Credentials`, and the backend reads with the caller's sign-in.
    Delegated { provider: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityAccess {
    pub scope: ScopeId,
    pub permission: PermissionId,
    pub enforce: Vec<(String, Policy)>,
    pub checks: Vec<Check>,
}

impl EntityAccess {
    pub fn policy(&self, backend: &str) -> Option<&Policy> {
        self.enforce
            .iter()
            .find(|(kind, _)| kind == backend)
            .map(|(_, policy)| policy)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipAccess {
    pub scope: ScopeId,
    pub permission: PermissionId,
}

/// The access every entity, relationship, property and attribute of a model declares.
#[derive(Debug, Default)]
pub struct Access {
    permissions: Vec<Permission>,
    scopes: Vec<Scope>,
    entities: Vec<EntityAccess>,
    relationships: Vec<RelationshipAccess>,
    variants: Vec<Vec<(String, EdgeScope)>>,
    properties: Vec<PropertyAccess>,
    attributes: Vec<PropertyAccess>,
}

impl Access {
    pub fn entity(&self, entity: EntityId) -> &EntityAccess {
        &self.entities[entity.index()]
    }

    pub fn relationship(&self, relationship: RelationshipId) -> &RelationshipAccess {
        &self.relationships[relationship.index()]
    }

    /// Which endpoints the variant's edge rows share a scope key with, on a backend kind.
    pub fn edge_scope(&self, variant: RelationshipVariantId, backend: &str) -> Option<EdgeScope> {
        self.variants[variant.index()]
            .iter()
            .find(|(kind, _)| kind == backend)
            .map(|(_, scope)| *scope)
    }

    pub fn property(&self, property: PropertyId) -> PropertyAccess {
        self.properties[property.index()]
    }

    pub fn attribute(&self, attribute: AttributeId) -> PropertyAccess {
        self.attributes[attribute.index()]
    }

    /// Whether every entity and relationship of the graph has an entry.
    pub(super) fn covers(&self, graph: &GraphCatalog) -> bool {
        self.entities.len() == graph.entities().count()
            && self.relationships.len() == graph.relationships().count()
    }

    pub fn is_global(&self, entity: EntityId) -> bool {
        self.scope(self.entity(entity).scope).kind == ScopeKind::Global
    }

    pub fn scope(&self, scope: ScopeId) -> &Scope {
        &self.scopes[scope.0]
    }

    pub fn permission(&self, permission: PermissionId) -> &Permission {
        &self.permissions[permission.0]
    }

    pub fn scopes(&self) -> impl Iterator<Item = (ScopeId, &Scope)> {
        self.scopes.iter().enumerate().map(|(i, s)| (ScopeId(i), s))
    }

    /// Every scope and permission a read of this model asks the caller's scope keys for.
    pub fn needs(&self) -> BTreeSet<(ScopeId, PermissionId)> {
        let entities = self.entities.iter().map(|e| (e.scope, e.permission));
        let relationships = self.relationships.iter().map(|r| (r.scope, r.permission));
        entities.chain(relationships).collect()
    }

    /// Every provider name a package uses, with whether a check needs it to answer `Check`.
    pub fn providers(&self) -> BTreeSet<(&str, ProviderUse)> {
        let scopes = self
            .scopes
            .iter()
            .map(|scope| (scope.provider.as_str(), ProviderUse::Scopes));
        let checks = self.entities.iter().flat_map(|entity| &entity.checks);
        let checks = checks.map(|check| match check {
            Check::Rows { provider, .. } => (provider.as_str(), ProviderUse::Check),
            Check::Delegated { provider } => (provider.as_str(), ProviderUse::Credentials),
        });
        scopes.chain(checks).collect()
    }

    pub(super) fn resolve(
        graph: &GraphCatalog,
        fragments: &[&AuthorizationFragment],
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut access = Self::default();
        for fragment in fragments {
            access.declare_vocabulary(graph, fragment, diagnostics);
        }
        let entities = entities::resolve(&access, graph, fragments, diagnostics);
        let relationships = relationships::resolve(&access, graph, fragments, diagnostics);
        match (entities, relationships) {
            (Some(entities), Some(relationships)) => Self {
                entities: entities.entities,
                properties: entities.properties,
                relationships: relationships.relationships,
                variants: relationships.variants,
                attributes: relationships.attributes,
                ..access
            },
            _ => access,
        }
    }
}

/// The query a provider must answer for the packages that name it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ProviderUse {
    Scopes,
    Check,
    Credentials,
}

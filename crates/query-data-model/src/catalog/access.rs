use std::collections::HashSet;

use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::{EdgeVariantScope, RequiredRole};

use super::Diagnostics;
use super::store::Store;
use crate::spec::{AuthorizationFragment, ScopeSpec, ScopeVariants, parse_variant, parse_word};
use crate::{AttributeId, Endpoint, EntityId, GraphCatalog, Hop, PropertyId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    Global,
    Namespace,
}

#[derive(Debug, Clone)]
pub struct Redaction {
    pub resource: String,
    pub ability: String,
    pub id: PropertyId,
    pub owner: Option<EntityId>,
}

#[derive(Debug, Clone)]
pub struct EntityAccess {
    pub scope: Scope,
    pub role: Option<RequiredRole>,
    pub redaction: Option<Redaction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PropertyAccess {
    pub hidden: bool,
    pub admin_only: bool,
    pub filterable: bool,
    pub like: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowFilter {
    None,
    Path {
        column: String,
    },
    PathThrough {
        hops: Vec<PathHop>,
        store: String,
        column: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PathHop {
    pub hop: Hop,
    pub store: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guard {
    pub filter: RowFilter,
    pub min_role: Option<u32>,
    pub redacted: bool,
}

#[derive(Debug, Default)]
pub struct Access {
    entities: Vec<Option<EntityAccess>>,
    properties: Vec<Option<PropertyAccess>>,
    attributes: Vec<Option<PropertyAccess>>,
    variant_scopes: Vec<Option<EdgeVariantScope>>,
}

impl PropertyAccess {
    pub const OPEN: Self = Self {
        hidden: false,
        admin_only: false,
        filterable: true,
        like: true,
    };
}

impl Access {
    pub fn entity(&self, entity: EntityId) -> Option<&EntityAccess> {
        self.entities[entity.index()].as_ref()
    }

    pub fn property(&self, property: PropertyId) -> PropertyAccess {
        self.properties[property.index()].unwrap_or(PropertyAccess::OPEN)
    }

    pub fn attribute(&self, attribute: AttributeId) -> PropertyAccess {
        self.attributes[attribute.index()].unwrap_or(PropertyAccess::OPEN)
    }

    pub fn variant_scope(&self, variant: crate::RelationshipVariantId) -> Option<EdgeVariantScope> {
        self.variant_scopes[variant.index()]
    }

    pub(super) fn resolve(
        graph: &GraphCatalog,
        fragments: &[&AuthorizationFragment],
        diagnostics: &mut Diagnostics,
    ) -> Self {
        let mut access = Self {
            entities: vec![None; graph.entities().count()],
            properties: vec![None; graph.properties().count()],
            attributes: vec![None; graph.attributes().count()],
            variant_scopes: vec![None; graph.variants().count()],
        };
        let mut scoped = HashSet::new();
        for fragment in fragments {
            access.declare_entities(graph, fragment, diagnostics);
            access.declare_variant_scopes(graph, fragment, &mut scoped, diagnostics);
            access.declare_properties(graph, fragment, diagnostics);
        }
        access.link_redaction_owners(graph);
        for entity in graph.entities() {
            if access.entities[entity.id.index()].is_none() {
                diagnostics.error(
                    &entity.package,
                    format!("entity {} has no authorization entry", entity.name),
                );
            }
        }
        access
    }

    fn declare_entities(
        &mut self,
        graph: &GraphCatalog,
        fragment: &AuthorizationFragment,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.entities {
            let Some(entity) = graph.entity_id(name) else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown entity {name}"),
                );
                continue;
            };
            let Some(scope) = parse_word::<Scope>(&spec.scope) else {
                diagnostics.error(
                    package,
                    format!("{name} has unknown scope `{}`", spec.scope),
                );
                continue;
            };
            let role = spec.role.as_deref().and_then(|role| {
                diagnostics.require(parse_word::<RequiredRole>(role), package, || {
                    format!("{name} has unknown role `{role}`")
                })
            });
            let redaction = spec.redaction.as_ref().and_then(|redaction| {
                let column = redaction.id.as_deref().unwrap_or(DEFAULT_PRIMARY_KEY);
                let id = graph.property_id(entity, column);
                if id.is_none() {
                    diagnostics.error(
                        package,
                        format!("{name} redaction id {column} is not a property"),
                    );
                }
                Some(Redaction {
                    resource: redaction.resource.clone(),
                    ability: redaction.ability.clone(),
                    id: id?,
                    owner: None,
                })
            });
            if self.entities[entity.index()]
                .replace(EntityAccess {
                    scope,
                    role,
                    redaction,
                })
                .is_some()
            {
                diagnostics.error(package, format!("{name} has two authorization entries"));
            }
        }
    }

    fn declare_variant_scopes(
        &mut self,
        graph: &GraphCatalog,
        fragment: &AuthorizationFragment,
        scoped: &mut HashSet<crate::RelationshipId>,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.relationships {
            let Some(relationship) = graph.relationship_id(name) else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown relationship {name}"),
                );
                continue;
            };
            if !scoped.insert(relationship) {
                diagnostics.error(package, format!("{name} has two scope entries"));
                continue;
            }
            let parse = |word: &str, diagnostics: &mut Diagnostics| {
                diagnostics.require(parse_word::<EdgeVariantScope>(word), package, || {
                    format!("{name} has unknown scope `{word}`")
                })
            };
            let (default, variants) = match spec {
                ScopeSpec::All(scope) => (parse(scope, diagnostics), None),
                ScopeSpec::PerVariant(ScopeVariants { scope, variants }) => (
                    scope.as_deref().and_then(|scope| parse(scope, diagnostics)),
                    Some(variants),
                ),
            };
            for id in &graph.relationship(relationship).variants {
                self.variant_scopes[id.index()] = default;
            }
            for (variant, scope) in variants.into_iter().flatten() {
                let id = parse_variant(variant)
                    .and_then(|(source, target)| graph.variant_named(name, source, target))
                    .map(|variant| variant.id);
                match id {
                    Some(id) => self.variant_scopes[id.index()] = parse(scope, diagnostics),
                    None => diagnostics.error(
                        package,
                        format!("{name} scopes unknown variant `{variant}`"),
                    ),
                }
            }
        }
    }

    fn declare_properties(
        &mut self,
        graph: &GraphCatalog,
        fragment: &AuthorizationFragment,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (path, spec) in &fragment.properties {
            let declared = PropertyAccess {
                hidden: spec.hidden,
                admin_only: spec.admin_only,
                filterable: spec.filterable,
                like: spec.like,
            };
            let Some((owner, name)) = path.split_once('.') else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown property {path}"),
                );
                continue;
            };
            let slot = if let Some(property) = graph.property_named(owner, name) {
                &mut self.properties[property.id.index()]
            } else if let Some(attribute) = graph
                .relationship_id(owner)
                .and_then(|relationship| graph.attribute_id(relationship, name))
            {
                &mut self.attributes[attribute.index()]
            } else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown property {path}"),
                );
                continue;
            };
            if slot.replace(declared).is_some() {
                diagnostics.error(package, format!("{path} has two access entries"));
            }
        }
    }

    fn link_redaction_owners(&mut self, graph: &GraphCatalog) {
        let keyed_by_id =
            |redaction: &Redaction| graph.property(redaction.id).name == DEFAULT_PRIMARY_KEY;
        let owners: Vec<(String, EntityId)> = self
            .entities
            .iter()
            .enumerate()
            .filter_map(|(index, access)| {
                let redaction = access.as_ref()?.redaction.as_ref()?;
                keyed_by_id(redaction).then(|| (redaction.resource.clone(), EntityId(index)))
            })
            .collect();
        for redaction in self
            .entities
            .iter_mut()
            .flatten()
            .filter_map(|access| access.redaction.as_mut())
        {
            if !keyed_by_id(redaction) {
                redaction.owner = owners
                    .iter()
                    .rev()
                    .find(|(resource, _)| *resource == redaction.resource)
                    .map(|(_, owner)| *owner);
            }
        }
    }

    pub(super) fn resolve_guards(
        &self,
        graph: &GraphCatalog,
        stores: &mut [Store],
        diagnostics: &mut Diagnostics,
    ) {
        let guards: Vec<Vec<(EntityId, Option<Guard>)>> = stores
            .iter()
            .map(|store| {
                store
                    .sources()
                    .map(|source| {
                        (
                            source.entity,
                            self.guard(graph, stores, store, source.entity),
                        )
                    })
                    .collect()
            })
            .collect();
        for (store, guards) in stores.iter_mut().zip(guards) {
            for (entity, guard) in guards {
                if guard.is_none() && self.entity(entity).is_some() {
                    let entity = graph.entity(entity);
                    diagnostics.error(
                        &entity.package,
                        format!(
                            "{} in store {} has no namespace path column, reaches no entity that has one, and has no redaction rule",
                            entity.name, store.name
                        ),
                    );
                }
                store.guards[entity.index()] = guard;
            }
        }
    }

    fn guard(
        &self,
        graph: &GraphCatalog,
        stores: &[Store],
        store: &Store,
        entity: EntityId,
    ) -> Option<Guard> {
        let access = self.entity(entity)?;
        let redacted = access.redaction.is_some();
        if store.trusted || access.scope == Scope::Global {
            return Some(Guard {
                filter: RowFilter::None,
                min_role: None,
                redacted: redacted && !store.trusted,
            });
        }
        let min_role = Some(
            access
                .role
                .unwrap_or(RequiredRole::Reporter)
                .as_access_level(),
        );
        if let Some(column) = store
            .source(entity)
            .and_then(|source| source.path_column.clone())
        {
            return Some(Guard {
                filter: RowFilter::Path { column },
                min_role,
                redacted,
            });
        }
        let nearby =
            || {
                std::iter::once(store).chain(stores.iter().filter(|other| {
                    other.connection == store.connection && other.name != store.name
                }))
            };
        let anchor_store = |hop: &Hop| {
            nearby()
                .filter(|candidate| hop.from != entity || candidate.name == store.name)
                .find(|candidate| self.anchors(candidate, hop))
        };
        let with_path = |entity: EntityId| {
            nearby().find_map(|candidate| {
                let column = candidate.source(entity)?.path_column.clone()?;
                Some((candidate.name.clone(), column))
            })
        };
        let hops = graph
            .walk(entity)
            .max_hops(graph.entities().count())
            .filter(|hop| anchor_store(hop).is_some())
            .shortest(|reached| with_path(reached).is_some());
        match hops {
            Some(hops) => {
                let (store, column) = with_path(hops.last()?.to)?;
                let hops = hops
                    .into_iter()
                    .map(|hop| {
                        let store = anchor_store(&hop)?.name.clone();
                        Some(PathHop { hop, store })
                    })
                    .collect::<Option<Vec<_>>>()?;
                Some(Guard {
                    filter: RowFilter::PathThrough {
                        hops,
                        store,
                        column,
                    },
                    min_role,
                    redacted,
                })
            }
            None => redacted.then_some(Guard {
                filter: RowFilter::None,
                min_role,
                redacted,
            }),
        }
    }

    fn anchors(&self, store: &Store, hop: &Hop) -> bool {
        let anchor = self.variant_scope(hop.variant) == Some(EdgeVariantScope::NamespaceAnchor);
        let held_by_source = store
            .foreign_key(hop.variant)
            .is_some_and(|key| key.holder == Endpoint::Source);
        hop.forward && anchor && held_by_source
    }
}

//! How each store restricts the reads of every entity and edge table it holds.

use std::collections::BTreeMap;

use super::access::{Access, PermissionId, Policy, ScopeId, ScopeKind};
use super::{Diagnostics, Store, registered};
use crate::{Endpoint, EntityId, GraphCatalog, Hop};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Guard {
    pub scope: ScopeId,
    pub permission: PermissionId,
    pub rule: Rule,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rule {
    /// The store is trusted, so nothing restricts its reads.
    Trusted,
    /// A global entity: the caller needs the permission, and no key restricts the rows.
    Global,
    /// The rows hold their scope key in this column.
    Column(String),
    /// The rows reach their scope key through these relationships.
    Via(Vec<Hop>),
    /// The rows name their owner in this column.
    Owner(String),
    /// Only the entity's checks restrict the rows.
    Checked,
}

/// The policy kinds a backend can render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyKind {
    ScopeKey,
    Via,
    Owner,
    None,
}

impl Policy {
    fn kind(&self) -> PolicyKind {
        match self {
            Self::ScopeKey(_) => PolicyKind::ScopeKey,
            Self::Via(_) => PolicyKind::Via,
            Self::Owner(_) => PolicyKind::Owner,
            Self::None => PolicyKind::None,
        }
    }
}

pub(super) fn resolve(
    graph: &GraphCatalog,
    access: &Access,
    stores: &mut [Store],
    diagnostics: &mut Diagnostics,
) {
    for store in stores.iter_mut() {
        let entities: Vec<EntityId> = store.sources().map(|source| source.entity).collect();
        for entity in entities {
            let guard = entity_guard(graph, access, store, entity, diagnostics);
            store.guards[entity.index()] = guard;
        }
        let edges = edge_guards(graph, access, store, diagnostics);
        store.edge_guards = edges;
    }
    via_keys(graph, stores, diagnostics);
}

/// Each `via` step is a foreign key on the entity it leaves, so a lookup can join it.
fn via_keys(graph: &GraphCatalog, stores: &[Store], diagnostics: &mut Diagnostics) {
    for store in stores {
        for source in store.sources() {
            let Some(Guard {
                rule: Rule::Via(hops),
                ..
            }) = store.guard(source.entity)
            else {
                continue;
            };
            for hop in hops {
                let keyed = stores.iter().any(|other| {
                    other
                        .foreign_key(hop.variant)
                        .is_some_and(|key| key.holder == Endpoint::Source)
                });
                if !keyed {
                    diagnostics.error(
                        &source.package,
                        format!(
                            "{}'s via step {} has no foreign key on {} in any store",
                            graph.entity(source.entity).name,
                            graph.relationship(hop.relationship).name,
                            graph.entity(hop.from).name
                        ),
                    );
                }
            }
        }
    }
}

fn entity_guard(
    graph: &GraphCatalog,
    access: &Access,
    store: &Store,
    entity: EntityId,
    diagnostics: &mut Diagnostics,
) -> Option<Guard> {
    let declared = access.entity(entity);
    let name = &graph.entity(entity).name;
    let package = &graph.entity(entity).package;
    let guard = |rule| Guard {
        scope: declared.scope,
        permission: declared.permission,
        rule,
    };
    let policy = declared.policy(&store.kind);
    if store.trusted {
        return Some(guard(Rule::Trusted));
    }
    if access.scope(declared.scope).kind == ScopeKind::Global {
        return Some(guard(Rule::Global));
    }
    let Some(policy) = policy else {
        diagnostics.error(
            package,
            format!(
                "{name} is stored in {} but enforces no {} policy",
                store.name, store.kind
            ),
        );
        return None;
    };
    let renders =
        registered(&store.kind).is_some_and(|backend| backend.policies().contains(&policy.kind()));
    if !renders {
        diagnostics.error(
            package,
            format!(
                "{} cannot enforce {name}'s {:?} policy",
                store.kind,
                policy.kind()
            ),
        );
        return None;
    }
    let rule = match policy {
        Policy::ScopeKey(property) => {
            let column = store.column(*property).map(str::to_string);
            Rule::Column(diagnostics.require(column, package, || {
                format!(
                    "{name} scope key {} is not stored in {}",
                    graph.property(*property).name,
                    store.name
                )
            })?)
        }
        Policy::Via(hops) => Rule::Via(hops.clone()),
        Policy::Owner(hop) => {
            let held = match hop.forward {
                true => Endpoint::Source,
                false => Endpoint::Target,
            };
            let column = store
                .foreign_key(hop.variant)
                .filter(|key| key.holder == held)
                .and_then(|key| store.column(key.property))
                .map(str::to_string);
            Rule::Owner(diagnostics.require(column, package, || {
                format!(
                    "{name}'s owner {} is no foreign key on {name} in {}",
                    graph.relationship(hop.relationship).name,
                    store.name
                )
            })?)
        }
        Policy::None => Rule::Checked,
    };
    Some(guard(rule))
}

/// Every edge table the store holds, filtered by the store's path column under the one scope
/// and permission of the relationships it stores.
fn edge_guards(
    graph: &GraphCatalog,
    access: &Access,
    store: &Store,
    diagnostics: &mut Diagnostics,
) -> BTreeMap<String, Guard> {
    let mut guards: BTreeMap<String, Guard> = BTreeMap::new();
    let keyed = registered(&store.kind)
        .is_some_and(|backend| backend.policies().contains(&PolicyKind::ScopeKey));
    for (variant, package, _) in store.bindings() {
        let relationship = graph.variant(variant).relationship;
        let name = &graph.relationship(relationship).name;
        if !store.trusted && keyed && access.edge_scope(variant, &store.kind).is_none() {
            diagnostics.error(
                package,
                format!(
                    "{name} is stored in {} but enforces no {} policy",
                    store.name, store.kind
                ),
            );
        }
        let Some(table) = store.edge_table(variant) else {
            continue;
        };
        let rule = match (store.trusted, &store.path_column) {
            (true, _) => Rule::Trusted,
            (false, Some(column)) => Rule::Column(column.clone()),
            (false, None) => {
                diagnostics.error(
                    package,
                    format!(
                        "store {} holds {name} in {table} but has no path column",
                        store.name
                    ),
                );
                continue;
            }
        };
        let declared = access.relationship(relationship);
        let guard = Guard {
            scope: declared.scope,
            permission: declared.permission,
            rule,
        };
        match guards.get(table) {
            Some(existing) if *existing != guard => diagnostics.error(package, format!(
                "{table} holds relationships of more than one scope and permission, {name} among them"
            )),
            Some(_) => {}
            None => {
                guards.insert(table.to_string(), guard);
            }
        }
    }
    guards
}

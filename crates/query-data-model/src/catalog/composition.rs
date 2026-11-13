//! How packages compose: each names only what it owns or requires, within its tier.

use std::collections::{BTreeMap, BTreeSet};

use super::Diagnostics;
use super::access::{Access, ScopeKind};
use crate::packages::Tier;
use crate::spec::{
    AuthorizationFragment, CheckSpec, EndpointKind, Fragment, Fragments, PolicySpec, PolicyWord,
    ScopeKeySpec, parse_variant,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Entity,
    Relationship,
    Store,
    Scope,
    Permission,
}

pub(super) fn check(fragments: &Fragments, access: &Access, diagnostics: &mut Diagnostics) {
    let mut requires: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for fragment in fragments.iter() {
        if let Fragment::Package(manifest) = fragment {
            requires.insert(
                &manifest.name,
                manifest.requires.iter().map(String::as_str).collect(),
            );
        }
    }
    for (package, required) in &requires {
        for name in required {
            if !requires.contains_key(name) {
                diagnostics.error(
                    package,
                    format!("{package} requires unknown package {name}"),
                );
            }
        }
    }
    let owners = owners(fragments);
    let trusted: BTreeSet<&str> = fragments
        .iter()
        .filter_map(|fragment| match fragment {
            Fragment::Storage(storage) if storage.engine.as_ref().is_some_and(|e| e.trusted) => {
                Some(storage.store.as_str())
            }
            _ => None,
        })
        .collect();
    for fragment in fragments.iter() {
        let package = fragment.package();
        let allowed = |owner: &str| {
            owner == package || requires.get(package).is_some_and(|r| r.contains(owner))
        };
        for (kind, name) in references(fragment) {
            let Some(owner) = owners.get(&(kind, name.clone())) else {
                continue;
            };
            if !allowed(owner) {
                diagnostics.error(
                    package,
                    format!(
                        "{package} names {name} from package {owner}, which it does not require"
                    ),
                );
            }
        }
        if let Fragment::Graph(graph) = fragment {
            for name in graph.relationships.keys() {
                let owner = &owners[&(Kind::Relationship, name.clone())];
                if owner != package && fragments.tier(owner) != Some(Tier::Platform) {
                    diagnostics.error(package, format!(
                        "{package} adds variants to {name}, which {owner} owns and is not a platform package"
                    ));
                }
            }
        }
        let tier = fragments.tier(package);
        if let (Some(tier), Fragment::Authorization(fragment)) = (tier, fragment) {
            tiers(tier, fragment, access, fragments, diagnostics);
        }
        if let (Some(Tier::Integration | Tier::Tenant), Fragment::Storage(storage)) =
            (tier, fragment)
            && trusted.contains(storage.store.as_str())
        {
            diagnostics.error(
                package,
                format!(
                    "{package} uses trusted store {}, which only a platform package may",
                    storage.store
                ),
            );
        }
    }
}

/// The package that declares each entity, relationship, store, scope and permission first.
fn owners(fragments: &Fragments) -> BTreeMap<(Kind, String), String> {
    let mut owners = BTreeMap::new();
    let mut own = |kind, name: &str, package: &str| {
        owners
            .entry((kind, name.to_string()))
            .or_insert_with(|| package.to_string());
    };
    for fragment in fragments.iter() {
        let package = fragment.package();
        match fragment {
            Fragment::Graph(graph) => {
                graph
                    .entities
                    .keys()
                    .for_each(|name| own(Kind::Entity, name, package));
                graph
                    .relationships
                    .keys()
                    .for_each(|name| own(Kind::Relationship, name, package));
            }
            Fragment::Storage(storage) if storage.engine.is_some() => {
                own(Kind::Store, &storage.store, package)
            }
            Fragment::Authorization(access) => {
                access
                    .scopes
                    .keys()
                    .for_each(|name| own(Kind::Scope, name, package));
                access
                    .permissions
                    .iter()
                    .for_each(|name| own(Kind::Permission, name, package));
            }
            _ => {}
        }
    }
    owners
}

fn variant_entities(variant: &str) -> Vec<(Kind, String)> {
    parse_variant(variant)
        .map(|(source, target)| {
            vec![
                (Kind::Entity, source.to_string()),
                (Kind::Entity, target.to_string()),
            ]
        })
        .unwrap_or_default()
}

/// Every name a fragment uses that another package may own.
fn references(fragment: &Fragment) -> Vec<(Kind, String)> {
    let mut names = Vec::new();
    match fragment {
        Fragment::Graph(graph) => {
            for (name, relationship) in &graph.relationships {
                names.push((Kind::Relationship, name.clone()));
                relationship
                    .variants
                    .keys()
                    .for_each(|variant| names.extend(variant_entities(variant)));
            }
        }
        Fragment::Storage(storage) => {
            names.push((Kind::Store, storage.store.clone()));
            names.extend(
                storage
                    .entities
                    .keys()
                    .map(|name| (Kind::Entity, name.clone())),
            );
            for (name, variants) in &storage.relationships {
                names.push((Kind::Relationship, name.clone()));
                variants
                    .keys()
                    .for_each(|variant| names.extend(variant_entities(variant)));
            }
        }
        Fragment::Authorization(access) => names.extend(authorization_references(access)),
        Fragment::Service(service) => {
            names.extend(
                service
                    .lookups
                    .values()
                    .map(|lookup| (Kind::Entity, lookup.entity.clone())),
            );
        }
        Fragment::Source(source) => {
            names.extend(
                source
                    .entities
                    .keys()
                    .map(|name| (Kind::Entity, name.clone())),
            );
            names.extend(
                source
                    .relationships
                    .keys()
                    .map(|name| (Kind::Relationship, name.clone())),
            );
            let pipelines = source.entities.values().chain(source.derived.values());
            for edge in pipelines.flat_map(|pipeline| pipeline.edges.iter().flatten()) {
                names.push((Kind::Relationship, edge.relationship.clone()));
                for endpoint in [&edge.source, &edge.target] {
                    if let EndpointKind::Entity(entity) = &endpoint.kind {
                        names.push((Kind::Entity, entity.clone()));
                    }
                }
            }
        }
        Fragment::Package(_) => {}
    }
    names
}

fn authorization_references(access: &AuthorizationFragment) -> Vec<(Kind, String)> {
    let mut names = Vec::new();
    for (name, entity) in &access.entities {
        names.push((Kind::Entity, name.clone()));
        names.push((Kind::Scope, entity.scope.clone()));
        names.push((Kind::Permission, entity.permission.clone()));
        for policy in entity.enforce.values() {
            match policy {
                PolicySpec::ScopeKey {
                    scope_key: ScopeKeySpec::Via(via),
                } => {
                    names.extend(
                        via.via
                            .values()
                            .iter()
                            .map(|r| (Kind::Relationship, r.clone())),
                    );
                }
                PolicySpec::Owner { owner } => names.push((Kind::Relationship, owner.clone())),
                PolicySpec::ScopeKey { .. } | PolicySpec::Word(_) => {}
            }
        }
        for check in entity.checks.listed() {
            if let CheckSpec::Rows(row) = check {
                names.push((Kind::Permission, row.permission.clone()));
                names.extend(
                    row.object
                        .iter()
                        .map(|object| (Kind::Entity, object.clone())),
                );
            }
        }
    }
    for (name, relationship) in &access.relationships {
        names.push((Kind::Relationship, name.clone()));
        names.push((Kind::Scope, relationship.scope.clone()));
        names.push((Kind::Permission, relationship.permission.clone()));
    }
    names
}

/// What a package below the platform tier may not declare.
fn tiers(
    tier: Tier,
    fragment: &AuthorizationFragment,
    access: &Access,
    fragments: &Fragments,
    diagnostics: &mut Diagnostics,
) {
    let package = fragment.package.as_str();
    if tier == Tier::Platform {
        return;
    }
    if !fragment.permissions.is_empty() || !fragment.scopes.is_empty() {
        diagnostics.error(package, format!(
            "{package} is a {tier:?} package and declares permissions or scopes, which only a platform package may"
        ));
    }
    for (name, entity) in &fragment.entities {
        if let Some(scope) = access.scope_id(&entity.scope).map(|id| access.scope(id)) {
            let platform = fragments.tier(&scope.package) == Some(Tier::Platform);
            let allowed = match tier {
                Tier::Integration => platform && scope.kind != ScopeKind::Global,
                _ => matches!(scope.kind, ScopeKind::Hierarchy | ScopeKind::Owner),
            };
            if !allowed {
                diagnostics.error(
                    package,
                    format!(
                        "{name} is in {:?} scope {}, which a {tier:?} package may not use",
                        scope.kind, scope.name
                    ),
                );
            }
        }
        if entity
            .enforce
            .values()
            .any(|policy| matches!(policy, PolicySpec::Word(PolicyWord::None)))
        {
            diagnostics.error(
                package,
                format!("{name} enforces none, which only a platform package may"),
            );
        }
        if entity.checks.is_open() {
            diagnostics.error(
                package,
                format!("{name} declares checks: open, which only a platform package may"),
            );
        }
        for check in entity.checks.listed() {
            match (tier, check) {
                (Tier::Integration, CheckSpec::Delegated(_)) => {}
                (Tier::Integration, CheckSpec::Rows(row)) => diagnostics.error(package, format!(
                    "{name} checks rows through {}; an integration package declares only delegated checks",
                    row.provider
                )),
                _ => diagnostics.error(package, format!("{name} declares a check; a tenant package declares none")),
            }
        }
    }
}

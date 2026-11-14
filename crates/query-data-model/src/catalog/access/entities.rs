use super::policies::{Context, check_references};
use super::{Access, EntityAccess, Policy, PropertyAccess, ScopeKind};
use crate::GraphCatalog;
use crate::catalog::{Diagnostics, registered};
use crate::spec::{AuthorizationFragment, PropertyClasses};

pub(super) struct Entities {
    pub entities: Vec<EntityAccess>,
    pub properties: Vec<PropertyAccess>,
}

pub(super) fn resolve(
    access: &Access,
    graph: &GraphCatalog,
    fragments: &[&AuthorizationFragment],
    diagnostics: &mut Diagnostics,
) -> Option<Entities> {
    let mut entities: Vec<Option<EntityAccess>> = vec![None; graph.entities().count()];
    let mut properties = vec![None; graph.properties().count()];
    let mut written = vec![false; graph.entities().count()];
    for fragment in fragments {
        let package = fragment.package.as_str();
        for (name, spec) in &fragment.entities {
            let Some(entity) = graph.entity_id(name) else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown entity {name}"),
                );
                continue;
            };
            written[entity.index()] = true;
            let find = |property: &str| graph.property_id(entity, property).map(|id| id.index());
            classify(
                package,
                name,
                &spec.properties,
                find,
                &mut properties,
                diagnostics,
            );
            let scope = diagnostics.require(access.scope_id(&spec.scope), package, || {
                format!("{name} names unknown scope {}", spec.scope)
            });
            let permission =
                diagnostics.require(access.permission_id(&spec.permission), package, || {
                    format!("{name} names unknown permission {}", spec.permission)
                });
            let (Some(scope), Some(permission)) = (scope, permission) else {
                continue;
            };
            let global = access.scope(scope).kind == ScopeKind::Global;
            let mut enforce = Vec::new();
            for (backend, written) in &spec.enforce {
                if registered(backend).is_none() {
                    diagnostics.error(
                        package,
                        format!("{name} enforces a policy on unknown backend {backend}"),
                    );
                    continue;
                }
                let policy = Context {
                    access,
                    graph,
                    package,
                    entity,
                    diagnostics,
                }
                .policy(written, scope);
                enforce.extend(policy.map(|policy| (backend.clone(), policy)));
            }
            if global && !spec.enforce.is_empty() {
                diagnostics.error(
                    package,
                    format!(
                        "{name} is in global scope {}, so it enforces no policy",
                        spec.scope
                    ),
                );
            }
            match (global, spec.checks.is_open()) {
                (true, false) if spec.checks.listed().is_empty() => diagnostics.error(
                    package,
                    format!(
                        "{name} is in global scope {}, so it declares checks or checks: open",
                        spec.scope
                    ),
                ),
                (false, true) => diagnostics.error(
                    package,
                    format!("{name} declares checks: open, which only a global entity may"),
                ),
                _ => {}
            }
            let checks = spec.checks.listed().iter().filter_map(|check| {
                Context {
                    access,
                    graph,
                    package,
                    entity,
                    diagnostics,
                }
                .check(check)
            });
            let resolved = EntityAccess {
                scope,
                permission,
                enforce,
                checks: checks.collect(),
            };
            if resolved
                .policy_kinds()
                .any(|policy| *policy == Policy::None)
                && resolved.checks.is_empty()
            {
                diagnostics.error(
                    package,
                    format!("{name} enforces none and declares no check"),
                );
            }
            if entities[entity.index()].replace(resolved).is_some() {
                diagnostics.error(package, format!("{name} has two authorization entries"));
            }
        }
    }
    let mut complete = true;
    for entity in graph.entities() {
        if entities[entity.id.index()].is_none() {
            complete = false;
        }
        if !written[entity.id.index()] {
            complete = false;
            diagnostics.error(
                &entity.package,
                format!("entity {} has no authorization entry", entity.name),
            );
        }
    }
    for property in graph.properties() {
        let entity = graph.entity(property.entity);
        if properties[property.id.index()].is_none() && written[entity.id.index()] {
            diagnostics.error(
                &entity.package,
                format!("{}.{} has no access class", entity.name, property.name),
            );
        }
    }
    if !complete {
        return None;
    }
    let entities: Vec<EntityAccess> = entities.into_iter().collect::<Option<_>>()?;
    check_references(graph, &entities, diagnostics);
    Some(Entities {
        entities,
        properties: properties.into_iter().collect::<Option<_>>()?,
    })
}

impl EntityAccess {
    fn policy_kinds(&self) -> impl Iterator<Item = &Policy> {
        self.enforce.iter().map(|(_, policy)| policy)
    }
}

/// Puts every member `classes` names into its access slot, once.
pub(super) fn classify(
    package: &str,
    owner: &str,
    classes: &PropertyClasses,
    find: impl Fn(&str) -> Option<usize>,
    slots: &mut [Option<PropertyAccess>],
    diagnostics: &mut Diagnostics,
) {
    for (members, class) in classes.classes() {
        for member in members {
            let Some(index) = find(member) else {
                diagnostics.error(
                    package,
                    format!("authorization names unknown property {owner}.{member}"),
                );
                continue;
            };
            if slots[index].replace(class).is_some() {
                diagnostics.error(package, format!("{owner}.{member} has two access classes"));
            }
        }
    }
}

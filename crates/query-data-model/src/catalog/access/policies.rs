use super::{Access, Check, EntityAccess, Policy, ScopeId, ScopeKind};
use crate::catalog::Diagnostics;
use crate::constants::DEFAULT_PRIMARY_KEY;
use crate::spec::{CheckSpec, PolicySpec, PolicyWord, ScopeKeySpec};
use crate::types::Cardinality;
use crate::{EntityId, GraphCatalog, Hop};

/// Resolves one entity's written policies and checks.
pub(super) struct Context<'a> {
    pub access: &'a Access,
    pub graph: &'a GraphCatalog,
    pub package: &'a str,
    pub entity: EntityId,
    pub diagnostics: &'a mut Diagnostics,
}

impl Context<'_> {
    fn name(&self) -> &str {
        &self.graph.entity(self.entity).name
    }

    fn error(&mut self, message: String) {
        self.diagnostics.error(self.package, message);
    }

    pub fn policy(mut self, written: &PolicySpec, scope: ScopeId) -> Option<Policy> {
        let name = self.name().to_string();
        let scope = self.access.scope(scope);
        let owner_scope = scope.kind == ScopeKind::Owner;
        if owner_scope != matches!(written, PolicySpec::Owner { .. }) {
            self.error(format!(
                "{name} is in {:?} scope {}, which an owner policy enforces exactly when the scope is an owner scope",
                scope.kind, scope.name
            ));
            return None;
        }
        match written {
            PolicySpec::Word(PolicyWord::None) => Some(Policy::None),
            PolicySpec::ScopeKey {
                scope_key: ScopeKeySpec::Property(property),
            } => {
                let found = self.graph.property_id(self.entity, property);
                self.diagnostics
                    .require(found, self.package, || {
                        format!("{name} scope key {property} is not a property of {name}")
                    })
                    .map(Policy::ScopeKey)
            }
            PolicySpec::ScopeKey {
                scope_key: ScopeKeySpec::Via(via),
            } => {
                let mut from = self.entity;
                let mut hops = Vec::new();
                for relationship in via.via.values() {
                    let hop = self.step(from, relationship, None)?;
                    from = hop.to;
                    hops.push(hop);
                }
                Some(Policy::Via(hops))
            }
            PolicySpec::Owner { owner } => {
                let target = scope.owner?;
                let hop = self.step(self.entity, owner, Some(target))?;
                let variant = self.graph.variant(hop.variant);
                let one = match hop.forward {
                    true => matches!(
                        variant.cardinality,
                        Cardinality::ManyToOne | Cardinality::OneToOne
                    ),
                    false => matches!(
                        variant.cardinality,
                        Cardinality::OneToMany | Cardinality::OneToOne
                    ),
                };
                if !one {
                    self.error(format!(
                        "{name} owner {owner} allows more than one owner per row"
                    ));
                }
                Some(Policy::Owner(hop))
            }
        }
    }

    /// The one variant of `relationship` that leaves `from`, to `to` when it is given.
    fn step(&mut self, from: EntityId, relationship: &str, to: Option<EntityId>) -> Option<Hop> {
        let name = self.name().to_string();
        let Some(id) = self.graph.relationship_id(relationship) else {
            self.error(format!("{name} names unknown relationship {relationship}"));
            return None;
        };
        let hops: Vec<Hop> = self
            .graph
            .relationship(id)
            .variants
            .iter()
            .map(|variant| self.graph.variant(*variant))
            .filter_map(|variant| {
                let forward = variant.source == from;
                let (near, far) = match forward {
                    true => (variant.source, variant.target),
                    false => (variant.target, variant.source),
                };
                (near == from && to.is_none_or(|to| to == far) && (forward || to.is_some()))
                    .then_some(Hop {
                        variant: variant.id,
                        relationship: id,
                        from,
                        to: far,
                        forward,
                    })
            })
            .collect();
        match hops.as_slice() {
            [hop] => Some(*hop),
            _ => {
                let start = &self.graph.entity(from).name;
                self.error(format!(
                    "{name} reaches {} entities from {start} through {relationship}; a step needs one target",
                    hops.len()
                ));
                None
            }
        }
    }

    pub fn check(mut self, written: &CheckSpec) -> Option<Check> {
        let name = self.name().to_string();
        let row = match written {
            CheckSpec::Delegated(delegated) => {
                return Some(Check::Delegated {
                    provider: delegated.provider.clone(),
                });
            }
            CheckSpec::Rows(row) => row,
        };
        let permission = self.access.permission_id(&row.permission);
        let permission = self.diagnostics.require(permission, self.package, || {
            format!("{name} checks unknown permission {}", row.permission)
        });
        let (object, id) = match (&row.object, &row.id) {
            (Some(object), Some(id)) => (Some(object.as_str()), id.as_str()),
            (None, None) => (None, DEFAULT_PRIMARY_KEY),
            _ => {
                self.error(format!(
                    "{name} check names one of object and id; it names both or neither"
                ));
                return None;
            }
        };
        let object = match object {
            Some(object) => {
                let found = self.graph.entity_id(object);
                Some(self.diagnostics.require(found, self.package, || {
                    format!("{name} check names unknown entity {object}")
                })?)
            }
            None => None,
        };
        let id = self.graph.property_id(self.entity, id);
        let id = self.diagnostics.require(id, self.package, || {
            format!("{name} check id is not a property of {name}")
        })?;
        Some(Check::Rows {
            provider: row.provider.clone(),
            permission: permission?,
            resource: row.resource.clone(),
            id,
            object,
        })
    }
}

/// A `via` chain ends at an entity that holds a scope key, and a check's object checks itself.
pub(super) fn check_references(
    graph: &GraphCatalog,
    entities: &[EntityAccess],
    diagnostics: &mut Diagnostics,
) {
    for (index, access) in entities.iter().enumerate() {
        let entity = graph.entity(EntityId(index));
        for (_, policy) in &access.enforce {
            let Policy::Via(hops) = policy else { continue };
            let Some(end) = hops.last().map(|hop| hop.to) else {
                continue;
            };
            let holds = entities[end.index()]
                .enforce
                .iter()
                .any(|(_, policy)| matches!(policy, Policy::ScopeKey(_)));
            if !holds {
                diagnostics.error(
                    &entity.package,
                    format!(
                        "{}'s via chain ends at {}, which holds no scope key",
                        entity.name,
                        graph.entity(end).name
                    ),
                );
            }
        }
        for check in &access.checks {
            let Check::Rows {
                provider,
                resource,
                object: Some(object),
                ..
            } = check
            else {
                continue;
            };
            let checks_itself = entities[object.index()].checks.iter().any(|own| matches!(own,
                Check::Rows { provider: p, resource: r, object: None, .. } if p == provider && r == resource));
            if !checks_itself {
                diagnostics.error(&entity.package, format!(
                    "{} checks {resource} through {}, which declares no {provider} check of {resource} on itself",
                    entity.name, graph.entity(*object).name
                ));
            }
        }
    }
}

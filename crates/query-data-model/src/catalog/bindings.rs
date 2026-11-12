use std::collections::HashSet;

use indexmap::IndexMap;

use super::Diagnostics;
use super::store::Store;
use crate::spec::{
    Binding, Discriminator, Key, KeySpec, Raw, Resolved, StorageFragment, parse_variant,
};
use crate::{Endpoint, ForeignKey, GraphCatalog, PropertyId, RelationshipVariant};

impl Store {
    pub(super) fn declare_bindings(
        &mut self,
        fragment: &StorageFragment,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) {
        let package = fragment.package.as_str();
        for (name, variants) in &fragment.relationships {
            for (written, binding) in variants {
                let variant = parse_variant(written)
                    .and_then(|(source, target)| graph.variant_named(name, source, target));
                let Some(variant) = diagnostics.require(variant, package, || {
                    format!(
                        "store {} binds unknown variant {name}({written})",
                        self.name
                    )
                }) else {
                    continue;
                };
                if !self.declared.insert(variant.id) {
                    diagnostics.error(
                        package,
                        format!("store {} binds {name}({written}) twice", self.name),
                    );
                    continue;
                }
                let label = format!("store {} {name}({written})", self.name);
                if let Some(bound) =
                    self.bind(variant, binding, &label, package, graph, diagnostics)
                {
                    self.bindings[variant.id.index()] = Some((package.to_string(), bound));
                }
            }
        }
    }

    fn bind(
        &self,
        variant: &RelationshipVariant,
        binding: &Binding<Raw>,
        label: &str,
        package: &str,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) -> Option<Binding<Resolved>> {
        if binding.fk.is_none() && binding.edge_table.is_none() {
            diagnostics.error(
                package,
                format!("{label} names neither a foreign key nor an edge table"),
            );
            return None;
        }
        if binding.index.is_some() && (binding.fk.is_none() || binding.edge_table.is_some()) {
            diagnostics.error(
                package,
                format!(
                    "{label} derives an edge index, which needs a foreign key and no edge table"
                ),
            );
            return None;
        }
        let fk = match &binding.fk {
            Some(key) => Some(self.key(variant, key, label, package, graph, diagnostics)?),
            None => None,
        };
        if (binding.edge_table.is_some() || binding.index.is_some())
            && self.source(variant.source).is_none()
        {
            diagnostics.error(
                package,
                format!("{label} names an edge table but the store does not hold its source"),
            );
            return None;
        }
        let mut copies = IndexMap::new();
        for (column, sources) in &binding.copies {
            let resolved: Option<Vec<(Endpoint, PropertyId)>> = sources
                .values()
                .iter()
                .map(|source| {
                    let (side, property) = source.split_once('.')?;
                    let side = crate::spec::parse_word::<Endpoint>(side)?;
                    Some((side, graph.property_id(variant.endpoint(side), property)?))
                })
                .collect();
            let resolved = diagnostics.require(resolved, package, || {
                format!("{label} copy {column} names no property of an endpoint")
            })?;
            copies.insert(column.clone(), resolved);
        }
        Some(Binding {
            fk,
            edge_table: binding.edge_table.clone(),
            index: binding.index.clone(),
            copies,
        })
    }

    fn key(
        &self,
        variant: &RelationshipVariant,
        key: &KeySpec,
        label: &str,
        package: &str,
        graph: &GraphCatalog,
        diagnostics: &mut Diagnostics,
    ) -> Option<Key> {
        if !variant.cardinality.allows_key_on(key.on) {
            diagnostics.error(
                package,
                format!(
                    "{label} is {:?} and cannot hold a foreign key on its {:?}",
                    variant.cardinality, key.on
                ),
            );
            return None;
        }
        let holder = variant.endpoint(key.on);
        let holder_name = &graph.entity(holder).name;
        let column = |written: &str, what: &str, diagnostics: &mut Diagnostics| {
            let id = graph
                .property_id(holder, written)
                .filter(|id| self.source(holder).is_some() && self.column(*id).is_some());
            diagnostics.require(id, package, || {
                format!("{label} {what} {written} is not a column of {holder_name}")
            })
        };
        let columns = key.column.values();
        let properties = columns
            .iter()
            .map(|written| column(written, "foreign key", diagnostics))
            .collect::<Option<Vec<PropertyId>>>()?;
        let discriminator = match (&key.type_column, &key.type_value) {
            (None, None) => None,
            (Some(written), Some(values)) if properties.len() == 1 => Some(Discriminator {
                column: column(written, "type column", diagnostics)?,
                values: values.values().to_vec(),
            }),
            (Some(_), Some(_)) => {
                diagnostics.error(
                    package,
                    format!("{label} composite foreign key cannot carry a type column"),
                );
                return None;
            }
            _ => {
                diagnostics.error(
                    package,
                    format!("{label} foreign key needs both a type column and a type value"),
                );
                return None;
            }
        };
        let other = match key.on {
            Endpoint::Source => Endpoint::Target,
            Endpoint::Target => Endpoint::Source,
        };
        let referenced = variant.endpoint(other);
        let identity = &graph.entity(referenced).identity;
        if identity.len() != properties.len() {
            diagnostics.error(
                package,
                format!(
                    "{label} names {} key columns, but {} has {} identity properties",
                    properties.len(),
                    graph.entity(referenced).name,
                    identity.len()
                ),
            );
            return None;
        }
        for ((property, written), referenced_key) in properties.iter().zip(columns).zip(identity) {
            let (held, id) = (
                graph.property(*property).data_type,
                graph.property(*referenced_key).data_type,
            );
            if held != id {
                diagnostics.error(
                    package,
                    format!(
                        "{label} foreign key {written} is {held:?} but {}.{} is {id:?}; keys are never cast",
                        graph.entity(referenced).name,
                        graph.property(*referenced_key).name
                    ),
                );
                return None;
            }
        }
        let pairs: Vec<(PropertyId, PropertyId)> = properties
            .iter()
            .copied()
            .zip(identity.iter().copied())
            .collect();
        Some(Key {
            foreign_key: ForeignKey {
                holder: key.on,
                property: properties[0],
                referenced_key: identity[0],
            },
            discriminator,
            composite: match pairs.len() {
                1 => Vec::new(),
                _ => pairs,
            },
        })
    }
}

/// Variants that key on the same columns but reach different entities each name their own
/// discriminator value; a composite key carries none, so two of them on the same columns clash.
fn check_shared_keys(graph: &GraphCatalog, store: &Store, diagnostics: &mut Diagnostics) {
    let keys: Vec<(&RelationshipVariant, &str, &Key)> = store
        .bindings()
        .filter_map(|(variant, package, binding)| {
            Some((graph.variant(variant), package, binding.fk.as_ref()?))
        })
        .collect();
    for (index, (variant, package, key)) in keys.iter().enumerate() {
        let columns = key_columns(key);
        let referenced = key
            .foreign_key
            .referenced_entity(variant.source, variant.target);
        let clash = keys[..index].iter().find(|(other, _, other_key)| {
            key_columns(other_key) == columns
                && other_key
                    .foreign_key
                    .referenced_entity(other.source, other.target)
                    != referenced
                && match (&key.discriminator, &other_key.discriminator) {
                    (Some(mine), Some(theirs)) => mine
                        .values
                        .iter()
                        .any(|value| theirs.values.contains(value)),
                    _ => true,
                }
        });
        if let Some((other, _, _)) = clash {
            diagnostics.error(
                package,
                format!(
                    "store {} keys {} and {} on {} without distinct type values",
                    store.name,
                    label(graph, other),
                    label(graph, variant),
                    columns
                        .iter()
                        .map(|column| graph.property(*column).name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            );
        }
    }
}

/// The holder columns a key reads: its one column, or each part of a composite key.
fn key_columns(key: &Key) -> Vec<PropertyId> {
    match key.composite.is_empty() {
        true => vec![key.foreign_key.property],
        false => key.composite.iter().map(|(held, _)| *held).collect(),
    }
}

fn label(graph: &GraphCatalog, variant: &RelationshipVariant) -> String {
    let relationship = &graph.relationship(variant.relationship).name;
    let source = &graph.entity(variant.source).name;
    let target = &graph.entity(variant.target).name;
    format!("{relationship}({source} -> {target})")
}

/// The one store that marks the entity `complete: true`.
fn complete_store<'s>(
    entity: &crate::spec::Entity,
    holders: &[&'s Store],
    diagnostics: &mut Diagnostics,
) -> Option<&'s Store> {
    let mut complete = holders
        .iter()
        .copied()
        .filter(|store| store.complete(entity.id));
    let first = complete.next();
    match (first, complete.next()) {
        (None, _) => diagnostics.error(
            &entity.package,
            format!(
                "{} is stored in {} but no store marks it complete: true",
                entity.name,
                holders
                    .iter()
                    .map(|store| store.name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ),
        (Some(first), Some(second)) => diagnostics.error(
            &second.package,
            format!(
                "stores {} and {} both mark {} complete: true; exactly one store is complete",
                first.name, second.name, entity.name
            ),
        ),
        _ => {}
    }
    first
}

fn agree_on_text(
    entity: &crate::spec::Entity,
    graph: &GraphCatalog,
    holders: &[&Store],
    property: PropertyId,
    diagnostics: &mut Diagnostics,
) {
    let mut stored = holders
        .iter()
        .filter(|store| store.column(property).is_some());
    if let Some(first) = stored.next()
        && let Some(other) =
            stored.find(|store| store.stored_as_text(property) != first.stored_as_text(property))
    {
        diagnostics.error(
            &other.package,
            format!(
                "stores {} and {} disagree on whether {}.{} is stored as text",
                first.name,
                other.name,
                entity.name,
                graph.property(property).name
            ),
        );
    }
}

/// The checks that span stores: every property and variant is bound somewhere, stores agree on
/// immutability, and every foreign key joins within its connection.
pub(super) fn check(
    graph: &GraphCatalog,
    stores: &[Store],
    served: &HashSet<PropertyId>,
    diagnostics: &mut Diagnostics,
) {
    for entity in graph.entities() {
        let holders: Vec<&Store> = stores
            .iter()
            .filter(|store| store.source(entity.id).is_some())
            .collect();
        if holders.is_empty() {
            continue;
        }
        let complete = complete_store(entity, &holders, diagnostics);
        for id in &entity.properties {
            let covered = served.contains(id)
                || complete.is_none_or(|store| store.column(*id).is_some() || store.excludes(*id));
            if !covered {
                diagnostics.error(
                    &entity.package,
                    format!(
                        "store {} is complete for {} but neither stores nor excludes {}.{}",
                        complete.map_or("", |store| store.name.as_str()),
                        entity.name,
                        entity.name,
                        graph.property(*id).name
                    ),
                );
            }
            agree_on_text(entity, graph, &holders, *id, diagnostics);
            let mut stored = holders.iter().filter(|store| store.column(*id).is_some());
            if let Some(first) = stored.next()
                && let Some(other) =
                    stored.find(|store| store.immutable(*id) != first.immutable(*id))
            {
                diagnostics.error(
                    &other.package,
                    format!(
                        "stores {} and {} disagree on whether {}.{} is immutable",
                        first.name,
                        other.name,
                        entity.name,
                        graph.property(*id).name
                    ),
                );
            }
        }
    }
    for store in stores {
        check_shared_keys(graph, store, diagnostics);
    }
    for variant in graph.variants() {
        let on = |connection: &str, entity| {
            stores
                .iter()
                .any(|store| store.connection == connection && store.source(entity).is_some())
        };
        for store in stores {
            let holds =
                store.source(variant.source).is_some() && on(&store.connection, variant.target);
            match store.binding(variant.id) {
                Some(binding) => {
                    let tableless = store.sources().all(|source| source.table.is_none());
                    let joins = binding.fk.as_ref().is_none_or(|key| {
                        let referenced = key
                            .foreign_key
                            .referenced_entity(variant.source, variant.target);
                        tableless || !key.composite.is_empty() || on(&store.connection, referenced)
                    }) && (store.edge_table(variant.id).is_none()
                        || on(&store.connection, variant.target));
                    if !joins {
                        diagnostics.error(
                            &store.package,
                            format!(
                                "store {} binds {}, whose other endpoint is not on connection {}",
                                store.name,
                                label(graph, variant),
                                store.connection
                            ),
                        );
                    }
                }
                None if holds && !store.declared.contains(&variant.id) => diagnostics.error(
                    &store.package,
                    format!(
                        "store {} holds both endpoints of {} but does not bind it",
                        store.name,
                        label(graph, variant)
                    ),
                ),
                None => {}
            }
        }
        if !stores
            .iter()
            .any(|store| store.declared.contains(&variant.id))
        {
            let package = &graph.entity(variant.source).package;
            let message = format!("{} is bound in no store", label(graph, variant));
            diagnostics.error(package, message);
        }
    }
}

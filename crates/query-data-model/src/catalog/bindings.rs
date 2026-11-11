use std::collections::HashSet;

use indexmap::IndexMap;

use super::Diagnostics;
use super::store::Store;
use crate::constants::DEFAULT_PRIMARY_KEY;
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
        let property = column(&key.column, "foreign key", diagnostics)?;
        let discriminator = match (&key.type_column, &key.type_value) {
            (None, None) => None,
            (Some(written), Some(values)) => Some(Discriminator {
                column: column(written, "type column", diagnostics)?,
                values: values.values().to_vec(),
            }),
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
        let referenced_key = graph.property_id(referenced, DEFAULT_PRIMARY_KEY);
        let referenced_key = diagnostics.require(referenced_key, package, || {
            format!(
                "{label} references {}, which has no id",
                graph.entity(referenced).name
            )
        })?;
        let (held, id) = (
            graph.property(property).data_type,
            graph.property(referenced_key).data_type,
        );
        if held != id {
            diagnostics.error(
                package,
                format!(
                    "{label} foreign key {} is {held:?} but {}.id is {id:?}; keys are never cast",
                    key.column,
                    graph.entity(referenced).name
                ),
            );
            return None;
        }
        Some(Key {
            foreign_key: ForeignKey {
                holder: key.on,
                property,
                referenced_key,
            },
            discriminator,
        })
    }
}

/// Variants that key on one column but reach different entities each name their own
/// discriminator value.
fn check_shared_keys(graph: &GraphCatalog, store: &Store, diagnostics: &mut Diagnostics) {
    let keys: Vec<(&RelationshipVariant, &str, &Key)> = store
        .bindings()
        .filter_map(|(variant, package, binding)| {
            Some((graph.variant(variant), package, binding.fk.as_ref()?))
        })
        .collect();
    for (index, (variant, package, key)) in keys.iter().enumerate() {
        let column = key.foreign_key.property;
        let referenced = key
            .foreign_key
            .referenced_entity(variant.source, variant.target);
        let clash = keys[..index].iter().find(|(other, _, other_key)| {
            other_key.foreign_key.property == column
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
                    graph.property(column).name
                ),
            );
        }
    }
}

fn label(graph: &GraphCatalog, variant: &RelationshipVariant) -> String {
    let relationship = &graph.relationship(variant.relationship).name;
    let source = &graph.entity(variant.source).name;
    let target = &graph.entity(variant.target).name;
    format!("{relationship}({source} -> {target})")
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
        for id in &entity.properties {
            let covered = served.contains(id)
                || holders
                    .iter()
                    .any(|store| store.column(*id).is_some() || store.excludes(*id));
            if !covered {
                diagnostics.error(
                    &entity.package,
                    format!(
                        "{}.{} is neither stored nor excluded by any store",
                        entity.name,
                        graph.property(*id).name
                    ),
                );
            }
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
                    let joins = binding.fk.as_ref().is_none_or(|key| {
                        let referenced = key
                            .foreign_key
                            .referenced_entity(variant.source, variant.target);
                        on(&store.connection, referenced)
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

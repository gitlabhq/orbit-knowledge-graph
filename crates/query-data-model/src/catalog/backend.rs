use std::fmt::Debug;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::{Access, Diagnostics, Model, Source, Store};
use crate::spec::{BackendOptions, decode_options, parse_variant};
use crate::{DataModelError, EntityId, GraphCatalog, RelationshipId, RelationshipVariantId};

pub trait StorageBackend: Sized {
    const KIND: &'static str;
    type Options: DeserializeOwned + Default;
    type SourceOptions: DeserializeOwned + Default;
    type RouteOptions: DeserializeOwned + Default;
    type Store: Debug;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> Self::Store;
}

pub struct SourceInput<'a, O> {
    pub entity: EntityId,
    pub package: &'a str,
    pub options: O,
}

pub struct RouteInput<'a, O> {
    pub relationship: RelationshipId,
    pub package: &'a str,
    pub options: O,
    pub variants: Vec<(RelationshipVariantId, O)>,
}

pub struct StoreInput<'a, B: StorageBackend> {
    pub graph: &'a GraphCatalog,
    pub access: &'a Access,
    pub store: &'a Store,
    pub connection: &'a [&'a Store],
    pub options: B::Options,
    pub sources: Vec<SourceInput<'a, B::SourceOptions>>,
    pub routes: Vec<RouteInput<'a, B::RouteOptions>>,
}

#[derive(Debug)]
pub struct Catalog<B: StorageBackend> {
    model: Arc<Model>,
    connection: String,
    stores: Vec<(usize, B::Store)>,
}

impl<B: StorageBackend> Catalog<B> {
    pub fn model(&self) -> &Model {
        &self.model
    }

    pub fn graph(&self) -> &GraphCatalog {
        self.model.graph()
    }

    pub fn connection(&self) -> &str {
        &self.connection
    }

    pub fn stores(&self) -> impl Iterator<Item = (&Store, &B::Store)> {
        self.stores
            .iter()
            .map(|(index, facts)| (&self.model.stores[*index], facts))
    }

    pub fn store(&self, name: &str) -> Option<&Store> {
        self.stores()
            .find(|(store, _)| store.name == name)
            .map(|(store, _)| store)
    }

    pub fn backend(&self, name: &str) -> Option<&B::Store> {
        self.stores()
            .find(|(store, _)| store.name == name)
            .map(|(_, facts)| facts)
    }

    pub fn sources(&self, entity: EntityId) -> impl Iterator<Item = (&Store, &Source)> {
        self.stores()
            .filter_map(move |(store, _)| Some((store, store.source(entity)?)))
    }

    pub fn home(&self, entity: EntityId) -> Option<(&Store, &B::Store)> {
        self.stores()
            .find(|(store, _)| store.source(entity).is_some())
    }
}

impl Model {
    pub fn physical<B: StorageBackend>(
        self: &Arc<Self>,
        connection: &str,
    ) -> Result<Catalog<B>, DataModelError> {
        let indexes: Vec<usize> = (0..self.stores.len())
            .filter(|index| self.stores[*index].connection == connection)
            .collect();
        let members: Vec<&Store> = indexes.iter().map(|index| &self.stores[*index]).collect();
        let Some(first) = members.first() else {
            return Err(DataModelError::UnknownReference {
                kind: "connection",
                name: connection.to_string(),
            });
        };
        if first.kind != B::KIND {
            let mut diagnostics = Diagnostics::default();
            diagnostics.error(
                &first.package,
                format!("connection {connection} is {}, not {}", first.kind, B::KIND),
            );
            diagnostics.finish()?;
        }
        let mut diagnostics = Diagnostics::default();
        let inputs: Vec<StoreInput<'_, B>> = members
            .iter()
            .map(|store| self.input::<B>(store, &members, &mut diagnostics))
            .collect();
        diagnostics.finish()?;
        let mut diagnostics = Diagnostics::default();
        let stores = indexes
            .iter()
            .zip(inputs)
            .map(|(index, input)| (*index, B::resolve(input, &mut diagnostics)))
            .collect();
        diagnostics.finish()?;
        Ok(Catalog {
            model: Arc::clone(self),
            connection: connection.to_string(),
            stores,
        })
    }

    fn input<'a, B: StorageBackend>(
        &'a self,
        store: &'a Store,
        connection: &'a [&'a Store],
        diagnostics: &mut Diagnostics,
    ) -> StoreInput<'a, B> {
        let graph = &self.graph;
        let context = format!("store {}", store.name);
        let sources = store
            .source_options
            .iter()
            .map(|(entity, package, options)| SourceInput {
                entity: *entity,
                package,
                options: parse(
                    package,
                    &format!("{context} {}", graph.entity(*entity).name),
                    options,
                    diagnostics,
                ),
            })
            .collect();
        let routes = store
            .routes
            .iter()
            .map(|route| {
                let name = &graph.relationship(route.relationship).name;
                let label = format!("{context} route {name}");
                RouteInput {
                    relationship: route.relationship,
                    package: &route.package,
                    options: parse(&route.package, &label, &route.spec.backend, diagnostics),
                    variants: route
                        .spec
                        .variants
                        .iter()
                        .filter_map(|(variant, spec)| {
                            let (source, target) = parse_variant(variant)?;
                            let id = graph.variant_named(name, source, target)?.id;
                            let label = format!("{label}({variant})");
                            Some((
                                id,
                                parse(&route.package, &label, &spec.backend, diagnostics),
                            ))
                        })
                        .collect(),
                }
            })
            .collect();
        StoreInput {
            access: &self.access,
            graph,
            store,
            connection,
            options: parse(&store.package, &context, &store.options, diagnostics),
            sources,
            routes,
        }
    }
}

fn parse<T: DeserializeOwned + Default>(
    package: &str,
    context: &str,
    options: &BackendOptions,
    diagnostics: &mut Diagnostics,
) -> T {
    decode_options(options).unwrap_or_else(|error| {
        diagnostics.error(package, format!("{context}: {error}"));
        T::default()
    })
}

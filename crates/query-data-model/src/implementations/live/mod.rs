mod render;

pub use render::{ExtractionRequest, RequestFilter, Window};

use serde::Deserialize;

use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::{EntityId, PropertyId};

#[derive(Debug, Clone)]
pub struct Live;

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveOptions {}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveRoute {}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveSource {
    pub operation: String,
    #[serde(default)]
    pub filters: Vec<String>,
    #[serde(default)]
    pub text: Option<String>,
}

/// One API operation of the service and what it can push down.
#[derive(Debug)]
pub struct Endpoint {
    pub entity: EntityId,
    pub operation: String,
    pub filters: Vec<PropertyId>,
    pub text: Option<PropertyId>,
}

#[derive(Debug, Default)]
pub struct LiveStore {
    endpoints: Vec<Endpoint>,
}

impl LiveStore {
    pub fn endpoint(&self, entity: EntityId) -> Option<&Endpoint> {
        self.endpoints
            .iter()
            .find(|endpoint| endpoint.entity == entity)
    }
}

impl StorageBackend for Live {
    const KIND: &'static str = "live";
    type Options = LiveOptions;
    type SourceOptions = LiveSource;
    type RouteOptions = LiveRoute;
    type Store = LiveStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> LiveStore {
        let graph = input.graph;
        let store = &input.store.name;
        let mut endpoints = Vec::new();
        for source in input.sources {
            let name = &graph.entity(source.entity).name;
            if source.options.operation.is_empty() {
                diagnostics.error(
                    source.package,
                    format!("store {store} names no operation for {name}"),
                );
            }
            let mut resolve = |property: &str| {
                let id = graph
                    .property_id(source.entity, property)
                    .filter(|id| input.store.column(*id).is_some());
                diagnostics.require(id, source.package, || {
                    format!("store {store} pushes down unknown property {name}.{property}")
                })
            };
            let filters = source
                .options
                .filters
                .iter()
                .filter_map(|property| resolve(property))
                .collect();
            let text = source.options.text.as_deref().and_then(&mut resolve);
            endpoints.push(Endpoint {
                entity: source.entity,
                operation: source.options.operation,
                filters,
                text,
            });
        }
        LiveStore { endpoints }
    }
}

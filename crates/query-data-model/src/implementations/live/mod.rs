mod render;

pub use render::{ExtractionRequest, RequestFilter, Window};

use serde::Deserialize;

use crate::catalog::{
    Diagnostics, ExecuteError, NoOptions, PolicyKind, Row, StorageBackend, StoreInput,
    check_unrelational,
};
use crate::{EntityId, PropertyId};

#[derive(Debug, Clone)]
pub struct Live;

/// The Data Extraction Service, which runs a request with the signed-in user's own credentials.
pub trait DataExtraction {
    fn extract(&self, user: &str, request: &ExtractionRequest) -> Result<Vec<Row>, ExecuteError>;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveSource {
    pub operation: String,
    #[serde(default)]
    pub filters: Vec<String>,
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
    const POLICIES: &'static [PolicyKind] = &[PolicyKind::Via, PolicyKind::None];
    type Options = NoOptions;
    type SourceOptions = Option<LiveSource>;
    type ColumnOptions = NoOptions;
    type Store = LiveStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> LiveStore {
        let graph = input.graph;
        let store = &input.store.name;
        let mut endpoints = Vec::new();
        check_unrelational(&input, diagnostics);
        for source in &input.entities {
            let name = &graph.entity(source.entity).name;
            let Some(options) =
                diagnostics.require(source.options.as_ref(), source.package, || {
                    format!("store {store} names no operation for {name}")
                })
            else {
                continue;
            };
            let mut resolve = |property: &str| {
                let id = graph
                    .property_id(source.entity, property)
                    .filter(|id| input.store.column(*id).is_some());
                diagnostics.require(id, source.package, || {
                    format!("store {store} pushes down unknown property {name}.{property}")
                })
            };
            let filters: Vec<PropertyId> = options
                .filters
                .iter()
                .filter_map(|property| resolve(property))
                .collect();
            let text = options.text.as_deref().and_then(&mut resolve);
            let accepted = input.store.source(source.entity).map(|held| &held.accepts);
            for property in accepted.into_iter().flatten() {
                if !filters.contains(property) {
                    diagnostics.error(
                        source.package,
                        format!(
                            "store {store} accepts keys on {name}.{}, which its operation does not filter on",
                            graph.property(*property).name
                        ),
                    );
                }
            }
            endpoints.push(Endpoint {
                entity: source.entity,
                operation: options.operation.clone(),
                filters,
                text,
            });
        }
        LiveStore { endpoints }
    }
}

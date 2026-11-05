use serde::{Deserialize, Serialize};

use crate::PropertyId;
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};

#[derive(Debug, Clone)]
pub struct Search;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchOptions {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRoute {}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub text: Vec<String>,
}

#[derive(Debug)]
pub struct SearchStore {
    text: Vec<bool>,
}

impl SearchStore {
    pub fn is_text(&self, property: PropertyId) -> bool {
        self.text[property.index()]
    }
}

impl StorageBackend for Search {
    const KIND: &'static str = "search";
    type Options = SearchOptions;
    type SourceOptions = SearchSource;
    type RouteOptions = SearchRoute;
    type Store = SearchStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> SearchStore {
        let graph = input.graph;
        let store = input.store;
        let mut text = vec![false; graph.properties().count()];
        for source in &input.sources {
            let name = &graph.entity(source.entity).name;
            for property in &source.options.text {
                let id = graph
                    .property_id(source.entity, property)
                    .filter(|id| store.column(*id).is_some());
                if let Some(id) = diagnostics.require(id, source.package, || {
                    format!("store {} does not store {name}.{property}", store.name)
                }) {
                    text[id.index()] = true;
                }
            }
        }
        SearchStore { text }
    }
}

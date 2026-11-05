use indexmap::IndexMap;
use ontology::FieldSelectivity;
use serde::{Deserialize, Serialize};

use super::layout::ClickHouse;
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{
    RelationalFacts, RelationalOptions, RelationalRoute, RelationalStore,
};
use crate::{EntityId, PropertyId};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub indexes: IndexMap<String, IndexKinds>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub low_selectivity: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub high_fan_out: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexKinds {
    One(String),
    Many(Vec<String>),
}

impl IndexKinds {
    pub fn kinds(&self) -> &[String] {
        match self {
            Self::One(kind) => std::slice::from_ref(kind),
            Self::Many(kinds) => kinds,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    Text,
    Bloom,
    Set,
    MinMax,
}

impl IndexKind {
    fn parse(word: &str) -> Option<Self> {
        match word.split('(').next()? {
            "text" => Some(Self::Text),
            "bloom" | "bloom_filter" => Some(Self::Bloom),
            "set" => Some(Self::Set),
            "minmax" => Some(Self::MinMax),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClickHouseColumn {
    pub indexes: Vec<IndexKind>,
    pub selectivity: FieldSelectivity,
}

#[derive(Debug)]
pub struct ClickHouseStore {
    pub relational: RelationalStore,
    columns: Vec<Option<ClickHouseColumn>>,
    high_fan_out: Vec<bool>,
}

impl ClickHouseStore {
    pub fn column(&self, property: PropertyId) -> Option<&ClickHouseColumn> {
        self.columns[property.index()].as_ref()
    }

    pub fn high_fan_out(&self, entity: EntityId) -> bool {
        self.high_fan_out[entity.index()]
    }
}

impl RelationalFacts for ClickHouseStore {
    fn relational(&self) -> &RelationalStore {
        &self.relational
    }
}

impl StorageBackend for ClickHouse {
    const KIND: &'static str = "clickhouse";
    type Options = RelationalOptions;
    type SourceOptions = ClickHouseSource;
    type RouteOptions = RelationalRoute;
    type Store = ClickHouseStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> ClickHouseStore {
        let graph = input.graph;
        let store = input.store;
        let relational = RelationalStore::resolve(
            &input,
            &input.options,
            input
                .sources
                .iter()
                .map(|source| (source.entity, source.options.sort_key.clone())),
            diagnostics,
        );
        let mut columns = vec![None; graph.properties().count()];
        let mut high_fan_out = vec![false; graph.entities().count()];
        for source in &input.sources {
            let entity = source.entity;
            for property in &graph.entity(entity).properties {
                if store.column(*property).is_some() {
                    columns[property.index()] = Some(ClickHouseColumn {
                        indexes: Vec::new(),
                        selectivity: FieldSelectivity::from_data_type(
                            graph.property(*property).data_type,
                        ),
                    });
                }
            }
            let name = &graph.entity(entity).name;
            let stored = |property: &str, diagnostics: &mut Diagnostics| {
                let id = graph
                    .property_id(entity, property)
                    .filter(|id| store.column(*id).is_some());
                diagnostics.require(id, source.package, || {
                    format!("store {} does not store {name}.{property}", store.name)
                })
            };
            let options = &source.options;
            for (property, kinds) in &options.indexes {
                let Some(id) = stored(property, diagnostics) else {
                    continue;
                };
                for kind in kinds.kinds() {
                    let parsed =
                        diagnostics.require(IndexKind::parse(kind), source.package, || {
                            format!("{name}.{property} has unknown index kind `{kind}`")
                        });
                    if let (Some(parsed), Some(column)) = (parsed, columns[id.index()].as_mut()) {
                        column.indexes.push(parsed);
                    }
                }
            }
            for property in &options.low_selectivity {
                if let Some(column) =
                    stored(property, diagnostics).and_then(|id| columns[id.index()].as_mut())
                {
                    column.selectivity = FieldSelectivity::Low;
                }
            }
            high_fan_out[entity.index()] = options
                .high_fan_out
                .iter()
                .filter_map(|property| stored(property, diagnostics))
                .count()
                > 0;
        }
        ClickHouseStore {
            relational,
            columns,
            high_fan_out,
        }
    }
}

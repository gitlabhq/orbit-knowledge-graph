mod authorization;
mod from_ontology;
mod graph;
mod storage;

pub use authorization::{
    AuthorizationFragment, EntityAccessSpec, PropertyAccessSpec, RedactionSpec, ScopeSpec,
    ScopeVariants,
};
pub use from_ontology::from_ontology;
pub(crate) use from_ontology::graph_fragments;
pub use graph::{EntitySpec, GraphFragment, PropertyDetail, PropertySpec, RelationshipSpec};
pub use storage::{
    EdgeTableSpec, EngineSpec, IndexKinds, LookupSpec, RouteSpec, SourceSpec, StorageFragment,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fragment {
    Graph(GraphFragment),
    Storage(Box<StorageFragment>),
    Authorization(AuthorizationFragment),
}

impl Fragment {
    pub fn parse(yaml: &str) -> Result<Self, orbit_utils::yaml::Error> {
        orbit_utils::yaml::from_str(yaml)
    }

    pub fn to_yaml(&self) -> Result<String, orbit_utils::yaml::SerializeError> {
        orbit_utils::yaml::to_string(self)
    }

    pub fn package(&self) -> &str {
        match self {
            Self::Graph(fragment) => &fragment.package,
            Self::Storage(fragment) => &fragment.package,
            Self::Authorization(fragment) => &fragment.package,
        }
    }
}

pub(crate) fn parse_variant(variant: &str) -> Option<(&str, &str)> {
    let (source, target) = variant.split_once("->")?;
    let (source, target) = (source.trim(), target.trim());
    (!source.is_empty() && !target.is_empty()).then_some((source, target))
}

pub(crate) fn parse_word<'a, T: Deserialize<'a>>(word: &'a str) -> Option<T> {
    T::deserialize(serde::de::value::BorrowedStrDeserializer::<
        serde::de::value::Error,
    >::new(word))
    .ok()
}

pub(crate) fn format_variant(source: &str, target: &str) -> String {
    format!("{source} -> {target}")
}

pub(crate) fn snake_case(name: &str) -> String {
    let mut snake = String::with_capacity(name.len() + 4);
    for (index, character) in name.char_indices() {
        if character.is_uppercase() && index > 0 {
            snake.push('_');
        }
        snake.extend(character.to_lowercase());
    }
    snake
}

fn is_false(value: &bool) -> bool {
    !value
}

fn is_true(value: &bool) -> bool {
    *value
}

fn yes() -> bool {
    true
}

fn explicit_null<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

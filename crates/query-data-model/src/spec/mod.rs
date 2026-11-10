mod authorization;
mod graph;
mod service;
mod source;
mod storage;

pub use authorization::{
    AuthorizationFragment, EntityAccessSpec, PropertyAccess, RedactionSpec, ScopeSpec,
    ScopeVariants,
};
pub use graph::{
    AttributeDetail, AttributeSpec, EntitySpec, GraphFragment, PropertyDetail, PropertySpec,
    RelationshipSpec,
};
pub use service::{LookupSpec, ServiceFragment};
pub use source::{
    DerivedSourceSpec, EdgePipelineSpec, EdgeSpec, EndpointKindSpec, EndpointSpec,
    EntitySourceSpec, ExtractLookupSpec, GENERATED_QUERY, PathSpec, ReindexSpec,
    RelationshipSourceSpec, SourceDeclaration, SourceFragment, TEMPLATE_EXTENSION,
};
pub use storage::{
    BackendOptions, EngineSpec, OptionValue, RouteSpec, SourceSpec, StorageFragment,
    decode_options, encode_options,
};

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fragment {
    Graph(GraphFragment),
    Storage(Box<StorageFragment>),
    Authorization(AuthorizationFragment),
    Service(ServiceFragment),
    Source(Box<SourceFragment>),
}

impl Fragment {
    pub fn parse(yaml: &str) -> Result<Self, orbit_utils::yaml::Error> {
        orbit_utils::yaml::from_str(yaml)
    }

    pub fn parse_all<'a>(
        sources: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Vec<Self>, crate::DataModelError> {
        let mut diagnostics = crate::catalog::Diagnostics::default();
        let mut templates = BTreeMap::<&str, BTreeMap<String, String>>::new();
        let mut fragments = Vec::new();
        for (name, text) in sources {
            let (directory, file) = name.rsplit_once('/').unwrap_or(("", name));
            if file.ends_with(TEMPLATE_EXTENSION) {
                templates
                    .entry(directory)
                    .or_default()
                    .insert(file.to_string(), text.to_string());
                continue;
            }
            match Self::parse(text) {
                Ok(fragment) => fragments.push((directory, fragment)),
                Err(error) => diagnostics.error(name, error.to_string()),
            }
        }
        diagnostics.finish()?;
        Ok(fragments
            .into_iter()
            .map(|(directory, mut fragment)| {
                if let Self::Source(source) = &mut fragment {
                    source.templates = templates.get(directory).cloned().unwrap_or_default();
                }
                fragment
            })
            .collect())
    }

    pub fn package(&self) -> &str {
        match self {
            Self::Graph(fragment) => &fragment.package,
            Self::Storage(fragment) => &fragment.package,
            Self::Authorization(fragment) => &fragment.package,
            Self::Service(fragment) => &fragment.package,
            Self::Source(fragment) => &fragment.package,
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

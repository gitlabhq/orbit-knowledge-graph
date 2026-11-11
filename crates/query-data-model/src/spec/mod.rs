mod authorization;
mod graph;
mod service;
mod source;
mod storage;

pub use authorization::{
    Anchor, AuthorizationFragment, EntityAccess, PropertyAccess, Redaction, ScopeSpec,
    ScopeVariants,
};
pub use graph::{
    Attribute, DefaultValue, Entity, GraphFragment, Property, Relationship, RelationshipVariant,
};
pub use service::{Lookup, ServiceFragment};
pub use source::{
    EdgeMapping, Endpoint, EndpointKind, ExtractLookup, GENERATED_QUERY, PathSpec, PipelineSpec,
    ReindexTrigger, RelationshipSourceSpec, SourceDeclaration, SourceFragment, TEMPLATE_EXTENSION,
};
pub use storage::{
    AccessHints, BackendOptions, Binding, ColumnStorage, CopySources, Discriminator, EdgeIndex,
    EngineSpec, EntityStorage, Key, KeySpec, OneOrMany, OptionValue, StorageFragment,
    decode_options, encode_options,
};

use std::collections::BTreeMap;
use std::fmt::Debug;

use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{EntityId, PropertyId, RelationshipId};

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

/// A value a spec field holds in either stage.
pub trait Value: Debug + Clone + PartialEq + Serialize + DeserializeOwned {}

impl<T: Debug + Clone + PartialEq + Serialize + DeserializeOwned> Value for T {}

/// How specs name graph elements: by written name before the loader resolves them, by id after.
pub trait Naming: Value + Copy + Default {
    type Entity: Value;
    type Property: Value;
    type Relationship: Value;
    /// The written type, such as `int64?`, before resolve; the data type after.
    type Type: Value;
    /// Whether an endpoint enriches before resolve; the properties it carries after.
    type Enrich: Value + Default;
    type Key: Debug + Clone + PartialEq;
    type Copies: Debug + Clone + PartialEq;
    /// A field the loader fills in.
    type Loaded<T: Value + Default>: Value + Default;
    /// A field the written form may omit and the loader defaults.
    type Defaulted<T: Value>: Value;
    /// Members written as a map by name, resolved to ids in declaration order.
    type Members<T: Value, Id: Value>: Value + Default;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Raw;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Resolved;

impl Naming for Raw {
    type Entity = String;
    type Property = String;
    type Relationship = String;
    type Type = String;
    type Enrich = bool;
    type Key = KeySpec;
    type Copies = CopySources;
    type Loaded<T: Value + Default> = ();
    type Defaulted<T: Value> = Option<T>;
    type Members<T: Value, Id: Value> = IndexMap<String, T>;
}

impl Naming for Resolved {
    type Entity = EntityId;
    type Property = PropertyId;
    type Relationship = RelationshipId;
    type Type = crate::DataType;
    type Enrich = IndexMap<PropertyId, String>;
    type Key = Key;
    type Copies = Vec<(crate::Endpoint, PropertyId)>;
    type Loaded<T: Value + Default> = T;
    type Defaulted<T: Value> = T;
    type Members<T: Value, Id: Value> = Vec<Id>;
}

/// A value written as its full form, or as just the value of its `KEY` field.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct Short<T>(pub T);

pub trait ShortForm {
    const KEY: &'static str;
}

impl<'de, T: ShortForm + Deserialize<'de>> Deserialize<'de> for Short<T> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Form<T> {
            Key(String),
            Full(T),
        }
        let value = match Form::deserialize(deserializer)? {
            Form::Full(full) => return Ok(Self(full)),
            Form::Key(value) => value,
        };
        let written = std::iter::once((T::KEY, value));
        T::deserialize(serde::de::value::MapDeserializer::<_, D::Error>::new(
            written,
        ))
        .map(Self)
    }
}

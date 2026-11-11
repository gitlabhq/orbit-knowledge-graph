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
    AccessHints, Binding, Block, Blocks, ColumnStorage, CopySources, Discriminator, EdgeIndex,
    EngineSpec, EntityStorage, Erased, Key, KeySpec, OneOrMany, StorageFragment, Typed,
};

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use indexmap::IndexMap;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::catalog::{Diagnostics, ModelId, registered};
use crate::{EntityId, PropertyId, RelationshipId};

/// One package file. Its storage blocks parse only once the store's engine kind is known.
#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Fragment<S = Box<StorageFragment>> {
    Graph(GraphFragment),
    Storage(S),
    Authorization(AuthorizationFragment),
    Service(ServiceFragment),
    Source(Box<SourceFragment>),
}

/// A storage fragment's store and engine kind, which choose the backend that parses its blocks.
#[derive(Deserialize)]
struct StorageHeader {
    package: String,
    store: String,
    engine: Option<EngineKind>,
}

#[derive(Deserialize)]
struct EngineKind {
    kind: String,
}

/// The fragments of a set of packages, identified by the bytes they were parsed from.
#[derive(Debug)]
pub struct Fragments {
    fragments: Vec<Fragment>,
    id: ModelId,
}

impl Fragments {
    pub fn parse<'a>(
        sources: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, crate::DataModelError> {
        let sources: Vec<(&str, &str)> = sources.into_iter().collect();
        let id = ModelId::digest(
            sources
                .iter()
                .flat_map(|(name, text)| [name.as_bytes(), text.as_bytes()]),
        );
        let mut diagnostics = Diagnostics::default();
        let mut templates = BTreeMap::<&str, BTreeMap<String, String>>::new();
        let mut parsed = Vec::new();
        for (name, text) in &sources {
            let (directory, file) = name.rsplit_once('/').unwrap_or(("", name));
            if file.ends_with(TEMPLATE_EXTENSION) {
                templates
                    .entry(directory)
                    .or_default()
                    .insert(file.to_string(), text.to_string());
                continue;
            }
            match orbit_utils::yaml::from_str::<Fragment<StorageHeader>>(text) {
                Ok(fragment) => parsed.push((*name, directory, *text, fragment)),
                Err(error) => diagnostics.error(name, error.to_string()),
            }
        }
        let mut kinds = HashMap::new();
        for (_, _, _, fragment) in &parsed {
            if let Fragment::Storage(StorageHeader {
                store,
                engine: Some(engine),
                ..
            }) = fragment
            {
                kinds
                    .entry(store.clone())
                    .or_insert_with(|| engine.kind.clone());
            }
        }
        let mut fragments = Vec::with_capacity(parsed.len());
        for (name, directory, text, fragment) in parsed {
            fragments.push(match fragment {
                Fragment::Storage(header) => {
                    let (package, store) = (&header.package, &header.store);
                    let Some(kind) = kinds.get(store) else {
                        diagnostics.error(package, format!("store {store} declares no engine"));
                        continue;
                    };
                    let Some(backend) = registered(kind) else {
                        diagnostics.error(
                            package,
                            format!("store {store} has unknown engine kind {kind}"),
                        );
                        continue;
                    };
                    match backend.parse(text) {
                        Ok(storage) => Fragment::Storage(Box::new(storage)),
                        Err(error) => {
                            diagnostics.error(name, error.to_string());
                            continue;
                        }
                    }
                }
                Fragment::Graph(graph) => Fragment::Graph(graph),
                Fragment::Authorization(access) => Fragment::Authorization(access),
                Fragment::Service(service) => Fragment::Service(service),
                Fragment::Source(mut source) => {
                    source.templates = templates.get(directory).cloned().unwrap_or_default();
                    Fragment::Source(source)
                }
            });
        }
        diagnostics.finish()?;
        Ok(Self { fragments, id })
    }

    pub fn id(&self) -> ModelId {
        self.id
    }

    /// Marks fragments rewritten under `tag`, so their model differs from the untagged one.
    pub(crate) fn tag(&mut self, tag: &str) {
        self.id = self.id.tagged(tag);
    }
}

impl Deref for Fragments {
    type Target = Vec<Fragment>;

    fn deref(&self) -> &Vec<Fragment> {
        &self.fragments
    }
}

impl DerefMut for Fragments {
    fn deref_mut(&mut self) -> &mut Vec<Fragment> {
        &mut self.fragments
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
pub trait Value: Debug + Clone + PartialEq + DeserializeOwned {}

impl<T: Debug + Clone + PartialEq + DeserializeOwned> Value for T {}

/// How specs name graph elements: by written name before the loader resolves them, by id after.
pub trait Naming: Debug + Clone + Copy + PartialEq {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Raw;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq)]
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

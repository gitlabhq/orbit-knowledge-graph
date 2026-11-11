mod authorization;
mod graph;
mod service;
mod source;
mod storage;

pub use authorization::{
    AnchorSpec, AuthorizationFragment, EntityAccessSpec, PropertyAccess, RedactionSpec, ScopeSpec,
    ScopeVariants,
};
pub use graph::{
    AttributeDetail, AttributeSpec, DefaultValue, EntitySpec, GraphFragment, PropertyDetail,
    PropertySpec, RelationshipSpec, VariantSpec,
};
pub use service::{LookupSpec, ServiceFragment};
pub use source::{
    EdgeSpec, EndpointKindSpec, EndpointSpec, ExtractLookupSpec, GENERATED_QUERY, PathSpec,
    PipelineSpec, ReindexSpec, RelationshipSourceSpec, SourceDeclaration, SourceFragment,
    TEMPLATE_EXTENSION,
};
pub use storage::{
    AccessHints, Binding, Block, Blocks, ColumnStorage, CopySources, Discriminator, EdgeIndex,
    EngineSpec, EntityStorage, Erased, Key, KeySpec, Naming, OneOrMany, Raw, Resolved,
    StorageFragment, Typed,
};

use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};

use serde::Deserialize;

use crate::catalog::{Diagnostics, ModelId, registered};

#[derive(Debug)]
pub enum Fragment {
    Graph(GraphFragment),
    Storage(Box<StorageFragment>),
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

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Parsed {
    Graph(GraphFragment),
    Storage(StorageHeader),
    Authorization(AuthorizationFragment),
    Service(ServiceFragment),
    Source(Box<SourceFragment>),
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
            match orbit_utils::yaml::from_str::<Parsed>(text) {
                Ok(fragment) => parsed.push((*name, directory, *text, fragment)),
                Err(error) => diagnostics.error(name, error.to_string()),
            }
        }
        let mut kinds = HashMap::new();
        for (_, _, _, fragment) in &parsed {
            if let Parsed::Storage(StorageHeader {
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
                Parsed::Storage(header) => {
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
                Parsed::Graph(graph) => Fragment::Graph(graph),
                Parsed::Authorization(access) => Fragment::Authorization(access),
                Parsed::Service(service) => Fragment::Service(service),
                Parsed::Source(mut source) => {
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

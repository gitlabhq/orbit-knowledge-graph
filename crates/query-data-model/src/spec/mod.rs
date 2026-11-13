mod authorization;
mod graph;
mod schemas;
mod service;
mod source;
pub(crate) mod storage;

pub use authorization::{
    AuthorizationFragment, CheckSpec, EdgePolicy, EdgeScope, PackageManifest, PolicySpec,
    PolicyWord, PropertyAccess, PropertyClasses, ScopeKeySpec, ScopeKind,
};
pub use graph::{
    Attribute, DefaultValue, Entity, GraphFragment, Property, Relationship, RelationshipVariant,
};
pub use schemas::schemas;
pub use service::{Lookup, ServiceFragment};
pub use source::{
    CODE_INDEXER_SOURCE, EdgeMapping, Endpoint, EndpointKind, ExtractLookup, GENERATED_QUERY,
    PathSpec, PipelineSpec, ReindexTrigger, SourceFragment, TEMPLATE_EXTENSION, Writes,
};
pub use storage::{
    AccessHints, Binding, Block, Blocks, CopySources, Discriminator, EngineSpec, EntityStorage,
    Key, KeySpec, OneOrMany, StorageFragment, Typed, WrittenBlock,
};

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::ops::{Deref, DerefMut};

use indexmap::IndexMap;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use crate::catalog::{Diagnostics, ModelId, registered};
use crate::packages::{Tier, directory};
use crate::{EntityId, PropertyId, RelationshipId};

/// One package file. Its storage blocks parse only once the store's engine kind is known.
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[schemars(bound = "S: JsonSchema")]
pub enum Fragment<S = Box<StorageFragment>> {
    Graph(GraphFragment),
    Storage(S),
    Authorization(AuthorizationFragment),
    Service(ServiceFragment),
    Source(Box<SourceFragment>),
    Package(PackageManifest),
}

impl Fragment {
    pub fn package(&self) -> &str {
        match self {
            Self::Graph(graph) => &graph.package,
            Self::Storage(storage) => &storage.package,
            Self::Authorization(access) => &access.package,
            Self::Service(service) => &service.package,
            Self::Source(source) => &source.package,
            Self::Package(manifest) => &manifest.name,
        }
    }
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
    tiers: BTreeMap<String, Tier>,
    id: ModelId,
    /// The connections one deployment serves, whose storage fragments alone these keep.
    served: Option<Vec<String>>,
    /// What the storage fragments of the stores on unserved connections hold.
    elsewhere: Elsewhere,
}

/// The entities and relationship variants the stores of a deployment's unserved connections hold:
/// they belong to other deployments, not to none.
#[derive(Debug, Clone, Default)]
pub struct Elsewhere {
    entities: std::collections::HashSet<String>,
    variants: std::collections::HashSet<(String, String)>,
}

impl Elsewhere {
    pub fn holds(&self, entity: &str) -> bool {
        self.entities.contains(entity)
    }

    /// Whether an unserved store binds the variant `source -> target` of `relationship`.
    pub fn binds(&self, relationship: &str, source: &str, target: &str) -> bool {
        self.variants
            .contains(&(relationship.to_string(), format!("{source}->{target}")))
    }
}

impl Fragments {
    pub fn parse<'a>(
        sources: impl IntoIterator<Item = (&'a str, &'a str)>,
        tiers: &BTreeMap<String, Tier>,
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
        let mut owners = Vec::with_capacity(parsed.len());
        for (name, directory, text, fragment) in parsed {
            let fragment = match fragment {
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
                Fragment::Package(manifest) => Fragment::Package(manifest),
                Fragment::Source(mut source) => {
                    source.templates = templates.get(directory).cloned().unwrap_or_default();
                    Fragment::Source(source)
                }
            };
            owners.push((name, fragment.package().to_string()));
            fragments.push(fragment);
        }
        let tiers = package_tiers(&owners, &fragments, tiers, &mut diagnostics);
        diagnostics.finish()?;
        Ok(Self {
            fragments,
            tiers,
            id,
            served: None,
            elsewhere: Elsewhere::default(),
        })
    }

    /// Only the storage fragments whose store sits on a connection the deployment shape `shape`
    /// of `deployments` serves; the model's id names the shape's connections.
    pub fn deployment(
        self,
        deployments: &crate::Deployments,
        shape: &str,
    ) -> Result<Self, crate::DataModelError> {
        Ok(self.serving(deployments.connections(shape)?))
    }

    /// Only the storage fragments of the shape `shape` in `config/deployments.yaml`.
    pub fn deploy(self, shape: &str) -> Result<Self, crate::DataModelError> {
        self.deployment(&crate::Deployments::embedded(), shape)
    }

    fn serving(mut self, connections: &[String]) -> Self {
        let mut served = connections.to_vec();
        served.sort();
        served.dedup();
        let on: HashMap<String, String> = self
            .fragments
            .iter()
            .filter_map(|fragment| match fragment {
                Fragment::Storage(storage) => storage
                    .engine
                    .as_ref()
                    .map(|engine| (storage.store.clone(), engine.connection.clone())),
                _ => None,
            })
            .collect();
        let mut elsewhere = Elsewhere::default();
        self.fragments.retain(|fragment| match fragment {
            Fragment::Storage(storage) => {
                let kept = on
                    .get(&storage.store)
                    .is_some_and(|connection| served.contains(connection));
                if !kept {
                    elsewhere.entities.extend(storage.entities.keys().cloned());
                    for (relationship, variants) in &storage.relationships {
                        for variant in variants.keys() {
                            let variant: String =
                                variant.chars().filter(|c| !c.is_whitespace()).collect();
                            elsewhere.variants.insert((relationship.clone(), variant));
                        }
                    }
                }
                kept
            }
            _ => true,
        });
        self.elsewhere = elsewhere;
        self.id = self.id.tagged(&format!("serving {}", served.join(",")));
        self.served = Some(served);
        self
    }

    /// What the stores on the connections the deployment does not serve hold.
    pub fn elsewhere(&self) -> &Elsewhere {
        &self.elsewhere
    }

    /// The connections the deployment these fragments describe serves.
    pub fn served(&self) -> Option<&[String]> {
        self.served.as_deref()
    }

    pub fn id(&self) -> ModelId {
        self.id
    }

    /// The tier the package named `package` was loaded at.
    pub fn tier(&self, package: &str) -> Option<Tier> {
        self.tiers.get(package).copied()
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

/// Each package's tier, from where its directory was loaded. A directory holds one manifest
/// named after it, and every fragment in it names that package.
fn package_tiers(
    owners: &[(&str, String)],
    fragments: &[Fragment],
    loaded: &BTreeMap<String, Tier>,
    diagnostics: &mut Diagnostics,
) -> BTreeMap<String, Tier> {
    let mut manifests = BTreeMap::new();
    for ((path, _), fragment) in owners.iter().zip(fragments) {
        let Fragment::Package(manifest) = fragment else {
            continue;
        };
        let directory = directory(path);
        if manifest.name != directory {
            diagnostics.error(
                path,
                format!(
                    "package.yaml names package {}, but sits in directory {directory}",
                    manifest.name
                ),
            );
        }
        if manifests.insert(directory, &manifest.name).is_some() {
            diagnostics.error(
                path,
                format!("directory {directory} holds two package manifests"),
            );
        }
    }
    let mut tiers = BTreeMap::new();
    for (path, package) in owners {
        let directory = directory(path);
        let Some(name) = manifests.get(directory) else {
            diagnostics.error(
                path,
                format!("{path} sits in {directory}, which has no package.yaml"),
            );
            continue;
        };
        if package != *name {
            diagnostics.error(path, format!("{path} names package {package}, not {name}"));
        }
        match loaded.get(directory) {
            Some(tier) => {
                tiers.insert(package.clone(), *tier);
            }
            None => diagnostics.error(path, format!("package {directory} was loaded at no tier")),
        }
    }
    tiers
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
    /// A field the loader requires, so that its absence is one diagnostic among the rest.
    type Required<T: Value>: Value;
    /// One identity property, or several in order.
    type Identity: Value;
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
    type Required<T: Value> = Option<T>;
    type Identity = OneOrMany;
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
    type Required<T: Value> = T;
    type Identity = Vec<PropertyId>;
    type Members<T: Value, Id: Value> = Vec<Id>;
}

/// A value written as its full form, or as just the value of its `KEY` field.
#[derive(Debug, Clone, PartialEq)]
pub struct Short<T>(pub T);

pub trait ShortForm {
    const KEY: &'static str;
}

impl<T: ShortForm + JsonSchema> JsonSchema for Short<T> {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
        format!("Short_{}", T::schema_name()).into()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        let key = T::json_schema(generator)
            .get("properties")
            .and_then(|properties| properties.get(T::KEY))
            .cloned()
            .expect("a short form's key is a field of its full form");
        schemars::json_schema!({"anyOf": [key, generator.subschema_for::<T>()]})
    }
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

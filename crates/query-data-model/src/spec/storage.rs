use std::any::Any;
use std::fmt::Debug;

use indexmap::IndexMap;
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::explicit_null;
use crate::{Endpoint, ForeignKey, PropertyId};

/// How storage specs name graph elements: by written name before the loader resolves them,
/// by id after.
pub trait Naming {
    type Property: Debug + Clone + PartialEq;
    type Key: Debug + Clone + PartialEq;
    type Copies: Debug + Clone + PartialEq;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Raw;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolved;

impl Naming for Raw {
    type Property = String;
    type Key = KeySpec;
    type Copies = CopySources;
}

impl Naming for Resolved {
    type Property = PropertyId;
    type Key = Key;
    type Copies = Vec<(Endpoint, PropertyId)>;
}

/// A backend block, typed by its store's backend when the fragment is parsed.
pub trait Block: Any + Debug + Send + Sync {}

impl<T: Any + Debug + Send + Sync> Block for T {}

impl dyn Block {
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        (self as &dyn Any).downcast_ref()
    }

    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        (self as &mut dyn Any).downcast_mut()
    }
}

/// The types of a storage fragment's backend blocks. A block the YAML omits reads as its
/// type's default, so a backend that requires a block types it as an `Option`.
pub trait Blocks {
    type Engine: Debug;
    type Entity: Debug;
    type Column: Debug;
}

/// The blocks of a fragment whose backend only the store's own catalog reads.
#[derive(Debug)]
pub struct Erased;

impl Blocks for Erased {
    type Engine = Box<dyn Block>;
    type Entity = Box<dyn Block>;
    type Column = Box<dyn Block>;
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
pub struct StorageFragment<K: Blocks = Erased> {
    pub package: String,
    pub store: String,
    pub engine: Option<EngineSpec<K>>,
    #[serde(default)]
    pub entities: IndexMap<String, EntityStorage<K>>,
    #[serde(default)]
    pub relationships: IndexMap<String, IndexMap<String, Binding<Raw>>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
pub struct EngineSpec<K: Blocks = Erased> {
    pub kind: String,
    pub connection: String,
    #[serde(default)]
    pub trusted: bool,
    pub path_column: Option<String>,
    #[serde(default)]
    pub backend: K::Engine,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
pub struct EntityStorage<K: Blocks = Erased> {
    pub table: Option<String>,
    #[serde(default, deserialize_with = "explicit_null")]
    pub path_column: Option<Option<String>>,
    #[serde(default)]
    pub columns: IndexMap<String, ColumnStorage<K>>,
    #[serde(default)]
    pub exclude: Vec<String>,
    #[serde(default)]
    pub access: AccessHints<Raw>,
    #[serde(default)]
    pub backend: K::Entity,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
pub struct ColumnStorage<K: Blocks = Erased> {
    #[serde(default)]
    pub immutable: bool,
    pub column: Option<String>,
    #[serde(default)]
    pub backend: K::Column,
}

/// The blocks of one backend, which a fragment of its stores parses into.
pub trait Typed:
    Blocks<
        Engine: Block + DeserializeOwned + Default,
        Entity: Block + DeserializeOwned + Default,
        Column: Block + DeserializeOwned + Default,
    >
{
}

impl<K: Typed> StorageFragment<K> {
    pub(crate) fn parse(yaml: &str) -> Result<StorageFragment, orbit_utils::yaml::Error> {
        #[derive(Deserialize)]
        #[serde(tag = "kind", rename_all = "snake_case", bound = "K: Typed")]
        enum Tagged<K: Typed> {
            Storage(StorageFragment<K>),
        }
        let Tagged::Storage(fragment) = orbit_utils::yaml::from_str::<Tagged<K>>(yaml)?;
        Ok(fragment.erase())
    }

    fn erase(self) -> StorageFragment {
        fn erase<T: Block>(block: T) -> Box<dyn Block> {
            Box::new(block)
        }
        let column = |column: ColumnStorage<K>| ColumnStorage {
            immutable: column.immutable,
            column: column.column,
            backend: erase(column.backend),
        };
        let entity = |entity: EntityStorage<K>| EntityStorage {
            table: entity.table,
            path_column: entity.path_column,
            columns: entity
                .columns
                .into_iter()
                .map(|(name, spec)| (name, column(spec)))
                .collect(),
            exclude: entity.exclude,
            access: entity.access,
            backend: erase(entity.backend),
        };
        StorageFragment {
            package: self.package,
            store: self.store,
            engine: self.engine.map(|engine| EngineSpec {
                kind: engine.kind,
                connection: engine.connection,
                trusted: engine.trusted,
                path_column: engine.path_column,
                backend: erase(engine.backend),
            }),
            entities: self
                .entities
                .into_iter()
                .map(|(name, spec)| (name, entity(spec)))
                .collect(),
            relationships: self.relationships,
        }
    }
}

/// How queries read an entity, in terms every backend turns into its own indexes and order.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    default,
    deny_unknown_fields,
    bound(deserialize = "S::Property: Deserialize<'de>")
)]
pub struct AccessHints<S: Naming> {
    pub filter: Vec<S::Property>,
    pub text: Vec<S::Property>,
    pub order: Vec<S::Property>,
}

impl<S: Naming> Default for AccessHints<S> {
    fn default() -> Self {
        Self {
            filter: Vec::new(),
            text: Vec::new(),
            order: Vec::new(),
        }
    }
}

/// Where one relationship variant's rows live: a foreign key, an edge table, or both.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "S::Key: Deserialize<'de>, S::Copies: Deserialize<'de>")
)]
pub struct Binding<S: Naming> {
    pub fk: Option<S::Key>,
    pub edge_table: Option<String>,
    pub index: Option<EdgeIndex>,
    #[serde(default)]
    pub copies: IndexMap<String, S::Copies>,
}

/// An edge table that the variant's writer derives from its foreign key, read in its place.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeIndex {
    pub edge_table: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    pub on: Endpoint,
    pub column: String,
    pub type_column: Option<String>,
    pub type_value: Option<OneOrMany>,
}

/// A foreign key, with the discriminator that tells its variant apart when several variants
/// share the column.
#[derive(Debug, Clone, PartialEq)]
pub struct Key {
    pub foreign_key: ForeignKey,
    pub discriminator: Option<Discriminator>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Discriminator {
    pub column: PropertyId,
    pub values: Vec<String>,
}

/// One written value or a list of them.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

/// The endpoint properties an edge column copies, each written `source.<property>` or
/// `target.<property>`.
pub type CopySources = OneOrMany;

impl OneOrMany {
    pub fn values(&self) -> &[String] {
        match self {
            Self::One(source) => std::slice::from_ref(source),
            Self::Many(sources) => sources,
        }
    }
}

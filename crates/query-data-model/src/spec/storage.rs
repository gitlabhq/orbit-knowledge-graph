use std::any::Any;
use std::fmt::Debug;

use indexmap::IndexMap;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::Deserialize;
use serde::de::DeserializeOwned;

use super::{Naming, Raw, explicit_null};
use crate::catalog::StorageBackend;
use crate::{Endpoint, FieldSelectivity, ForeignKey, PropertyId};

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

/// A backend block's schema. The loader reads an omitted block as its default, and reports a
/// missing block the backend requires: one typed as an `Option`, or one with a required field.
pub trait WrittenBlock: JsonSchema {
    fn written(generator: &mut SchemaGenerator) -> Schema {
        generator.subschema_for::<Self>()
    }

    fn required() -> bool {
        Self::json_schema(&mut SchemaGenerator::default())
            .get("required")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|required| !required.is_empty())
    }
}

impl<T: JsonSchema> WrittenBlock for Option<T> {
    fn written(generator: &mut SchemaGenerator) -> Schema {
        generator.subschema_for::<T>()
    }

    fn required() -> bool {
        true
    }
}

fn block<T: WrittenBlock>(generator: &mut SchemaGenerator) -> Schema {
    T::written(generator)
}

fn optional_block<T: WrittenBlock>(schema: &mut Schema) {
    if T::required() {
        return;
    }
    if let Some(required) = schema
        .get_mut("required")
        .and_then(serde_json::Value::as_array_mut)
    {
        required.retain(|field| field != "backend");
        if required.is_empty() {
            schema.remove("required");
        }
    }
}

fn engine_kind<B: StorageBackend>(_: &mut SchemaGenerator) -> Schema {
    schemars::json_schema!({"const": B::KIND})
}

/// The blocks of a fragment whose backend only the store's own catalog reads.
#[derive(Debug)]
pub struct Erased;

impl Blocks for Erased {
    type Engine = Box<dyn Block>;
    type Entity = Box<dyn Block>;
    type Column = Box<dyn Block>;
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
#[schemars(bound = "K: StorageBackend")]
pub struct StorageFragment<K: Blocks = Erased> {
    pub package: String,
    pub store: String,
    pub engine: Option<EngineSpec<K>>,
    #[serde(default)]
    pub entities: IndexMap<String, EntityStorage<K>>,
    #[serde(default)]
    pub relationships: IndexMap<String, IndexMap<String, Binding<Raw>>>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
#[schemars(bound = "K: StorageBackend", transform = optional_block::<K::Options>)]
pub struct EngineSpec<K: Blocks = Erased> {
    #[schemars(schema_with = "engine_kind::<K>")]
    pub kind: String,
    pub connection: String,
    #[serde(default)]
    pub trusted: bool,
    pub path_column: Option<String>,
    #[serde(default)]
    #[schemars(!default, schema_with = "block::<K::Options>")]
    pub backend: K::Engine,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
#[schemars(bound = "K: StorageBackend", transform = optional_block::<K::SourceOptions>)]
pub struct EntityStorage<K: Blocks = Erased> {
    /// This store holds or excludes every graph property of the entity, and plans read it here.
    #[serde(default)]
    pub complete: bool,
    /// The stored properties a stage takes key sets on.
    #[schemars(required)]
    pub accepts: Option<Vec<String>>,
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
    #[schemars(!default, schema_with = "block::<K::SourceOptions>")]
    pub backend: K::Entity,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, bound = "K: Typed")]
#[schemars(bound = "K: StorageBackend", transform = optional_block::<K::ColumnOptions>)]
pub struct ColumnStorage<K: Blocks = Erased> {
    #[serde(default)]
    pub immutable: bool,
    pub column: Option<String>,
    /// How well an equality filter on the column narrows; without it the planner judges by type.
    pub selectivity: Option<FieldSelectivity>,
    /// The column holds an enum's labels rather than its integer codes.
    #[serde(default)]
    pub stored_as_text: bool,
    #[serde(default)]
    #[schemars(!default, schema_with = "block::<K::ColumnOptions>")]
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
            selectivity: column.selectivity,
            stored_as_text: column.stored_as_text,
            backend: erase(column.backend),
        };
        let entity = |entity: EntityStorage<K>| EntityStorage {
            complete: entity.complete,
            accepts: entity.accepts,
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
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(
    default,
    deny_unknown_fields,
    bound(deserialize = "S::Property: Deserialize<'de>")
)]
#[schemars(bound = "S::Property: JsonSchema")]
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
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(
    deny_unknown_fields,
    bound(deserialize = "S::Key: Deserialize<'de>, S::Copies: Deserialize<'de>")
)]
#[schemars(bound = "S::Key: JsonSchema, S::Copies: JsonSchema")]
pub struct Binding<S: Naming> {
    pub fk: Option<S::Key>,
    pub edge_table: Option<String>,
    pub index: Option<EdgeIndex>,
    #[serde(default)]
    pub copies: IndexMap<String, S::Copies>,
}

/// An edge table that the variant's writer derives from its foreign key, read in its place.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct EdgeIndex {
    pub edge_table: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    pub on: Endpoint,
    /// One column, or one per part of a composite identity, in identity order.
    pub column: OneOrMany,
    pub type_column: Option<String>,
    pub type_value: Option<OneOrMany>,
}

/// A foreign key, with the discriminator that tells its variant apart when several variants
/// share the column.
#[derive(Debug, Clone, PartialEq)]
pub struct Key {
    pub foreign_key: ForeignKey,
    pub discriminator: Option<Discriminator>,
    /// Every holder column and the identity property it matches, for a key on a composite
    /// identity; empty for a key on one column.
    pub composite: Vec<(PropertyId, PropertyId)>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Discriminator {
    pub column: PropertyId,
    pub values: Vec<String>,
}

/// One written value or a list of them.
#[derive(Debug, Clone, PartialEq, Deserialize, JsonSchema)]
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

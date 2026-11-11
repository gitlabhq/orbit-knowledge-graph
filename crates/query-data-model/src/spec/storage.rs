use std::fmt::Debug;

use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{explicit_null, is_false};
use crate::{Endpoint, ForeignKey, PropertyId};

pub type BackendOptions = IndexMap<String, OptionValue>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OptionValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    List(Vec<OptionValue>),
    Map(IndexMap<String, OptionValue>),
}

pub fn encode_options(value: impl Serialize) -> Result<BackendOptions, serde_json::Error> {
    serde_json::from_str(&serde_json::to_string(&value)?)
}

pub fn decode_options<T: DeserializeOwned>(
    options: &BackendOptions,
) -> Result<T, serde_json::Error> {
    serde_json::from_str(&serde_json::to_string(options)?)
}

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

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageFragment {
    pub package: String,
    pub store: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<EngineSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, EntityStorage>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, IndexMap<String, Binding<Raw>>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EngineSpec {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub trusted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_column: Option<String>,
    #[serde(default, skip_serializing_if = "BackendOptions::is_empty")]
    pub backend: BackendOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EntityStorage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(
        default,
        deserialize_with = "explicit_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub path_column: Option<Option<String>>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, ColumnStorage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude: Vec<String>,
    #[serde(default, skip_serializing_if = "AccessHints::is_empty")]
    pub access: AccessHints<Raw>,
    #[serde(default, skip_serializing_if = "BackendOptions::is_empty")]
    pub backend: BackendOptions,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnStorage {
    #[serde(default, skip_serializing_if = "is_false")]
    pub immutable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    #[serde(default, skip_serializing_if = "BackendOptions::is_empty")]
    pub backend: BackendOptions,
}

/// How queries read an entity, in terms every backend turns into its own indexes and order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(
        serialize = "S::Property: Serialize",
        deserialize = "S::Property: Deserialize<'de>"
    )
)]
pub struct AccessHints<S: Naming> {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub filter: Vec<S::Property>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub text: Vec<S::Property>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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

impl<S: Naming> AccessHints<S> {
    pub fn is_empty(&self) -> bool {
        self.filter.is_empty() && self.text.is_empty() && self.order.is_empty()
    }
}

/// Where one relationship variant's rows live: a foreign key, an edge table, or both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(
        serialize = "S::Key: Serialize, S::Copies: Serialize",
        deserialize = "S::Key: Deserialize<'de>, S::Copies: Deserialize<'de>"
    )
)]
pub struct Binding<S: Naming> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fk: Option<S::Key>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index: Option<EdgeIndex>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub copies: IndexMap<String, S::Copies>,
}

/// An edge table that the variant's writer derives from its foreign key, read in its place.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeIndex {
    pub edge_table: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeySpec {
    pub on: Endpoint,
    pub column: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub type_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

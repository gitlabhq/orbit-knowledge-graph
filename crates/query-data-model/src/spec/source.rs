use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};

use super::{Naming, Raw, Resolved, Short, ShortForm};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::{EntityId, RelationshipVariantId};

pub const GENERATED_QUERY: &str = "generated";
pub const TEMPLATE_EXTENSION: &str = ".sql.j2";

/// The source whose rows the code indexer writes itself, with no pipelines.
pub const CODE_INDEXER_SOURCE: &str = "code_indexer";

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(try_from = "WrittenFragment")]
pub struct SourceFragment {
    pub package: String,
    pub source: String,
    pub declare: Option<SourceDeclaration>,
    pub entities: Pipelines,
    pub relationships: IndexMap<String, RelationshipSourceSpec>,
    pub derived: Pipelines,
    /// What the source writes itself, listed by name instead of by pipeline.
    pub writes: Writes,
    pub templates: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct Writes {
    pub entities: Vec<String>,
    /// Each relationship's written variants, as `Source -> Target`.
    pub relationships: IndexMap<String, Vec<String>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WrittenFragment {
    package: String,
    source: String,
    #[serde(default)]
    declare: Option<SourceDeclaration>,
    #[serde(default)]
    entities: Option<Listed<Pipelines>>,
    #[serde(default)]
    relationships: IndexMap<String, Listed<RelationshipSourceSpec>>,
    #[serde(default, deserialize_with = "derived")]
    derived: Pipelines,
}

impl TryFrom<WrittenFragment> for SourceFragment {
    type Error = String;

    fn try_from(written: WrittenFragment) -> Result<Self, String> {
        let mut writes = Writes::default();
        let entities = match written.entities {
            None => Pipelines::new(),
            Some(Listed::Names(names)) => {
                writes.entities = names;
                Pipelines::new()
            }
            Some(Listed::Specs(pipelines)) => {
                for (name, spec) in &pipelines {
                    spec.check(name, "entities")?;
                }
                pipelines
            }
        };
        let mut relationships = IndexMap::new();
        for (name, section) in written.relationships {
            match section {
                Listed::Names(variants) => {
                    writes.relationships.insert(name, variants);
                }
                Listed::Specs(spec) => {
                    relationships.insert(name, spec);
                }
            }
        }
        Ok(Self {
            package: written.package,
            source: written.source,
            declare: written.declare,
            entities,
            relationships,
            derived: written.derived,
            writes,
            templates: BTreeMap::new(),
        })
    }
}

/// A section written as a list of names, or as a map of specs.
enum Listed<T> {
    Names(Vec<String>),
    Specs(T),
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Listed<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visit<T>(std::marker::PhantomData<T>);

        impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Visit<T> {
            type Value = Listed<T>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a list of names or a map of specs")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                seq: A,
            ) -> Result<Self::Value, A::Error> {
                Vec::deserialize(serde::de::value::SeqAccessDeserializer::new(seq))
                    .map(Listed::Names)
            }

            fn visit_map<A: serde::de::MapAccess<'de>>(
                self,
                map: A,
            ) -> Result<Self::Value, A::Error> {
                T::deserialize(serde::de::value::MapAccessDeserializer::new(map)).map(Listed::Specs)
            }
        }

        deserializer.deserialize_any(Visit(std::marker::PhantomData))
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDeclaration {
    pub connection: String,
    pub version: String,
    pub watermark: String,
    pub deleted: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipSourceSpec {
    #[serde(default)]
    pub reindex: Vec<Short<ReindexTrigger<Raw>>>,
    #[serde(deserialize_with = "relationship_pipelines")]
    pub pipelines: Pipelines,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub tables: Vec<String>,
    pub order_by: Vec<String>,
    pub query: String,
    pub fields: Option<Vec<String>>,
    pub filter: Option<String>,
    pub lookups: Option<Vec<ExtractLookup<Raw>>>,
    pub reindex: Option<Vec<Short<ReindexTrigger<Raw>>>>,
    pub partition_count: Option<u32>,
    pub columns: Option<IndexMap<String, String>>,
    pub binary: Option<Vec<String>>,
    pub enrich: Option<Vec<String>>,
    pub edges: Option<Vec<EdgeMapping<Raw>>>,
    pub from: Option<Endpoint<Raw>>,
    pub to: Option<Endpoint<Raw>>,
    pub emits: Option<Vec<String>>,
    pub transform: Option<String>,
}

type Pipelines = IndexMap<String, PipelineSpec>;

/// The fields each source section allows, and those it requires.
fn rules(section: &str) -> (&'static [&'static str], &'static [&'static str]) {
    match section {
        "entities" => (
            &[
                "filter",
                "reindex",
                "partition_count",
                "columns",
                "binary",
                "enrich",
                "edges",
            ],
            &[],
        ),
        "relationships" => (
            &["fields", "filter", "lookups", "from", "to"],
            &["from", "to"],
        ),
        _ => (&["reindex", "emits", "transform"], &["emits", "transform"]),
    }
}

impl PipelineSpec {
    fn check(&self, name: &str, section: &str) -> Result<(), String> {
        let present = [
            ("fields", self.fields.is_some()),
            ("filter", self.filter.is_some()),
            ("lookups", self.lookups.is_some()),
            ("reindex", self.reindex.is_some()),
            ("partition_count", self.partition_count.is_some()),
            ("columns", self.columns.is_some()),
            ("binary", self.binary.is_some()),
            ("enrich", self.enrich.is_some()),
            ("edges", self.edges.is_some()),
            ("from", self.from.is_some()),
            ("to", self.to.is_some()),
            ("emits", self.emits.is_some()),
            ("transform", self.transform.is_some()),
        ];
        let (allows, requires) = rules(section);
        for (field, set) in present {
            if set && !allows.contains(&field) {
                return Err(format!(
                    "pipeline {name} sets {field}, which the {section} section does not allow"
                ));
            }
            if !set && requires.contains(&field) {
                return Err(format!(
                    "pipeline {name} omits {field}, which the {section} section requires"
                ));
            }
        }
        Ok(())
    }
}

fn pipelines<'de, D: Deserializer<'de>>(
    deserializer: D,
    section: &str,
) -> Result<Pipelines, D::Error> {
    let pipelines = Pipelines::deserialize(deserializer)?;
    for (name, spec) in &pipelines {
        spec.check(name, section).map_err(D::Error::custom)?;
    }
    Ok(pipelines)
}

fn relationship_pipelines<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Pipelines, D::Error> {
    pipelines(deserializer, "relationships")
}

fn derived<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Pipelines, D::Error> {
    pipelines(deserializer, "derived")
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct ExtractLookup<S: Naming = Resolved> {
    pub entity: S::Entity,
    #[serde(rename = "id")]
    pub id_column: String,
    #[serde(default)]
    pub fields: IndexMap<String, String>,
    #[serde(skip)]
    pub table: S::Loaded<String>,
    #[serde(skip)]
    pub namespaced: S::Loaded<bool>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct EdgeMapping<S: Naming = Resolved> {
    #[serde(rename = "label")]
    pub relationship: S::Relationship,
    #[serde(rename = "from")]
    pub source: Endpoint<S>,
    #[serde(rename = "to")]
    pub target: Endpoint<S>,
    #[serde(default)]
    pub mutable: bool,
    #[serde(skip)]
    pub variants: S::Loaded<Vec<RelationshipVariantId>>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Endpoint<S: Naming = Resolved> {
    pub field: String,
    pub kind: EndpointKind<S>,
    #[serde(default, rename = "enrich")]
    pub properties: S::Enrich,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged, bound = "")]
pub enum EndpointKind<S: Naming = Resolved> {
    Entity(S::Entity),
    Column {
        #[serde(rename = "derive")]
        column: String,
        mapping: BTreeMap<String, S::Entity>,
    },
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct ReindexTrigger<S: Naming = Resolved> {
    pub table: String,
    #[serde(skip)]
    pub target: S::Loaded<String>,
    #[serde(default = "PathSpec::traversal_path")]
    pub traversal_path: PathSpec,
    #[serde(skip)]
    pub watermark: S::Loaded<String>,
}

impl<S: Naming> ShortForm for ReindexTrigger<S> {
    const KEY: &'static str = "table";
}

impl Eq for ReindexTrigger {}

impl PartialOrd for ReindexTrigger {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ReindexTrigger {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (
            &self.table,
            &self.target,
            &self.traversal_path,
            &self.watermark,
        )
            .cmp(&(
                &other.table,
                &other.target,
                &other.traversal_path,
                &other.watermark,
            ))
    }
}

impl EndpointKind {
    pub fn entity(&self) -> Option<EntityId> {
        match self {
            Self::Entity(entity) => Some(*entity),
            Self::Column { .. } => None,
        }
    }

    pub fn matches(&self, entity: EntityId) -> bool {
        self.entity().is_none_or(|kind| kind == entity)
    }

    pub(crate) fn entities(&self) -> Vec<EntityId> {
        match self {
            Self::Entity(entity) => vec![*entity],
            Self::Column { mapping, .. } => mapping.values().copied().collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PathSpec {
    Column(#[serde(deserialize_with = "column_path")] String),
    Dictionary {
        dictionary: String,
        key_column: String,
    },
}

impl PathSpec {
    fn traversal_path() -> Self {
        Self::Column(TRAVERSAL_PATH_COLUMN.into())
    }
}

fn column_path<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ColumnPath {
        column: String,
    }
    ColumnPath::deserialize(deserializer).map(|path| path.column)
}

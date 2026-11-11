use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use super::{Naming, Raw, Resolved, Short, ShortForm, is_false};
use crate::constants::TRAVERSAL_PATH_COLUMN;
use crate::{EntityId, RelationshipVariantId};

pub const GENERATED_QUERY: &str = "generated";
pub const TEMPLATE_EXTENSION: &str = ".sql.j2";

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceFragment {
    pub package: String,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declare: Option<SourceDeclaration>,
    #[serde(
        default,
        skip_serializing_if = "IndexMap::is_empty",
        deserialize_with = "entities"
    )]
    pub entities: Pipelines,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, RelationshipSourceSpec>,
    #[serde(
        default,
        skip_serializing_if = "IndexMap::is_empty",
        deserialize_with = "derived"
    )]
    pub derived: Pipelines,
    #[serde(skip)]
    pub templates: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDeclaration {
    pub connection: String,
    pub version: String,
    pub watermark: String,
    pub deleted: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipSourceSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reindex: Vec<Short<ReindexTrigger<Raw>>>,
    #[serde(deserialize_with = "relationship_pipelines")]
    pub pipelines: Pipelines,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub tables: Vec<String>,
    pub order_by: Vec<String>,
    pub query: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lookups: Option<Vec<ExtractLookup<Raw>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reindex: Option<Vec<Short<ReindexTrigger<Raw>>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub global: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<IndexMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binary: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enrich: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edges: Option<Vec<EdgeMapping<Raw>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<Endpoint<Raw>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<Endpoint<Raw>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emits: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform: Option<String>,
}

type Pipelines = IndexMap<String, PipelineSpec>;

struct Section {
    name: &'static str,
    allows: &'static [&'static str],
    requires: &'static [&'static str],
}

const ENTITIES: Section = Section {
    name: "entities",
    allows: &[
        "filter",
        "reindex",
        "global",
        "partition_count",
        "columns",
        "binary",
        "enrich",
        "edges",
    ],
    requires: &[],
};

const RELATIONSHIPS: Section = Section {
    name: "relationships",
    allows: &["fields", "filter", "lookups", "from", "to"],
    requires: &["from", "to"],
};

const DERIVED: Section = Section {
    name: "derived",
    allows: &["reindex", "emits", "transform"],
    requires: &["emits", "transform"],
};

impl PipelineSpec {
    fn check(&self, name: &str, section: &Section) -> Result<(), String> {
        let present = [
            ("fields", self.fields.is_some()),
            ("filter", self.filter.is_some()),
            ("lookups", self.lookups.is_some()),
            ("reindex", self.reindex.is_some()),
            ("global", self.global.is_some()),
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
        let section_name = section.name;
        for (field, set) in present {
            if set && !section.allows.contains(&field) {
                return Err(format!(
                    "pipeline {name} sets {field}, which the {section_name} section does not allow"
                ));
            }
            if !set && section.requires.contains(&field) {
                return Err(format!(
                    "pipeline {name} omits {field}, which the {section_name} section requires"
                ));
            }
        }
        Ok(())
    }
}

fn pipelines<'de, D: Deserializer<'de>>(
    deserializer: D,
    section: &Section,
) -> Result<Pipelines, D::Error> {
    let pipelines = Pipelines::deserialize(deserializer)?;
    for (name, spec) in &pipelines {
        spec.check(name, section).map_err(D::Error::custom)?;
    }
    Ok(pipelines)
}

fn entities<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Pipelines, D::Error> {
    pipelines(deserializer, &ENTITIES)
}

fn relationship_pipelines<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Pipelines, D::Error> {
    pipelines(deserializer, &RELATIONSHIPS)
}

fn derived<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Pipelines, D::Error> {
    pipelines(deserializer, &DERIVED)
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct ExtractLookup<S: Naming = Resolved> {
    pub entity: S::Entity,
    #[serde(rename = "id")]
    pub id_column: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub fields: IndexMap<String, String>,
    #[serde(skip)]
    pub table: S::Loaded<String>,
    #[serde(skip)]
    pub namespaced: S::Loaded<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct EdgeMapping<S: Naming = Resolved> {
    #[serde(rename = "label")]
    pub relationship: S::Relationship,
    #[serde(rename = "from")]
    pub source: Endpoint<S>,
    #[serde(rename = "to")]
    pub target: Endpoint<S>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub array_field: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mutable: bool,
    #[serde(skip)]
    pub variants: S::Loaded<Vec<RelationshipVariantId>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, bound = "")]
pub struct Endpoint<S: Naming = Resolved> {
    pub field: String,
    pub kind: EndpointKind<S>,
    #[serde(default, rename = "enrich")]
    pub properties: S::Enrich,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, bound = "")]
pub enum EndpointKind<S: Naming = Resolved> {
    Entity(S::Entity),
    Column {
        #[serde(rename = "derive")]
        column: String,
        mapping: BTreeMap<String, S::Entity>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PathSpec {
    Column(#[serde(with = "column_path")] String),
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

mod column_path {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[derive(Serialize, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct ColumnPath {
        column: String,
    }

    pub fn serialize<S: Serializer>(column: &str, serializer: S) -> Result<S::Ok, S::Error> {
        let column = column.to_string();
        ColumnPath { column }.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
        ColumnPath::deserialize(deserializer).map(|path| path.column)
    }
}

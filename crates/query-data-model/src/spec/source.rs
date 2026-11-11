use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize};

use super::is_false;

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
    pub reindex: Vec<ReindexSpec>,
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
    pub lookups: Option<Vec<ExtractLookupSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reindex: Option<Vec<ReindexSpec>>,
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
    pub edges: Option<Vec<EdgeSpec>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub from: Option<EndpointSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub to: Option<EndpointSpec>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub array_field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutable: Option<bool>,
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
    allows: &[
        "fields",
        "filter",
        "lookups",
        "from",
        "to",
        "array_field",
        "mutable",
    ],
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
            ("array_field", self.array_field.is_some()),
            ("mutable", self.mutable.is_some()),
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
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExtractLookupSpec {
    pub entity: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub fields: IndexMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeSpec {
    pub label: String,
    pub from: EndpointSpec,
    pub to: EndpointSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub array_field: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mutable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointSpec {
    pub field: String,
    pub kind: EndpointKindSpec,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub properties: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub enrich: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EndpointKindSpec {
    Entity(String),
    Derived {
        derive: String,
        mapping: BTreeMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ReindexSpec {
    Table(String),
    Detail {
        table: String,
        traversal_path: PathSpec,
    },
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

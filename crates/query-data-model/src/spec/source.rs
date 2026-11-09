use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

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
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub entities: IndexMap<String, EntitySourceSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub relationships: IndexMap<String, RelationshipSourceSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub derived: IndexMap<String, DerivedSourceSpec>,
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
pub struct EntitySourceSpec {
    #[serde(default, skip_serializing_if = "is_false")]
    pub global: bool,
    pub tables: Vec<String>,
    pub order_by: Vec<String>,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_count: Option<u32>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub binary: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enrich: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub edges: Vec<EdgeSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reindex: Vec<ReindexSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RelationshipSourceSpec {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reindex: Vec<ReindexSpec>,
    pub pipelines: IndexMap<String, EdgePipelineSpec>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgePipelineSpec {
    pub tables: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<String>,
    pub order_by: Vec<String>,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub lookups: Vec<ExtractLookupSpec>,
    pub from: EndpointSpec,
    pub to: EndpointSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub array_field: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub mutable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DerivedSourceSpec {
    pub emits: Vec<String>,
    pub tables: Vec<String>,
    pub order_by: Vec<String>,
    pub query: String,
    pub transform: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reindex: Vec<ReindexSpec>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
pub enum PathSpec {
    Column {
        column: String,
    },
    Dictionary {
        dictionary: String,
        key_column: String,
    },
}

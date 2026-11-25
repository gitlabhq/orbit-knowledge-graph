use std::collections::BTreeMap;

use indexmap::IndexMap;

pub const DEFAULT_TRANSFORM: &str = "datafusion";

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EtlScope {
    Global,
    Namespaced,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pipeline {
    pub name: String,
    pub scope: EtlScope,
    pub extract: Extract,
    pub transform: Transform,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Extract {
    ClickHouse(ClickHouseExtract),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseExtract {
    pub tables: Vec<String>,
    pub fields: Vec<String>,
    pub order_by: Vec<String>,
    pub version: String,
    pub watermark: String,
    pub deleted: String,
    pub query: ExtractQuery,
    pub lookups: Vec<ClickHouseExtractLookup>,
    pub partition_count: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseExtractLookup {
    pub node_kind: String,
    pub batch_id_column: String,
    pub output_fields: IndexMap<String, String>,
    pub resolved_source: Option<ClickHouseExtractLookupSource>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClickHouseExtractLookupSource {
    pub table: String,
    pub namespaced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractQuery {
    Generated { filter: Option<String> },
    Sql(String),
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReindexSource {
    pub table: String,
    pub target: String,
    pub traversal_path: PathResolution,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum PathResolution {
    Column(String),
    Dictionary {
        dictionary: String,
        key_column: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transform {
    DataFusion { edges: Vec<EdgeMapping> },
    Rust(String),
}

impl Transform {
    pub fn edges(&self) -> &[EdgeMapping] {
        match self {
            Self::DataFusion { edges } => edges,
            Self::Rust(_) => &[],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeMapping {
    pub label: String,
    pub source: NodeRef,
    pub target: NodeRef,
    pub array_field: Option<String>,
    pub mutable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeRef {
    pub field: String,
    pub kind: NodeRefKind,
    pub property_inputs: IndexMap<String, String>,
    pub enrich: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRefKind {
    Literal(String),
    Derived {
        column: String,
        mapping: BTreeMap<String, String>,
    },
}

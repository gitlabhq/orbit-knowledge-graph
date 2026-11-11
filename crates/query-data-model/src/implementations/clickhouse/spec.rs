use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::Deserialize;

use crate::{DenormalizedDirection, TraversalPathKind};

pub type Settings = IndexMap<String, String>;

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseOptions {
    #[serde(default)]
    pub version_column: Option<String>,
    #[serde(default)]
    pub deleted_column: Option<String>,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default)]
    pub settings: Settings,
    #[serde(default)]
    pub edge_tables: IndexMap<String, EdgeTable>,
    #[serde(default)]
    pub denormalized: Vec<DenormalizedSpec>,
    #[serde(default)]
    pub copies: IndexMap<String, CopySpec>,
    #[serde(default)]
    pub joins: IndexMap<String, JoinSpec>,
    #[serde(default)]
    pub tables: IndexMap<String, TableSpec>,
    #[serde(default)]
    pub dictionaries: IndexMap<String, DictionarySpec>,
    #[serde(default)]
    pub views: IndexMap<String, ViewSpec>,
    #[serde(default)]
    pub refreshable_views: IndexMap<String, RefreshableViewSpec>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    #[serde(rename = "type")]
    pub storage_type: String,
    #[serde(default)]
    pub default: Option<String>,
    #[serde(default)]
    pub codec: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum IndexSpec {
    Kind(String),
    Detail(IndexDetail),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexDetail {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default = "unit_granularity")]
    pub granularity: u32,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum IndexSpecs {
    One(IndexSpec),
    Many(Vec<IndexSpec>),
}

impl IndexSpec {
    pub fn kind(&self) -> &str {
        match self {
            Self::Kind(kind) => kind,
            Self::Detail(detail) => &detail.kind,
        }
    }

    pub fn granularity(&self) -> u32 {
        match self {
            Self::Kind(_) => 1,
            Self::Detail(detail) => detail.granularity,
        }
    }

    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Kind(_) => None,
            Self::Detail(detail) => detail.name.as_deref(),
        }
    }
}

impl IndexSpecs {
    pub fn specs(&self) -> &[IndexSpec] {
        match self {
            Self::One(spec) => std::slice::from_ref(spec),
            Self::Many(specs) => specs,
        }
    }
}

fn unit_granularity() -> u32 {
    1
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ProjectionSpec {
    Ordered(OrderedProjection),
    Aggregate(AggregateProjection),
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderedProjection {
    pub order_by: Vec<String>,
    #[serde(default)]
    pub lightweight: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateProjection {
    pub select: Vec<String>,
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSource {
    #[serde(default)]
    pub sort_key: Vec<String>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub indexes: Option<IndexMap<String, IndexSpecs>>,
    #[serde(default)]
    pub projections: IndexMap<String, ProjectionSpec>,
    #[serde(default)]
    pub settings: Settings,
    #[serde(default)]
    pub high_fan_out: Vec<String>,
    #[serde(default)]
    pub epoch_nulls: Vec<String>,
    #[serde(default)]
    pub path_lookups: IndexMap<TraversalPathKind, PathLookupSpec>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTable {
    #[serde(default)]
    pub shared: bool,
    #[serde(default)]
    pub sort_key: Vec<String>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default)]
    pub indexes: IndexMap<String, IndexSpecs>,
    #[serde(default)]
    pub projections: IndexMap<String, ProjectionSpec>,
    #[serde(default)]
    pub settings: Settings,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSpec {
    #[serde(default = "yes")]
    pub versioned: bool,
    pub engine: String,
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default)]
    pub sort_key: Vec<String>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub indexes: IndexMap<String, IndexSpecs>,
    #[serde(default)]
    pub projections: IndexMap<String, ProjectionSpec>,
    #[serde(default)]
    pub settings: Settings,
    #[serde(default)]
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DictionarySpec {
    pub source: String,
    pub key: String,
    pub attributes: IndexMap<String, String>,
    pub layout: String,
    #[serde(default)]
    pub size_in_cells: Option<u64>,
    pub lifetime: LifetimeSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifetimeSpec {
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewSpec {
    #[serde(default = "yes")]
    pub versioned: bool,
    #[serde(default)]
    pub to: Option<String>,
    pub select: String,
    #[serde(default)]
    pub engine: Option<String>,
    #[serde(default)]
    pub order_by: Vec<String>,
    #[serde(default)]
    pub populate: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshableViewSpec {
    #[serde(default = "yes")]
    pub versioned: bool,
    pub append_to: String,
    pub refresh: String,
    pub select: String,
}

fn yes() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenormalizedSpec {
    pub entity: String,
    pub property: String,
    pub relationship: String,
    pub side: DenormalizedDirection,
    pub column: String,
    pub tag: String,
    #[serde(default)]
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CopySpec {
    pub source: String,
    pub sort_key: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSpec {
    pub sort_key: Vec<String>,
    pub sources: Vec<JoinSourceSpec>,
    pub nodes: Vec<JoinNodeSpec>,
    #[serde(default)]
    pub relationships: Vec<JoinRelationshipSpec>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSourceSpec {
    pub table: String,
    #[serde(default)]
    pub path_column: Option<String>,
    pub columns: BTreeMap<String, String>,
    #[serde(default)]
    pub on: Option<JoinOnSpec>,
    #[serde(default)]
    pub filter: IndexMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinOnSpec {
    pub previous: String,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinNodeSpec {
    pub entity: String,
    pub source: usize,
    pub key: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRelationshipSpec {
    pub relationship: String,
    pub source: usize,
    pub target: usize,
    #[serde(default)]
    pub edge: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathLookupSpec {
    pub table: String,
    pub key: String,
}

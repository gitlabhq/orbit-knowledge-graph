use std::collections::BTreeMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::implementations::relational::{EdgeTableSpec, RelationalOptions};

pub type Settings = IndexMap<String, String>;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deleted_column: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub settings: Settings,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub edge_tables: IndexMap<String, EdgeTable>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub denormalized: Vec<DenormalizedSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub copies: IndexMap<String, CopySpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub joins: IndexMap<String, JoinSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub tables: IndexMap<String, TableSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub dictionaries: IndexMap<String, DictionarySpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub views: IndexMap<String, ViewSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub refreshable_views: IndexMap<String, RefreshableViewSpec>,
}

impl ClickHouseOptions {
    pub fn relational(&self) -> RelationalOptions {
        RelationalOptions {
            version_column: self.version_column.clone(),
            deleted_column: self.deleted_column.clone(),
            sort_key: self.sort_key.clone(),
            edge_tables: self
                .edge_tables
                .iter()
                .map(|(name, table)| {
                    let spec = EdgeTableSpec {
                        default: table.default,
                        domains: table.domains.clone(),
                        sort_key: table.sort_key.clone(),
                        columns: IndexMap::new(),
                    };
                    (name.clone(), spec)
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnSpec {
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub storage_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub codec: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexSpec {
    Kind(String),
    Detail(IndexDetail),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IndexDetail {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default = "unit_granularity", skip_serializing_if = "is_unit")]
    pub granularity: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

fn is_unit(granularity: &u32) -> bool {
    *granularity == 1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ProjectionSpec {
    Ordered(OrderedProjection),
    Aggregate(AggregateProjection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrderedProjection {
    pub order_by: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub lightweight: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AggregateProjection {
    pub select: Vec<String>,
    pub group_by: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub indexes: IndexMap<String, IndexSpecs>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub projections: IndexMap<String, ProjectionSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub settings: Settings,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub high_fan_out: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub epoch_nulls: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub path_lookups: IndexMap<String, PathLookupSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeTable {
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub default: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub domains: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub indexes: IndexMap<String, IndexSpecs>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub projections: IndexMap<String, ProjectionSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub settings: Settings,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub writers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableSpec {
    #[serde(default = "yes", skip_serializing_if = "is_yes")]
    pub versioned: bool,
    pub engine: String,
    pub columns: IndexMap<String, ColumnSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub indexes: IndexMap<String, IndexSpecs>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub projections: IndexMap<String, ProjectionSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub settings: Settings,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DictionarySpec {
    pub source: String,
    pub key: String,
    pub attributes: IndexMap<String, String>,
    pub layout: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_in_cells: Option<u64>,
    pub lifetime: LifetimeSpec,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifetimeSpec {
    pub min: u32,
    pub max: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ViewSpec {
    #[serde(default = "yes", skip_serializing_if = "is_yes")]
    pub versioned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<String>,
    pub select: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub populate: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefreshableViewSpec {
    #[serde(default = "yes", skip_serializing_if = "is_yes")]
    pub versioned: bool,
    pub append_to: String,
    pub refresh: String,
    pub select: String,
}

fn yes() -> bool {
    true
}

fn is_yes(value: &bool) -> bool {
    *value
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DenormalizedSpec {
    pub entity: String,
    pub property: String,
    pub relationship: String,
    pub side: String,
    pub column: String,
    pub tag: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub nullable: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CopySpec {
    pub source: String,
    pub sort_key: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSpec {
    pub sort_key: Vec<String>,
    pub sources: Vec<JoinSourceSpec>,
    pub nodes: Vec<JoinNodeSpec>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relationships: Vec<JoinRelationshipSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSourceSpec {
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_column: Option<String>,
    pub columns: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<JoinOnSpec>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub filter: IndexMap<String, String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinOnSpec {
    pub previous: String,
    pub column: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinNodeSpec {
    pub entity: String,
    pub source: usize,
    pub key: String,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinRelationshipSpec {
    pub relationship: String,
    pub source: usize,
    pub target: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edge: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PathLookupSpec {
    pub table: String,
    pub key: String,
}

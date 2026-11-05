use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum QueryType {
    Traversal,
    Aggregation,
    PathFinding,
    Neighbors,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(super) enum Direction {
    #[default]
    Outgoing,
    Incoming,
    Both,
}

impl From<Direction> for crate::Direction {
    fn from(direction: Direction) -> Self {
        match direction {
            Direction::Outgoing => Self::Outgoing,
            Direction::Incoming => Self::Incoming,
            Direction::Both => Self::Both,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct Request {
    pub query_type: QueryType,
    pub nodes: Vec<Node>,
    #[serde(default)]
    pub relationships: Vec<Relationship>,
    #[serde(default)]
    pub aggregations: Vec<Metric>,
    #[serde(default)]
    pub group_by: Vec<Value>,
    pub path: Option<Path>,
    pub neighbors: Option<Neighbors>,
    #[serde(default = "default_limit")]
    pub limit: u64,
    pub order_by: Option<String>,
    pub aggregation_sort: Option<String>,
    #[serde(default)]
    pub options: Options,
}

fn default_limit() -> u64 {
    30
}

#[derive(Debug, Deserialize)]
pub(super) struct Node {
    pub id: String,
    pub entity: String,
    pub columns: Option<Value>,
    #[serde(default)]
    pub filters: Map<String, Value>,
    #[serde(default)]
    pub not: Map<String, Value>,
    #[serde(default)]
    pub node_ids: Vec<Value>,
    pub id_range: Option<IdRange>,
    pub id_property: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct IdRange {
    pub start: Value,
    pub end: Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct Relationship {
    #[serde(rename = "type")]
    pub types: Value,
    pub from: String,
    pub to: String,
    pub hops: Option<[u32; 2]>,
    #[serde(default)]
    pub direction: Direction,
    #[serde(default)]
    pub filters: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Metric {
    pub count: Option<String>,
    pub sum: Option<String>,
    pub avg: Option<String>,
    pub min: Option<String>,
    pub max: Option<String>,
    pub collect: Option<String>,
    #[serde(default)]
    pub distinct: bool,
    #[serde(rename = "as")]
    pub alias: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Path {
    pub from: String,
    pub to: String,
    pub max_depth: u32,
    #[serde(default)]
    pub rel_types: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Neighbors {
    #[serde(default)]
    pub direction: Direction,
    #[serde(default)]
    pub rel_types: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct Options {
    pub dynamic_columns: Option<String>,
    #[serde(default)]
    pub include_debug_sql: bool,
}

use std::collections::BTreeMap;

use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryContract {
    pub limits: Limits,
    pub query_types: QueryTypes,
    pub filter_operators: BTreeMap<String, Vec<ValueType>>,
    pub aggregations: BTreeMap<String, Vec<ValueType>>,
    pub ordering: Vec<ValueType>,
    pub path_types: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Limits {
    pub max_hops: u32,
    pub max_depth: u32,
    pub max_nodes: usize,
    pub max_relationships: usize,
    pub max_node_ids: usize,
    pub max_in_values: usize,
    pub max_node_filters: usize,
    pub max_relationship_filters: usize,
    pub max_columns: usize,
    pub max_relationship_types: usize,
    pub max_aggregations: usize,
    pub max_group_by_keys: usize,
    pub max_filter_entries_per_property: usize,
    pub max_predicates: usize,
    pub max_limit: u32,
    pub max_skip: u32,
    pub max_identifier_length: usize,
    pub max_filter_string_length: usize,
    pub max_predicate_depth: usize,
    pub max_id_range_span: i64,
    pub max_path_anchor_range: i64,
    pub max_path_anchor_rows: i64,
    pub min_search_pattern_length: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryTypes {
    pub traversal: QueryTypeContract,
    pub aggregation: QueryTypeContract,
    pub path_finding: QueryTypeContract,
    pub neighbors: QueryTypeContract,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryTypeContract {
    pub nodes: [usize; 2],
    pub relationships: [usize; 2],
    pub path: Presence,
    pub neighbors: Presence,
    pub aggregations: Presence,
    #[serde(default)]
    pub relationship_directions: Vec<Direction>,
    pub relationship_shape: Option<String>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Presence {
    Required,
    Forbidden,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    String,
    Integer,
    Number,
    Boolean,
    Date,
    #[serde(rename = "datetime")]
    DateTime,
    Enum,
    Uuid,
}

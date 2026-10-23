use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryContract {
    pub limits: Limits,
    pub input_rules: Vec<InputRule>,
    pub filter_operators: BTreeMap<String, Vec<ValueType>>,
    pub aggregations: BTreeMap<String, Vec<ValueType>>,
    pub ordering: Vec<ValueType>,
    pub path_types: Vec<String>,
    #[allow(dead_code)]
    pub schema_limits: BTreeMap<String, Vec<SchemaLimit>>,
    #[allow(dead_code)]
    pub grammar_limits: Vec<GrammarLimit>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputRule {
    pub code: String,
    pub reject: String,
    pub message: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaLimit {
    #[allow(dead_code)]
    pub limit: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub branch: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrammarLimit {
    #[allow(dead_code)]
    pub rule: String,
    #[allow(dead_code)]
    pub limit: String,
    #[serde(default)]
    #[allow(dead_code)]
    pub repeated: Option<String>,
    #[serde(default)]
    #[allow(dead_code)]
    pub offset: u64,
}

#[derive(Deserialize, Serialize)]
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

impl Limits {
    #[allow(dead_code)]
    pub fn get(&self, name: &str) -> Option<u64> {
        match name {
            "max_hops" => Some(self.max_hops.into()),
            "max_depth" => Some(self.max_depth.into()),
            "max_nodes" => Some(self.max_nodes as u64),
            "max_relationships" => Some(self.max_relationships as u64),
            "max_node_ids" => Some(self.max_node_ids as u64),
            "max_in_values" => Some(self.max_in_values as u64),
            "max_node_filters" => Some(self.max_node_filters as u64),
            "max_relationship_filters" => Some(self.max_relationship_filters as u64),
            "max_columns" => Some(self.max_columns as u64),
            "max_relationship_types" => Some(self.max_relationship_types as u64),
            "max_aggregations" => Some(self.max_aggregations as u64),
            "max_group_by_keys" => Some(self.max_group_by_keys as u64),
            "max_filter_entries_per_property" => Some(self.max_filter_entries_per_property as u64),
            "max_predicates" => Some(self.max_predicates as u64),
            "max_limit" => Some(self.max_limit.into()),
            "max_skip" => Some(self.max_skip.into()),
            "max_identifier_length" => Some(self.max_identifier_length as u64),
            "max_filter_string_length" => Some(self.max_filter_string_length as u64),
            "max_predicate_depth" => Some(self.max_predicate_depth as u64),
            "max_id_range_span" => u64::try_from(self.max_id_range_span).ok(),
            "max_path_anchor_range" => u64::try_from(self.max_path_anchor_range).ok(),
            "max_path_anchor_rows" => u64::try_from(self.max_path_anchor_rows).ok(),
            "min_search_pattern_length" => Some(self.min_search_pattern_length as u64),
            _ => None,
        }
    }
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

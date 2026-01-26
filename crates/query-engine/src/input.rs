//! Input types for JSON query deserialization.
//!
//! Security validation (identifiers, SQL injection) is handled by JSON Schema in lib.rs.

use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;

// ─────────────────────────────────────────────────────────────────────────────
// Top-level input
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct Input {
    pub query_type: QueryType,
    pub nodes: Vec<InputNode>,
    #[serde(default)]
    pub relationships: Vec<InputRelationship>,
    #[serde(default)]
    pub aggregations: Vec<InputAggregation>,
    pub path: Option<InputPath>,
    #[serde(default = "default_limit")]
    pub limit: u32,
    pub order_by: Option<InputOrderBy>,
    pub aggregation_sort: Option<InputAggSort>,
}

fn default_limit() -> u32 {
    30
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryType {
    Traversal,
    Pattern,
    Aggregation,
    PathFinding,
}

// ─────────────────────────────────────────────────────────────────────────────
// Nodes
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputNode {
    pub id: String,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default, deserialize_with = "deserialize_filters")]
    pub filters: HashMap<String, InputFilter>,
    #[serde(default)]
    pub node_ids: Vec<i64>,
    pub id_range: Option<InputIdRange>,
    #[serde(default = "default_id_property")]
    pub id_property: String,
}

fn default_id_property() -> String {
    "id".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct InputIdRange {
    pub start: i64,
    pub end: i64,
}

// ─────────────────────────────────────────────────────────────────────────────
// Filters
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InputFilter {
    pub op: Option<FilterOp>,
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
    In,
    Contains,
    StartsWith,
    EndsWith,
    IsNull,
    IsNotNull,
}

fn deserialize_filters<'de, D>(deserializer: D) -> Result<HashMap<String, InputFilter>, D::Error>
where
    D: Deserializer<'de>,
{
    let raw: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
    Ok(raw.into_iter().map(|(k, v)| (k, parse_filter(v))).collect())
}

fn parse_filter(value: Value) -> InputFilter {
    if let Value::Object(ref obj) = value {
        if let Some(op_val) = obj.get("op") {
            if let Ok(op) = serde_json::from_value::<FilterOp>(op_val.clone()) {
                return InputFilter {
                    op: Some(op),
                    value: obj.get("value").cloned(),
                };
            }
        }
    }
    InputFilter {
        op: None,
        value: Some(value),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Relationships
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputRelationship {
    #[serde(rename = "type", deserialize_with = "deserialize_rel_types")]
    pub types: Vec<String>,
    pub from: String,
    pub to: String,
    #[serde(default = "default_hops")]
    pub min_hops: u32,
    #[serde(default = "default_hops")]
    pub max_hops: u32,
    #[serde(default)]
    pub direction: Direction,
    #[serde(default, deserialize_with = "deserialize_filters")]
    pub filters: HashMap<String, InputFilter>,
}

fn default_hops() -> u32 {
    1
}

fn deserialize_rel_types<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::String(s) => Ok(vec![s]),
        Value::Array(arr) => arr
            .into_iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .ok_or_else(|| serde::de::Error::custom("expected string"))
            })
            .collect(),
        _ => Err(serde::de::Error::custom("type must be string or array")),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    #[default]
    Outgoing,
    Incoming,
    Both,
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregations
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputAggregation {
    pub function: AggFunction,
    #[serde(default)]
    pub target: Option<String>,
    #[serde(default)]
    pub group_by: Option<String>,
    #[serde(default)]
    pub property: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

impl AggFunction {
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Count => "COUNT",
            Self::Sum => "SUM",
            Self::Avg => "AVG",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Collect => "groupArray",
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputPath {
    #[serde(rename = "type")]
    pub path_type: PathType,
    pub from: String,
    pub to: String,
    pub max_depth: u32,
    #[serde(default)]
    pub rel_types: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PathType {
    Shortest,
    AllShortest,
    Any,
}

// ─────────────────────────────────────────────────────────────────────────────
// Ordering
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct InputOrderBy {
    pub node: String,
    pub property: String,
    #[serde(default)]
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, Deserialize)]
pub struct InputAggSort {
    pub agg_index: usize,
    #[serde(default)]
    pub direction: OrderDirection,
}

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Parse JSON into Input structure.
#[must_use = "the parsed input should be used"]
pub fn parse_input(json: &str) -> Result<Input, serde_json::Error> {
    serde_json::from_str(json)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_traversal() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "label": "Note", "filters": {"system": false}},
                {"id": "u", "label": "User"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "n"}],
            "limit": 25
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::Traversal);
        assert_eq!(input.nodes.len(), 2);
        assert_eq!(input.relationships.len(), 1);
        assert_eq!(input.limit, 25);
    }

    #[test]
    fn operator_filter() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u", "label": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            }]
        }"#,
        )
        .unwrap();

        let filters = &input.nodes[0].filters;
        assert_eq!(filters.get("created_at").unwrap().op, Some(FilterOp::Gte));
        assert_eq!(filters.get("state").unwrap().op, Some(FilterOp::In));
    }

    #[test]
    fn multiple_rel_types() {
        let input = parse_input(
            r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [{"type": ["BLOCKS", "RELATES_TO"], "from": "a", "to": "b"}]
        }"#,
        )
        .unwrap();

        assert_eq!(input.relationships[0].types, vec!["BLOCKS", "RELATES_TO"]);
    }

    #[test]
    fn aggregation() {
        let input = parse_input(r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n"}, {"id": "u"}],
            "aggregations": [{"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}],
            "aggregation_sort": {"agg_index": 0, "direction": "DESC"}
        }"#).unwrap();

        assert_eq!(input.query_type, QueryType::Aggregation);
        assert_eq!(input.aggregations[0].function, AggFunction::Count);
        assert!(input.aggregation_sort.is_some());
    }

    #[test]
    fn path_finding() {
        let input = parse_input(
            r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "label": "Project", "node_ids": [100]},
                {"id": "end", "label": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        )
        .unwrap();

        assert_eq!(input.query_type, QueryType::PathFinding);
        let path = input.path.unwrap();
        assert_eq!(path.path_type, PathType::Shortest);
        assert_eq!(path.max_depth, 3);
    }

    #[test]
    fn default_values() {
        let input = parse_input(r#"{"query_type": "traversal", "nodes": [{"id": "n"}]}"#).unwrap();
        assert_eq!(input.limit, 30);
        assert!(input.relationships.is_empty());
        assert!(input.aggregations.is_empty());
    }
}

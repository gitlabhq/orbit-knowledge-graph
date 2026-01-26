//! Input types representing the JSON format that the LLM produces.
//!
//! This is NOT the AST - it's a structured representation of the input that
//! gets validated and then lowered to the AST.

use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::collections::HashMap;

use regex::Regex;
use std::sync::LazyLock;

/// Valid SQL identifier: starts with letter/underscore, then alphanumeric/underscore, max 64 chars.
static IDENTIFIER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]{0,63}$").unwrap());

fn validate_identifier(s: &str) -> Result<(), String> {
    if IDENTIFIER_RE.is_match(s) {
        Ok(())
    } else {
        Err(format!("invalid identifier: {s}"))
    }
}

/// Deserialize and validate a node/relationship ID as a safe identifier
fn deserialize_safe_id<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    validate_identifier(&s)
        .map_err(|e| serde::de::Error::custom(format!("invalid id \"{s}\": {e}")))?;
    Ok(s)
}

/// Deserialize and validate an optional node/relationship ID
fn deserialize_optional_safe_id<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    if let Some(ref s) = opt {
        validate_identifier(s)
            .map_err(|e| serde::de::Error::custom(format!("invalid id \"{s}\": {e}")))?;
    }
    Ok(opt)
}

/// Deserialize and validate a property name as a safe identifier
fn deserialize_property_name<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    validate_identifier(&s)
        .map_err(|e| serde::de::Error::custom(format!("invalid property \"{s}\": {e}")))?;
    Ok(s)
}

/// Deserialize and validate an optional property name
fn deserialize_optional_property<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    if let Some(ref s) = opt {
        validate_identifier(s)
            .map_err(|e| serde::de::Error::custom(format!("invalid property \"{s}\": {e}")))?;
    }
    Ok(opt)
}

/// Parsed JSON query from the LLM
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

/// The type of query to execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryType {
    /// Standard graph traversal: walk nodes and relationships
    Traversal,
    /// Pattern matching query (currently an alias for Traversal).
    /// Future: may support more expressive pattern matching syntax.
    Pattern,
    /// Aggregation query with GROUP BY
    Aggregation,
    /// Find shortest paths between nodes using recursive CTE
    PathFinding,
}

/// Node selector in the input
#[derive(Debug, Clone, Deserialize)]
pub struct InputNode {
    #[serde(deserialize_with = "deserialize_safe_id")]
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
    "id".to_string()
}

/// ID range filter
#[derive(Debug, Clone, Deserialize)]
pub struct InputIdRange {
    pub start: i64,
    pub end: i64,
}

/// Property filter - supports both simple equality and operator-based filters
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
    let mut filters = HashMap::with_capacity(raw.len());

    for (key, value) in raw {
        // Validate property name is a safe identifier
        validate_identifier(&key).map_err(|e| {
            serde::de::Error::custom(format!("invalid filter property \"{key}\": {e}"))
        })?;
        let filter = parse_filter(value);
        filters.insert(key, filter);
    }

    Ok(filters)
}

fn parse_filter(value: Value) -> InputFilter {
    // Try operator-based filter first
    if let Value::Object(ref obj) = value {
        if let Some(op_value) = obj.get("op") {
            if let Ok(op) = serde_json::from_value::<FilterOp>(op_value.clone()) {
                return InputFilter {
                    op: Some(op),
                    value: obj.get("value").cloned(),
                };
            }
        }
    }

    // Simple equality value
    InputFilter {
        op: None,
        value: Some(value),
    }
}

/// Relationship selector for graph traversal.
///
/// Defines how to traverse edges between nodes, including direction
/// and type filtering.
#[derive(Debug, Clone, Deserialize)]
pub struct InputRelationship {
    /// Relationship type(s) to match. Use `["*"]` for any type.
    #[serde(rename = "type", deserialize_with = "deserialize_rel_types")]
    pub types: Vec<String>,
    /// Source node ID (must match an InputNode.id)
    #[serde(deserialize_with = "deserialize_safe_id")]
    pub from: String,
    /// Target node ID (must match an InputNode.id)
    #[serde(deserialize_with = "deserialize_safe_id")]
    pub to: String,
    /// Minimum hops (default: 1).
    /// NOTE: Variable-length paths (min_hops != max_hops) are not yet implemented.
    /// Currently only single-hop traversals are supported.
    #[serde(default = "default_hops")]
    pub min_hops: u32,
    /// Maximum hops (default: 1).
    /// NOTE: Variable-length paths are not yet implemented.
    #[serde(default = "default_hops")]
    pub max_hops: u32,
    /// Edge direction: "outgoing" (default), "incoming", or "both"
    #[serde(default = "default_direction")]
    pub direction: Direction,
    /// Property filters to apply to edges
    #[serde(default, deserialize_with = "deserialize_filters")]
    pub filters: HashMap<String, InputFilter>,
}

fn default_hops() -> u32 {
    1
}

fn default_direction() -> Direction {
    Direction::Outgoing
}

fn deserialize_rel_types<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    match value {
        Value::String(s) => Ok(vec![s]),
        Value::Array(arr) => arr
            .into_iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .ok_or_else(|| serde::de::Error::custom("expected string in array"))
            })
            .collect(),
        _ => Err(serde::de::Error::custom("type must be string or array")),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Outgoing,
    Incoming,
    Both,
}

/// Aggregation specification
#[derive(Debug, Clone, Deserialize)]
pub struct InputAggregation {
    pub function: AggFunction,
    #[serde(default, deserialize_with = "deserialize_optional_safe_id")]
    pub target: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_safe_id")]
    pub group_by: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_property")]
    pub property: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_safe_id")]
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
            AggFunction::Count => "COUNT",
            AggFunction::Sum => "SUM",
            AggFunction::Avg => "AVG",
            AggFunction::Min => "MIN",
            AggFunction::Max => "MAX",
            AggFunction::Collect => "groupArray",
        }
    }
}

/// Path finding configuration
#[derive(Debug, Clone, Deserialize)]
pub struct InputPath {
    #[serde(rename = "type")]
    pub path_type: PathType,
    #[serde(deserialize_with = "deserialize_safe_id")]
    pub from: String,
    #[serde(deserialize_with = "deserialize_safe_id")]
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

/// Ordering specification
#[derive(Debug, Clone, Deserialize)]
pub struct InputOrderBy {
    #[serde(deserialize_with = "deserialize_safe_id")]
    pub node: String,
    #[serde(deserialize_with = "deserialize_property_name")]
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

/// Aggregation sorting
#[derive(Debug, Clone, Deserialize)]
pub struct InputAggSort {
    pub agg_index: usize,
    #[serde(default)]
    pub direction: OrderDirection,
}

/// Parse JSON into Input structure.
///
/// This validates identifier safety during deserialization to prevent SQL injection.
#[must_use = "the parsed input should be used"]
pub fn parse_input(json_data: &str) -> Result<Input, serde_json::Error> {
    serde_json::from_str(json_data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_traversal() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "n", "label": "Note", "filters": {"system": false}},
                {"id": "u", "label": "User"}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"}
            ],
            "limit": 25
        }"#;

        let input = parse_input(json).unwrap();
        assert_eq!(input.query_type, QueryType::Traversal);
        assert_eq!(input.nodes.len(), 2);
        assert_eq!(input.relationships.len(), 1);
        assert_eq!(input.limit, 25);
    }

    #[test]
    fn test_parse_operator_filter() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{
                "id": "u",
                "label": "User",
                "filters": {
                    "created_at": {"op": "gte", "value": "2024-01-01"},
                    "state": {"op": "in", "value": ["active", "blocked"]}
                }
            }]
        }"#;

        let input = parse_input(json).unwrap();
        let node = &input.nodes[0];

        let created_at_filter = node.filters.get("created_at").unwrap();
        assert_eq!(created_at_filter.op, Some(FilterOp::Gte));

        let state_filter = node.filters.get("state").unwrap();
        assert_eq!(state_filter.op, Some(FilterOp::In));
    }

    #[test]
    fn test_parse_multiple_rel_types() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [
                {"type": ["BLOCKS", "RELATES_TO"], "from": "a", "to": "b"}
            ]
        }"#;

        let input = parse_input(json).unwrap();
        assert_eq!(input.relationships[0].types, vec!["BLOCKS", "RELATES_TO"]);
    }

    #[test]
    fn test_parse_aggregation() {
        let json = r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "n"}, {"id": "u"}],
            "aggregations": [
                {"function": "count", "target": "n", "group_by": "u", "alias": "note_count"}
            ],
            "aggregation_sort": {"agg_index": 0, "direction": "DESC"}
        }"#;

        let input = parse_input(json).unwrap();
        assert_eq!(input.query_type, QueryType::Aggregation);
        assert_eq!(input.aggregations.len(), 1);
        assert_eq!(input.aggregations[0].function, AggFunction::Count);
        assert!(input.aggregation_sort.is_some());
    }

    #[test]
    fn test_parse_path_finding() {
        let json = r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "label": "Project", "node_ids": [100]},
                {"id": "end", "label": "Project", "node_ids": [200]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#;

        let input = parse_input(json).unwrap();
        assert_eq!(input.query_type, QueryType::PathFinding);
        let path = input.path.unwrap();
        assert_eq!(path.path_type, PathType::Shortest);
        assert_eq!(path.max_depth, 3);
    }

    #[test]
    fn test_default_values() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n"}]
        }"#;

        let input = parse_input(json).unwrap();
        assert_eq!(input.limit, 30); // default limit
        assert!(input.relationships.is_empty());
        assert!(input.aggregations.is_empty());
    }

    #[test]
    fn test_rejects_sql_injection_in_node_id() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "n; DROP TABLE users; --"}]
        }"#;

        let result = parse_input(json);
        assert!(result.is_err(), "should reject SQL injection in node id");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("invalid id"),
            "error should mention invalid id: {err}"
        );
    }

    #[test]
    fn test_rejects_sql_injection_in_relationship() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "a"}, {"id": "b"}],
            "relationships": [
                {"type": "REL", "from": "a' OR '1'='1", "to": "b"}
            ]
        }"#;

        let result = parse_input(json);
        assert!(
            result.is_err(),
            "should reject SQL injection in relationship from"
        );
    }

    #[test]
    fn test_rejects_empty_node_id() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": ""}]
        }"#;

        let result = parse_input(json);
        assert!(result.is_err(), "should reject empty node id");
    }

    #[test]
    fn test_rejects_id_starting_with_number() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [{"id": "123abc"}]
        }"#;

        let result = parse_input(json);
        assert!(result.is_err(), "should reject id starting with number");
    }

    #[test]
    fn test_valid_identifiers_accepted() {
        let json = r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "user_node"},
                {"id": "_private"},
                {"id": "CamelCase"},
                {"id": "node123"}
            ]
        }"#;

        let result = parse_input(json);
        assert!(result.is_ok(), "should accept valid identifiers");
    }
}

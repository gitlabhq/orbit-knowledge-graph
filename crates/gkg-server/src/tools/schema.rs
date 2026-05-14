use std::sync::LazyLock;

use semver::Version;
use serde_json::{Map, Value};
use toon_format::{EncodeOptions, encode};

const BASE_SCHEMA: &str = include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"));
const QUERY_RESPONSE_SCHEMA: &str =
    include_str!(concat!(env!("SCHEMA_DIR"), "/query_response.json"));
const QUERY_DSL_VERSION_TEXT: &str =
    include_str!(concat!(env!("CONFIG_DIR"), "/QUERY_DSL_VERSION"));

pub static QUERY_DSL_VERSION: LazyLock<Version> = LazyLock::new(|| {
    QUERY_DSL_VERSION_TEXT
        .trim()
        .parse()
        .expect("QUERY_DSL_VERSION must be valid semver")
});

const TRIVIAL_DESCRIPTIONS: &[&str] = &[
    "Integer value",
    "String value",
    "Boolean value",
    "List of values",
];

pub fn condensed_query_schema() -> Result<String, String> {
    let schema: Value = serde_json::from_str(BASE_SCHEMA)
        .map_err(|e| format!("failed to parse base schema: {e}"))?;

    let condensed = condense_schema(schema);

    let options = EncodeOptions::default();
    encode(&condensed, &options).map_err(|e| e.to_string())
}

pub fn raw_query_schema() -> &'static str {
    BASE_SCHEMA
}

pub fn query_dsl_version() -> String {
    QUERY_DSL_VERSION.to_string()
}

pub fn query_response_schema() -> &'static str {
    QUERY_RESPONSE_SCHEMA
}

fn condense_schema(mut schema: Value) -> Value {
    condense_value(&mut schema);
    schema
}

fn condense_value(value: &mut Value) {
    match value {
        Value::Object(map) => condense_object(map),
        Value::Array(arr) => {
            for item in arr {
                condense_value(item);
            }
        }
        _ => {}
    }
}

fn condense_object(map: &mut Map<String, Value>) {
    map.remove("default");

    let should_remove = matches!(
        map.get("description"),
        Some(Value::String(desc)) if is_trivial_description(desc)
    );
    if should_remove {
        map.remove("description");
    }

    for value in map.values_mut() {
        condense_value(value);
    }
}

fn is_trivial_description(desc: &str) -> bool {
    TRIVIAL_DESCRIPTIONS.contains(&desc)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn condensed_schema_is_valid_toon() {
        let result = condensed_query_schema();

        assert!(result.is_ok(), "Should produce valid TOON: {:?}", result);

        let toon = result.unwrap();
        assert!(!toon.is_empty(), "TOON output should not be empty");
        assert!(
            !toon.starts_with('{'),
            "Should be TOON format, not raw JSON"
        );
    }

    #[test]
    fn condensed_schema_reasonable_size() {
        let condensed = condensed_query_schema().expect("Should condense");

        assert!(
            condensed.len() < 22000,
            "Condensed schema should be under 22KB, got {} bytes",
            condensed.len()
        );
    }

    #[test]
    fn condensed_schema_preserves_structure() {
        let toon = condensed_query_schema().expect("Should condense");

        assert!(toon.contains("query_type"), "Should contain query_type");
        assert!(toon.contains("traversal"), "Should contain traversal");
        assert!(toon.contains("aggregation"), "Should contain aggregation");
        assert!(
            !toon.contains("search"),
            "Should not contain removed search type"
        );
        assert!(toon.contains("neighbors"), "Should contain neighbors");
        assert!(toon.contains("path_finding"), "Should contain path_finding");

        assert!(toon.contains("$defs"), "Should preserve $defs");
        assert!(toon.contains("allOf"), "Should preserve allOf conditionals");
        assert!(toon.contains("NodeSelector"), "Should contain NodeSelector");
        assert!(toon.contains("Filter"), "Should contain Filter");
    }

    #[test]
    fn condensed_schema_mentions_disconnected_multi_node_aggregation_grouping() {
        let toon = condensed_query_schema().expect("Should condense");

        assert!(
            toon.contains("Required for disconnected multi-node aggregation"),
            "Should tell agents when group_by is required"
        );
    }

    #[test]
    fn condensed_schema_removes_trivial_descriptions() {
        let toon = condensed_query_schema().expect("Should condense");

        assert!(
            !toon.contains("Integer value"),
            "Should remove trivial descriptions"
        );
    }

    #[test]
    fn condensed_schema_keeps_security_notes() {
        let toon = condensed_query_schema().expect("Should condense");

        assert!(
            toon.contains("SECURITY"),
            "Should preserve SECURITY notes in descriptions"
        );
    }

    #[test]
    fn print_condensed_schema() {
        let toon = condensed_query_schema().expect("Should condense");
        eprintln!(
            "\n--- condensed schema ({} bytes) ---\n{toon}\n--- end ---\n",
            toon.len()
        );
    }

    #[test]
    fn condensed_schema_excludes_ontology_specific_data() {
        let toon = condensed_query_schema().expect("Should condense");

        assert!(
            !toon.contains("username"),
            "Should not include entity-specific fields like username"
        );
        assert!(
            !toon.contains("AUTHORED"),
            "Should not include relationship types (use get_graph_entities)"
        );
    }

    #[test]
    fn query_schema_id_major_matches_query_dsl_version() {
        let schema: Value =
            serde_json::from_str(BASE_SCHEMA).expect("query DSL schema must be valid JSON");

        let id = schema
            .get("$id")
            .and_then(Value::as_str)
            .expect("query DSL schema must declare $id");

        let id_major: u64 = id
            .rsplit('/')
            .next()
            .and_then(|segment| segment.strip_prefix('v'))
            .and_then(|major| major.parse().ok())
            .unwrap_or_else(|| panic!("$id '{id}' must end with /vN"));

        assert_eq!(
            id_major, QUERY_DSL_VERSION.major,
            "graph_query.schema.json $id '{id}' does not match QUERY_DSL_VERSION major ({})",
            QUERY_DSL_VERSION.major,
        );
    }

    fn validate_query_schema(value: Value) -> Vec<String> {
        let schema: Value =
            serde_json::from_str(BASE_SCHEMA).expect("query DSL schema must be valid JSON");
        let validator = jsonschema::validator_for(&schema).expect("query DSL schema must compile");

        validator
            .iter_errors(&value)
            .map(|e| e.to_string())
            .collect()
    }

    #[test]
    fn query_schema_rejects_disconnected_multi_node_aggregation_without_group_by() {
        let errors = validate_query_schema(serde_json::json!({
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [9970]},
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
            ],
            "aggregations": [
                {"function": "count", "target": "mr", "alias": "open_mr_count"}
            ],
            "limit": 1
        }));

        assert!(
            errors.iter().any(|error| error.contains("group_by")),
            "expected group_by schema error, got: {errors:?}"
        );
    }

    #[test]
    fn query_schema_accepts_constrained_multi_node_scalar_aggregation() {
        let errors = validate_query_schema(serde_json::json!({
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project", "filters": {"archived": false}},
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "merged"}}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "mr", "to": "p"}
            ],
            "aggregations": [
                {"function": "count", "target": "mr", "alias": "merged_mr_count"}
            ],
            "limit": 1
        }));

        assert!(
            errors.is_empty(),
            "expected schema success, got: {errors:?}"
        );
    }

    #[test]
    fn query_schema_accepts_single_node_scalar_aggregation() {
        let errors = validate_query_schema(serde_json::json!({
            "query_type": "aggregation",
            "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}},
            "aggregations": [
                {"function": "count", "target": "mr", "alias": "open_mr_count"}
            ],
            "limit": 1
        }));

        assert!(
            errors.is_empty(),
            "expected schema success, got: {errors:?}"
        );
    }

    #[test]
    fn query_schema_accepts_multi_node_aggregation_with_group_by() {
        let errors = validate_query_schema(serde_json::json!({
            "query_type": "aggregation",
            "nodes": [
                {"id": "p", "entity": "Project"},
                {"id": "mr", "entity": "MergeRequest", "filters": {"state": "opened"}}
            ],
            "relationships": [
                {"type": "IN_PROJECT", "from": "mr", "to": "p"}
            ],
            "group_by": [{"kind": "node", "node": "p"}],
            "aggregations": [
                {"function": "count", "target": "mr", "alias": "open_mr_count"}
            ],
            "limit": 1
        }));

        assert!(
            errors.is_empty(),
            "expected schema success, got: {errors:?}"
        );
    }
}

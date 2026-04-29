//! Query DSL schema condensation.
//!
//! Loads `graph_query.schema.json` at compile time and strips trivial
//! descriptions and defaults. Returns condensed `serde_json::Value`
//! for callers to encode as they see fit (TOON, JSON, etc).

use serde_json::{Map, Value};

const BASE_SCHEMA: &str = include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"));

const TRIVIAL_DESCRIPTIONS: &[&str] = &[
    "Integer value",
    "String value",
    "Boolean value",
    "List of values",
];

/// Return the condensed query DSL schema as a `serde_json::Value`.
pub fn condensed_query_schema() -> Result<Value, String> {
    let schema: Value = serde_json::from_str(BASE_SCHEMA)
        .map_err(|e| format!("failed to parse base schema: {e}"))?;

    Ok(condense_schema(schema))
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
    fn condensed_schema_is_valid_json() {
        let result = condensed_query_schema();
        assert!(result.is_ok(), "Should produce valid JSON: {:?}", result);
        assert!(result.unwrap().is_object(), "Should be a JSON object");
    }

    #[test]
    fn condensed_schema_reasonable_size() {
        let condensed = condensed_query_schema().expect("Should condense");
        let json = serde_json::to_string(&condensed).unwrap();
        assert!(
            json.len() < 30000,
            "Condensed schema should be under 30KB, got {} bytes",
            json.len()
        );
    }

    #[test]
    fn condensed_schema_preserves_structure() {
        let condensed = condensed_query_schema().expect("Should condense");
        let json = serde_json::to_string(&condensed).unwrap();

        assert!(json.contains("query_type"), "Should contain query_type");
        assert!(json.contains("traversal"), "Should contain traversal");
        assert!(json.contains("aggregation"), "Should contain aggregation");
        assert!(
            !json.contains("search"),
            "Should not contain removed search type"
        );
        assert!(json.contains("neighbors"), "Should contain neighbors");
        assert!(json.contains("path_finding"), "Should contain path_finding");

        assert!(json.contains("$defs"), "Should preserve $defs");
        assert!(json.contains("allOf"), "Should preserve allOf conditionals");
        assert!(json.contains("NodeSelector"), "Should contain NodeSelector");
        assert!(json.contains("Filter"), "Should contain Filter");
    }

    #[test]
    fn condensed_schema_removes_trivial_descriptions() {
        let condensed = condensed_query_schema().expect("Should condense");
        let json = serde_json::to_string(&condensed).unwrap();
        assert!(
            !json.contains("Integer value"),
            "Should remove trivial descriptions"
        );
    }

    #[test]
    fn condensed_schema_keeps_security_notes() {
        let condensed = condensed_query_schema().expect("Should condense");
        let json = serde_json::to_string(&condensed).unwrap();
        assert!(
            json.contains("SECURITY"),
            "Should preserve SECURITY notes in descriptions"
        );
    }
}

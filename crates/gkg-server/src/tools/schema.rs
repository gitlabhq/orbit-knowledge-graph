use serde_json::{Map, Value};
use toon_format::{EncodeOptions, encode};

const BASE_SCHEMA: &str = include_str!(concat!(env!("SCHEMA_DIR"), "/graph_query.schema.json"));

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
            condensed.len() < 20000,
            "Condensed schema should be under 20KB, got {} bytes",
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
}

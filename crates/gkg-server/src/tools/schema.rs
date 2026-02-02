use std::sync::Arc;

use ontology::Ontology;
use serde_json::{Map, Value};
use toon_format::{EncodeOptions, encode};

const BASE_SCHEMA: &str = include_str!("../../../ontology/schema.json");

const TRIVIAL_DESCRIPTIONS: &[&str] = &[
    "Integer value",
    "String value",
    "Boolean value",
    "List of values",
];

pub fn condensed_query_schema(ontology: &Arc<Ontology>) -> Result<String, String> {
    let derived = ontology
        .derive_json_schema(BASE_SCHEMA)
        .map_err(|e| e.to_string())?;

    let condensed = condense_schema(derived);

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

    if let Some(Value::String(desc)) = map.get("description") {
        if is_trivial_description(desc) {
            map.remove("description");
        }
    }

    for value in map.values_mut() {
        condense_value(value);
    }
}

fn is_trivial_description(desc: &str) -> bool {
    TRIVIAL_DESCRIPTIONS.iter().any(|&trivial| desc == trivial)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ontology() -> Arc<Ontology> {
        Arc::new(Ontology::load_embedded().expect("Failed to load ontology"))
    }

    #[test]
    fn condensed_schema_is_valid_toon() {
        let ontology = test_ontology();
        let result = condensed_query_schema(&ontology);

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
        let ontology = test_ontology();
        let condensed = condensed_query_schema(&ontology).expect("Should condense");

        assert!(
            condensed.len() < 35000,
            "Condensed schema should be under 35KB, got {} bytes",
            condensed.len()
        );
    }

    #[test]
    fn condensation_reduces_original_schema() {
        let ontology = test_ontology();

        let derived = ontology
            .derive_json_schema(BASE_SCHEMA)
            .expect("Should derive schema");
        let original_json = serde_json::to_string(&derived).expect("Should serialize");

        let condensed = condense_schema(derived.clone());
        let condensed_json = serde_json::to_string(&condensed).expect("Should serialize");

        assert!(
            condensed_json.len() < original_json.len(),
            "Condensed JSON ({} bytes) should be smaller than original ({} bytes)",
            condensed_json.len(),
            original_json.len()
        );
    }

    #[test]
    fn condensed_schema_preserves_structure() {
        let ontology = test_ontology();
        let toon = condensed_query_schema(&ontology).expect("Should condense");

        assert!(toon.contains("query_type"), "Should contain query_type");
        assert!(toon.contains("traversal"), "Should contain traversal");
        assert!(toon.contains("aggregation"), "Should contain aggregation");
        assert!(toon.contains("search"), "Should contain search");
        assert!(toon.contains("neighbors"), "Should contain neighbors");
        assert!(toon.contains("path_finding"), "Should contain path_finding");

        assert!(toon.contains("$defs"), "Should preserve $defs");
        assert!(toon.contains("allOf"), "Should preserve allOf conditionals");
        assert!(toon.contains("NodeSelector"), "Should contain NodeSelector");
        assert!(toon.contains("Filter"), "Should contain Filter");
    }

    #[test]
    fn condensed_schema_includes_ontology_values() {
        let ontology = test_ontology();
        let toon = condensed_query_schema(&ontology).expect("Should condense");

        assert!(toon.contains("User"), "Should include User entity");
        assert!(toon.contains("Project"), "Should include Project entity");
        assert!(
            toon.contains("MergeRequest"),
            "Should include MergeRequest entity"
        );

        assert!(
            toon.contains("AUTHORED"),
            "Should include AUTHORED relationship"
        );
        assert!(
            toon.contains("CONTAINS"),
            "Should include CONTAINS relationship"
        );
    }

    #[test]
    fn condensed_schema_removes_trivial_descriptions() {
        let ontology = test_ontology();

        let derived = ontology
            .derive_json_schema(BASE_SCHEMA)
            .expect("Should derive schema");
        let original_json = serde_json::to_string(&derived).expect("Should serialize");

        let toon = condensed_query_schema(&ontology).expect("Should condense");

        assert!(
            original_json.contains("Integer value"),
            "Original should have trivial description"
        );
        assert!(
            !toon.contains("Integer value"),
            "Should remove trivial descriptions"
        );
    }

    #[test]
    fn condensed_schema_removes_defaults() {
        let ontology = test_ontology();

        let derived = ontology
            .derive_json_schema(BASE_SCHEMA)
            .expect("Should derive schema");
        let original_json = serde_json::to_string(&derived).expect("Should serialize");

        let toon = condensed_query_schema(&ontology).expect("Should condense");

        let original_default_count = original_json.matches("\"default\"").count();
        let toon_default_count = toon.matches("default").count();

        assert!(
            toon_default_count < original_default_count,
            "Should have fewer 'default' occurrences in condensed output"
        );
    }

    #[test]
    fn condensed_schema_keeps_security_notes() {
        let ontology = test_ontology();
        let toon = condensed_query_schema(&ontology).expect("Should condense");

        assert!(
            toon.contains("SECURITY"),
            "Should preserve SECURITY notes in descriptions"
        );
    }
}

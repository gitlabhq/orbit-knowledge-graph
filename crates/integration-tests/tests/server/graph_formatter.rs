use serde_json::Value;

static RESPONSE_SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
    std::sync::LazyLock::new(|| {
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("SCHEMA_DIR"),
            "/query_response.json"
        )))
        .unwrap();
        jsonschema::validator_for(&schema).unwrap()
    });

fn assert_valid(value: &Value) {
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(value).collect();
    assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
}

#[test]
fn schema_rejects_response_missing_format_version() {
    let value = serde_json::json!({
        "query_type": "traversal",
        "nodes": [],
        "edges": []
    });
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(&value).collect();
    assert!(
        !errors.is_empty(),
        "schema must reject responses missing format_version"
    );
}

#[test]
fn schema_rejects_invalid_format_version_string() {
    let value = serde_json::json!({
        "format_version": "not-a-version",
        "query_type": "traversal",
        "nodes": [],
        "edges": []
    });
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(&value).collect();
    assert!(
        !errors.is_empty(),
        "schema must reject non-semver format_version"
    );
}

#[test]
fn schema_accepts_valid_format_version() {
    let value = serde_json::json!({
        "format_version": "1.0.0",
        "query_type": "traversal",
        "nodes": [],
        "edges": []
    });
    assert_valid(&value);
}

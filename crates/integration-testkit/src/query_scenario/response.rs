use std::sync::{Arc, LazyLock};

use query_engine::compiler::{CompiledQueryContext, Frontend, Input, QueryType};
use query_engine::formatters::{GraphFormatter, GraphResponse, ResultFormatter};
use query_engine::pipeline::PipelineError;
use query_engine::shared::PipelineOutput;
use regex::Regex;
use serde_json::Value;

use super::{QueryExpect, assert_gql_table, assert_response};

static RESPONSE_SCHEMA: LazyLock<jsonschema::Validator> = LazyLock::new(|| {
    let schema: Value = serde_json::from_str(include_str!(concat!(
        env!("SCHEMA_DIR"),
        "/query_response.json"
    )))
    .unwrap();
    jsonschema::validator_for(&schema).unwrap()
});

pub(super) fn validate_response(input: &Input, response: Value, label: &str) -> GraphResponse {
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(&response).collect();
    assert!(
        errors.is_empty(),
        "{label}: schema validation failed: {errors:?}"
    );
    let response: GraphResponse =
        serde_json::from_value(response).expect("response should deserialize");
    assert!(
        !response.format_version.is_empty(),
        "{label}: empty format_version"
    );
    if input.query_type == QueryType::Aggregation {
        assert!(
            response.rows.is_some(),
            "{label}: aggregation response missing rows"
        );
        assert!(
            response.columns.is_some(),
            "{label}: aggregation response missing columns"
        );
        assert!(
            response.group_columns.is_some(),
            "{label}: aggregation response missing group_columns"
        );
    }
    response
}

pub(super) fn assert_repeated_response(
    baseline: &Value,
    response: &Value,
    run: usize,
    label: &str,
) {
    let group_nodes_by_type = |response: &Value| {
        let mut response = response.clone();
        response["nodes"]
            .as_array_mut()
            .expect("response nodes")
            .sort_by(|left, right| left["type"].as_str().cmp(&right["type"].as_str()));
        response
    };
    assert_eq!(
        group_nodes_by_type(baseline),
        group_nodes_by_type(response),
        "{label}: run {run} returned different ordered results"
    );
}

static NEVER_IN_COMPILE_ERRORS: LazyLock<Vec<(&'static str, Regex)>> = LazyLock::new(|| {
    [
        ("a traversal path", r"(^|[^/\w])(\d+/)+($|\W)"),
        ("an email address", r"[\w.+-]+@[\w-]+\.\w+"),
        ("a physical table name", r"\b(v\d+_)?gl_[a-z_]+"),
        ("a private fixture literal", r"private-[a-z-]+-literal"),
        ("a private fixture type", r"[A-Z_]*PRIVATE[A-Z_]*"),
    ]
    .into_iter()
    .map(|(name, pattern)| (name, Regex::new(pattern).unwrap()))
    .collect()
});

pub(crate) fn shown_private_detail(message: &str) -> Option<&'static str> {
    NEVER_IN_COMPILE_ERRORS
        .iter()
        .find(|(_, pattern)| pattern.is_match(message))
        .map(|(name, _)| *name)
}

pub(super) fn assert_no_private_details(message: &str, label: &str) {
    if let Some(detail) = shown_private_detail(message) {
        panic!("{label}: compile error shows {detail}: {message}");
    }
}

pub(super) fn assert_compilation(
    compiled: Result<Arc<CompiledQueryContext>, PipelineError>,
    expect: &QueryExpect,
    (backend, frontend): (&str, &str),
    label: &str,
) -> Option<Arc<CompiledQueryContext>> {
    let expected = expect
        .compile_error
        .as_ref()
        .map(|error| <&str>::from(error.reason_for(backend, frontend)));
    assert!(
        expected.is_some() || expect.compile_error_not_contains.is_empty(),
        "{label}: compile_error_not_contains requires compile_error"
    );
    match (compiled, expected) {
        (Ok(compiled), None) => Some(compiled),
        (Ok(_), Some(reason)) => {
            panic!("{label}: expected a {reason} rejection but the query compiled")
        }
        (Err(error), None) => panic!("{label}: unexpected compile error: {error}"),
        (Err(error), Some(reason)) => {
            let actual = error.failure_reason().as_str();
            assert!(
                actual == reason,
                "{label}: rejected as {actual}, expected {reason}: {error}"
            );
            let message = error.to_string();
            assert_no_private_details(&message, label);
            for pattern in &expect.compile_error_not_contains {
                assert!(
                    !pattern.0.is_match(&message),
                    "{label}: compile error shows /{}/: {message}",
                    pattern.0
                );
            }
            None
        }
    }
}

pub(super) fn assert_output(
    output: &PipelineOutput,
    frontend: Frontend,
    expect: &QueryExpect,
    label: &str,
) -> Value {
    assert_pagination(output, label);
    if frontend == Frontend::Gql {
        assert_gql_table(output, expect, label);
    }
    let response = GraphFormatter.format(output);
    assert_response(&output.compiled.input, response.clone(), expect, label);
    response
}

pub(super) fn assert_pagination(output: &PipelineOutput, label: &str) {
    let pagination = output
        .pagination
        .as_ref()
        .expect("pagination should be present");
    assert_eq!(
        pagination.truncated, pagination.has_more,
        "{label}: inconsistent truncation metadata"
    );
}

pub(super) fn repeat_count(expect: &QueryExpect, label: &str) -> usize {
    if let Some(count) = expect.repeat_count {
        assert!(count >= 2, "{label}: repeat_count must be >= 2");
        count
    } else {
        1
    }
}

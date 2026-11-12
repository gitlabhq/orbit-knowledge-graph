use std::sync::{Arc, LazyLock};

use query_engine::compiler::{CompiledQueryContext, Frontend, Input, QueryType};
use query_engine::formatters::{GraphFormatter, GraphResponse, ResultFormatter};
use query_engine::shared::PipelineOutput;
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

pub(super) fn assert_compilation(
    compiled: Result<Arc<CompiledQueryContext>, impl std::fmt::Display>,
    expect: &QueryExpect,
    label: &str,
) -> Option<Arc<CompiledQueryContext>> {
    match (compiled, expect.rejected) {
        (Ok(compiled), false) => Some(compiled),
        (Ok(_), true) => panic!("{label}: expected the compiler to reject the query"),
        (Err(error), false) => panic!("{label}: unexpected compile error: {error}"),
        (Err(_), true) => None,
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

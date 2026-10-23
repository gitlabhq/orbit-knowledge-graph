use std::sync::{Arc, LazyLock};

use query_engine::compiler::{CompiledQueryContext, Frontend, Input, QueryType};
use query_engine::formatters::{GraphFormatter, GraphResponse, ResultFormatter};
use query_engine::shared::PipelineOutput;
use serde_json::Value;

use super::{QueryExpect, assert_gql_table, assert_response, format::CompileErrorExpect};

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
    frontend: &str,
    label: &str,
) -> Option<Arc<CompiledQueryContext>> {
    let contains = expect.compile_error_contains.for_frontend(frontend);
    let not_contains = expect.compile_error_not_contains.for_frontend(frontend);
    let substring = expect
        .compile_error
        .as_ref()
        .and_then(|error| error.substring_for(frontend));
    let expects_error = !matches!(
        expect.compile_error,
        None | Some(CompileErrorExpect::Flag(false))
    ) || !contains.is_empty();
    assert!(
        expects_error || not_contains.is_empty(),
        "{label}: compile_error_not_contains requires an expected compile error"
    );
    let compiled = match compiled {
        Ok(compiled) => {
            assert!(
                !expects_error,
                "{label}: expected compile error but got success"
            );
            compiled
        }
        Err(error) => {
            assert!(expects_error, "{label}: unexpected compile error: {error}");
            let message = error.to_string();
            for fragment in substring
                .into_iter()
                .chain(contains.iter().map(String::as_str))
            {
                assert!(
                    message.contains(fragment),
                    "{label}: compile error '{message}' does not contain '{fragment}'"
                );
            }
            for fragment in not_contains {
                assert!(
                    !message.contains(fragment),
                    "{label}: compile error must not contain '{fragment}'\nerror: {message}"
                );
            }
            return None;
        }
    };
    if let Some(expected) = expect.hydration {
        assert_eq!(
            compiled.hydration.kind(),
            expected,
            "{label}: unexpected hydration plan\n{:?}",
            compiled.hydration
        );
    }
    let settings: std::collections::BTreeMap<_, _> = compiled
        .base
        .query_config
        .to_clickhouse_settings()
        .expect("query settings render")
        .into_iter()
        .collect();
    for (key, value) in &expect.settings {
        let expected = match value {
            Value::Null => None,
            Value::Bool(flag) => Some(u8::from(*flag).to_string()),
            Value::String(text) => Some(format!("'{text}'")),
            other => Some(other.to_string()),
        };
        assert_eq!(
            settings.get(key),
            expected.as_ref(),
            "{label}: unexpected setting {key}\nsettings: {settings:?}"
        );
    }
    let sql = compiled.base.render();
    for fragment in &expect.sql_contains {
        assert!(
            sql.contains(fragment),
            "{label}: SQL does not contain '{fragment}'\nSQL: {sql}"
        );
    }
    for fragment in &expect.sql_not_contains {
        assert!(
            !sql.contains(fragment),
            "{label}: SQL should not contain '{fragment}'\nSQL: {sql}"
        );
    }
    Some(compiled)
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
    assert_hydration_sql(output, expect, label);
    let response = GraphFormatter.format(output);
    assert_response(&output.compiled.input, response.clone(), expect, label);
    response
}

fn assert_hydration_sql(output: &PipelineOutput, expect: &QueryExpect, label: &str) {
    let sql: Vec<&str> = output
        .execution_log
        .iter()
        .map(|execution| execution.rendered_sql.as_str())
        .collect();
    for fragment in &expect.hydration_sql_contains {
        assert!(
            sql.iter().any(|sql| sql.contains(fragment)),
            "{label}: hydration SQL does not contain '{fragment}'\nSQL: {sql:?}"
        );
    }
    for fragment in &expect.hydration_sql_not_contains {
        assert!(
            !sql.iter().any(|sql| sql.contains(fragment)),
            "{label}: hydration SQL should not contain '{fragment}'\nSQL: {sql:?}"
        );
    }
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

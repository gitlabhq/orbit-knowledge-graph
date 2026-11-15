use std::sync::Arc;

use crate::common::compile_model;
use crate::common::{
    MockRedactionService, TestContext, derive_clickhouse_data_model, load_ontology, run_redaction,
};
use orbit_server::pipeline::HydrationStage;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::SecurityContext;
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::{PipelineOutput, RedactionOutput};
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

pub(super) async fn pipeline_output(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> PipelineOutput {
    let ontology = load_ontology();
    let data_model = derive_clickhouse_data_model(&ontology);
    let client = Arc::new(ctx.create_client());
    let compiled = Arc::new(
        compile_model(
            json,
            query_engine::compiler::Frontend::JsonDsl,
            &data_model,
            &security_ctx,
        )
        .unwrap(),
    );

    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context).unwrap();
    let redacted_count = run_redaction(&mut result, svc);

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);
    server_extensions.insert(data_model);
    let mut pipeline_ctx = QueryPipelineContext {
        frontend: query_engine::compiler::Frontend::JsonDsl,
        query_json: String::new(),
        compiled: Some(Arc::clone(&compiled)),
        ontology: Arc::clone(&ontology),
        security_context: Some(security_ctx),
        server_extensions,
        phases: TypeMap::default(),
    };
    pipeline_ctx.phases.insert(RedactionOutput {
        query_result: result,
        redacted_count,
    });
    let mut obs = NoOpObserver;

    let hydration_output = HydrationStage
        .execute(&mut pipeline_ctx, &mut obs)
        .await
        .expect("pipeline should succeed");

    let mut query_result = hydration_output.query_result;
    let pagination = Some(query_engine::shared::paginate(
        &mut query_result,
        &compiled.input,
    ));

    PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: hydration_output.redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(&compiled),
        query_result,
        result_context: hydration_output.result_context,
        execution_log: vec![],
        pagination,
    }
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

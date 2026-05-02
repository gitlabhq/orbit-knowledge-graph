//! Data correctness integration tests.
//!
//! Seeds known data into ClickHouse, runs the full query pipeline
//! (compile → execute → redact → hydrate → format), and asserts that
//! returned values exactly match expectations. Every response is validated
//! against `query_response.json` and deserialized into typed [`GraphResponse`]
//! structs for type-safe assertions via [`ResponseView`].
//!
//! What these tests verify:
//! - Specific property values match seeded data (e.g. alice.username == "alice")
//! - Exact node counts, edge endpoints, and edge types per query
//! - Result ordering when `order_by` is specified
//! - Aggregation results are numerically correct against known inputs
//! - Path finding returns complete, connected paths with exact hop counts
//! - Redaction removes exactly the unauthorized nodes/edges
//! - Referential integrity: every edge endpoint exists in the nodes array

pub(super) use std::collections::HashSet;
pub(super) use std::sync::Arc;

pub(super) use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, admin_security_context,
    load_ontology, run_redaction, test_security_context,
};
pub(super) use gkg_server::pipeline::HydrationStage;
pub(super) use gkg_server::redaction::QueryResult;
pub(super) use integration_testkit::load_seed;
pub(super) use integration_testkit::visitor::{NodeExt, Requirement, ResponseView};
pub(super) use query_engine::compiler::{SecurityContext, compile};
pub(super) use query_engine::formatters::{GraphFormatter, ResultFormatter};
pub(super) use query_engine::pipeline::{
    NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap,
};
pub(super) use query_engine::shared::RedactionOutput;
pub(super) use serde_json::Value;

pub(super) static RESPONSE_SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
    std::sync::LazyLock::new(|| {
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("GKG_SERVER_SCHEMAS_DIR"),
            "/query_response.json"
        )))
        .unwrap();
        jsonschema::validator_for(&schema).unwrap()
    });

pub(super) fn assert_valid(value: &Value) {
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(value).collect();
    assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
}

pub(super) async fn run_query(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
) -> ResponseView {
    run_query_with_security(ctx, json, svc, test_security_context()).await
}

pub(super) async fn run_query_with_security(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> ResponseView {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());

    // When GKG_TEST_V2=1, inject use_v2 into the query to exercise the
    // skeleton-first compiler. Every data correctness test runs through
    // v2 automatically — same expected results, different compiler.
    let json = if std::env::var("GKG_TEST_V2").as_deref() == Ok("1") {
        let mut v: Value = serde_json::from_str(json).unwrap();
        v["options"]["use_v2"] = Value::Bool(true);
        v.to_string()
    } else {
        json.to_string()
    };

    let compiled = Arc::new(compile(&json, &ontology, &security_ctx).unwrap());

    let batches = ctx.query_parameterized(&compiled.base).await;

    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);
    let redacted_count = run_redaction(&mut result, svc);

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(client);
    let mut pipeline_ctx = QueryPipelineContext {
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
    let pagination = compiled.input.cursor.map(|cursor| {
        let total_rows = query_result.authorized_count();
        let has_more = query_result.apply_cursor(cursor.offset, cursor.page_size);
        query_engine::shared::PaginationMeta {
            has_more,
            total_rows,
        }
    });

    let pipeline_output = query_engine::shared::PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: hydration_output.redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(&compiled),
        query_result,
        result_context: hydration_output.result_context,
        execution_log: vec![],
        pagination,
    };

    let value = GraphFormatter.format(&pipeline_output);
    assert_valid(&value);
    let response: query_engine::formatters::GraphResponse =
        serde_json::from_value(value).expect("response should deserialize to GraphResponse");
    assert!(
        !response.format_version.is_empty(),
        "every response must carry a non-empty format_version"
    );
    ResponseView::for_query(&compiled.input, response)
}

pub(super) fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5, 6, 7]);
    svc.allow("group", &[100, 101, 102, 200, 300, 900]);
    svc.allow("project", &[1000, 1001, 1002, 1003, 1004, 1010, 9000]);
    svc.allow("merge_request", &[2000, 2001, 2002, 2003, 2004, 2005, 9100]);
    svc.allow("note", &[3000, 3001, 3002, 3003]);
    svc.allow("work_item", &[4000, 4001, 4002, 4003, 4010]);
    svc.allow("milestone", &[6000, 6001]);
    svc.allow("label", &[7000, 7001, 7002]);
    svc
}

// Topology is documented in config/seeds/data_correctness.sql.

pub(super) async fn seed(ctx: &TestContext) {
    load_seed(ctx, "data_correctness").await;
    ctx.optimize_all().await;
}

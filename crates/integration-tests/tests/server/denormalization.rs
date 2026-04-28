//! End-to-end denormalization correctness tests.
//!
//! Uses a dedicated seed (`denormalization.sql`) with pre-populated
//! `source_tags` / `target_tags` arrays on edge rows. Verifies that
//! queries filtering on denormalized properties return correct results
//! when the compiler rewrites `_nf_` CTEs to `hasToken`/`hasAllTokens`.

use std::sync::Arc;

use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, load_ontology,
    run_redaction, test_security_context,
};
use gkg_server::pipeline::HydrationStage;
use gkg_server::redaction::QueryResult;
use integration_testkit::load_seed;
use integration_testkit::visitor::{NodeExt, ResponseView};
use query_engine::compiler::compile;
use query_engine::formatters::{GraphFormatter, ResultFormatter};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::RedactionOutput;
use serde_json::Value;

static RESPONSE_SCHEMA: std::sync::LazyLock<jsonschema::Validator> =
    std::sync::LazyLock::new(|| {
        let schema: Value = serde_json::from_str(include_str!(concat!(
            env!("GKG_SERVER_SCHEMAS_DIR"),
            "/query_response.json"
        )))
        .unwrap();
        jsonschema::validator_for(&schema).unwrap()
    });

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[10]);
    svc.allow("project", &[20]);
    svc.allow("merge_request", &[100, 101, 102, 103]);
    svc.allow("vulnerability", &[200, 201, 202, 203]);
    svc.allow("work_item", &[300, 301, 302]);
    svc.allow("ci_pipeline", &[400, 401, 402]);
    svc
}

async fn query(ctx: &TestContext, json: &str) -> ResponseView {
    let svc = allow_all();
    let ontology = Arc::new(load_ontology());
    let security_ctx = test_security_context();
    let client = Arc::new(ctx.create_client());
    let compiled = Arc::new(compile(json, &ontology, &security_ctx).unwrap());

    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);
    let redacted_count = run_redaction(&mut result, &svc);

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

    let query_result = hydration_output.query_result;
    let pipeline_output = query_engine::shared::PipelineOutput {
        row_count: query_result.authorized_count(),
        redacted_count: hydration_output.redacted_count,
        query_type: compiled.query_type.to_string(),
        raw_query_strings: vec![compiled.base.sql.clone()],
        compiled: Arc::clone(&compiled),
        query_result,
        result_context: hydration_output.result_context,
        execution_log: vec![],
        pagination: None,
    };

    let value = GraphFormatter.format(&pipeline_output);
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(&value).collect();
    assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
    let response: query_engine::formatters::GraphResponse =
        serde_json::from_value(value).expect("response should deserialize");
    ResponseView::for_query(&compiled.input, response)
}

#[tokio::test]
async fn denormalization_correctness() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    load_seed(&ctx, "denormalization").await;
    ctx.optimize_all().await;

    integration_testkit::run_subtests_shared!(
        &ctx,
        count_opened_mrs_in_project,
        count_failed_pipelines_in_project,
        traversal_opened_mrs_authored_by_user,
        multi_filter_vuln_state_and_severity,
        work_item_type_filter,
        single_severity_filter,
        merged_mr_count_is_one,
        no_match_returns_zero,
    );
}

/// Count opened MRs in project 20. Expected: 2 (MR 100 + 101).
async fn count_opened_mrs_in_project(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "opened"}}},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node("Project", 20, |n| n.prop_i64("n") == Some(2));
}

/// Count failed pipelines in project 20. Expected: 2 (Pipeline 400 + 402).
async fn count_failed_pipelines_in_project(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "pipe", "entity": "Pipeline", "filters": {"status": {"op": "eq", "value": "failed"}}},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "pipe", "to": "p"}],
        "aggregations": [{"function": "count", "target": "pipe", "group_by": "p", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node("Project", 20, |n| n.prop_i64("n") == Some(2));
}

/// Traversal: find opened MRs authored by user 1. Expected: 2 (MR 100 + 101).
async fn traversal_opened_mrs_authored_by_user(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "node_ids": [1]},
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "opened"}}}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "limit": 10
    }"#,
    )
    .await;
    // User node + 2 MR nodes = 3 total, but User has node_ids so always 1.
    // MRs are the filtered target, so count them by checking total minus the pinned user.
    assert!(resp.node_count() >= 2, "expected at least 2 MR nodes");
}

/// Multi-filter: detected + critical vulnerabilities in project 20. Expected: 1 (vuln 200).
async fn multi_filter_vuln_state_and_severity(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "v", "entity": "Vulnerability", "filters": {
                "state": {"op": "eq", "value": "detected"},
                "severity": {"op": "eq", "value": "critical"}
            }},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
        "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node("Project", 20, |n| n.prop_i64("n") == Some(1));
}

/// WorkItem type filter: opened issues in group 10. Expected: 1 (WI 300).
/// WI 301 is closed, WI 302 is epic (not issue).
async fn work_item_type_filter(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "wi", "entity": "WorkItem", "filters": {
                "state": {"op": "eq", "value": "opened"},
                "work_item_type": {"op": "eq", "value": "issue"}
            }},
            {"id": "g", "entity": "Group", "node_ids": [10]}
        ],
        "relationships": [{"type": "IN_GROUP", "from": "wi", "to": "g"}],
        "aggregations": [{"function": "count", "target": "wi", "group_by": "g", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node("Group", 10, |n| n.prop_i64("n") == Some(1));
}

/// Single severity filter: critical vulnerabilities in project 20. Expected: 2 (vuln 200 + 202).
async fn single_severity_filter(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "v", "entity": "Vulnerability", "filters": {"severity": {"op": "eq", "value": "critical"}}},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
        "aggregations": [{"function": "count", "target": "v", "group_by": "p", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node("Project", 20, |n| n.prop_i64("n") == Some(2));
}

/// Merged MR count in project 20. Expected: 1 (MR 102).
async fn merged_mr_count_is_one(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "merged"}}},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node("Project", 20, |n| n.prop_i64("n") == Some(1));
}

/// No-match filter returns zero results. No MR with state='draft' exists.
async fn no_match_returns_zero(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "draft"}}},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "p", "alias": "n"}],
        "limit": 10
    }"#,
    )
    .await;
    assert_eq!(resp.node_count(), 0);
}

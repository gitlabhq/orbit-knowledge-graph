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
use integration_testkit::visitor::{NodeExt, Requirement, ResponseView};
use query_engine::compiler::compile;
use query_engine::compiler::{SecurityContext, TraversalPath};
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
    query_with_security(ctx, json, test_security_context()).await
}

/// SecurityContext with SecurityManager access (level 25), needed for
/// Vulnerability queries where `required_role: security_manager` blocks
/// the default Reporter-level context.
fn security_manager_context() -> SecurityContext {
    SecurityContext::new_with_roles(1, vec![TraversalPath::new("1/", 25)])
        .expect("valid security context")
}

async fn query_with_security(
    ctx: &TestContext,
    json: &str,
    security_ctx: SecurityContext,
) -> ResponseView {
    let svc = allow_all();
    let ontology = Arc::new(load_ontology());
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

/// Opened MRs in project 20. Expected: MR 100 + 101 (both opened).
/// Traversal so we can verify actual filtered nodes, not just a count.
async fn count_opened_mrs_in_project(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "opened"}}, "columns": ["state"]},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node_count(3); // 2 MRs + 1 Project
    resp.assert_node_ids("Project", &[20]);
    resp.assert_filter("MergeRequest", "state", |n| {
        n.prop_str("state") == Some("opened")
    });
    resp.assert_edge_exists("MergeRequest", 100, "Project", 20, "IN_PROJECT");
}

/// Failed pipelines in project 20. Expected: Pipeline 400 + 402.
async fn count_failed_pipelines_in_project(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "pipe", "entity": "Pipeline", "filters": {"status": {"op": "eq", "value": "failed"}}, "columns": ["status"]},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "pipe", "to": "p"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node_count(3); // 2 Pipelines + 1 Project
    resp.assert_node_ids("Project", &[20]);
    resp.assert_filter("Pipeline", "status", |n| {
        n.prop_str("status") == Some("failed")
    });
    resp.assert_edge_exists("Pipeline", 400, "Project", 20, "IN_PROJECT");
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
    resp.assert_node_count(3);
    resp.assert_node_ids("User", &[1]);
    resp.assert_filter("MergeRequest", "state", |n| {
        n.prop_str("state") == Some("opened")
    });
    resp.assert_edge_exists("User", 1, "MergeRequest", 100, "AUTHORED");
}

/// Multi-filter: detected + critical vulnerabilities in project 20. Expected: vuln 200 only.
async fn multi_filter_vuln_state_and_severity(ctx: &TestContext) {
    let resp = query_with_security(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "v", "entity": "Vulnerability", "filters": {
                "state": {"op": "eq", "value": "detected"},
                "severity": {"op": "eq", "value": "critical"}
            }, "columns": ["state", "severity"]},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
        "limit": 10
    }"#,
        security_manager_context(),
    )
    .await;
    resp.assert_node_count(2); // 1 Vulnerability + 1 Project
    resp.assert_node_ids("Project", &[20]);
    resp.assert_filter("Vulnerability", "state", |n| {
        n.prop_str("state") == Some("detected")
    });
    resp.assert_filter("Vulnerability", "severity", |n| {
        n.prop_str("severity") == Some("critical")
    });
    resp.assert_edge_exists("Vulnerability", 200, "Project", 20, "IN_PROJECT");
}

/// WorkItem type filter: opened issues in group 10. Expected: WI 300 only.
/// WI 301 is closed, WI 302 is epic (not issue).
async fn work_item_type_filter(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "wi", "entity": "WorkItem", "filters": {
                "state": {"op": "eq", "value": "opened"},
                "work_item_type": {"op": "eq", "value": "issue"}
            }, "columns": ["state", "work_item_type"]},
            {"id": "g", "entity": "Group", "node_ids": [10]}
        ],
        "relationships": [{"type": "IN_GROUP", "from": "wi", "to": "g"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node_count(2); // 1 WorkItem + 1 Group
    resp.assert_node_ids("Group", &[10]);
    resp.assert_filter("WorkItem", "state", |n| {
        n.prop_str("state") == Some("opened")
    });
    resp.assert_filter("WorkItem", "work_item_type", |n| {
        n.prop_str("work_item_type") == Some("issue")
    });
    resp.assert_edge_exists("WorkItem", 300, "Group", 10, "IN_GROUP");
}

/// Critical vulnerabilities in project 20. Expected: vuln 200 + 202.
async fn single_severity_filter(ctx: &TestContext) {
    let resp = query_with_security(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "v", "entity": "Vulnerability", "filters": {"severity": {"op": "eq", "value": "critical"}}, "columns": ["severity"]},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "v", "to": "p"}],
        "limit": 10
    }"#,
        security_manager_context(),
    )
    .await;
    resp.assert_node_count(3); // 2 Vulnerabilities + 1 Project
    resp.assert_node_ids("Project", &[20]);
    resp.assert_filter("Vulnerability", "severity", |n| {
        n.prop_str("severity") == Some("critical")
    });
    resp.assert_edge_exists("Vulnerability", 200, "Project", 20, "IN_PROJECT");
}

/// Merged MRs in project 20. Expected: MR 102 only.
async fn merged_mr_count_is_one(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "merged"}}, "columns": ["state"]},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "limit": 10
    }"#,
    )
    .await;
    resp.assert_node_count(2); // 1 MR + 1 Project
    resp.assert_node_ids("Project", &[20]);
    resp.assert_filter("MergeRequest", "state", |n| {
        n.prop_str("state") == Some("merged")
    });
    resp.assert_edge_exists("MergeRequest", 102, "Project", 20, "IN_PROJECT");
}

/// No-match filter returns zero results. No MR with state='draft' exists.
async fn no_match_returns_zero(ctx: &TestContext) {
    let resp = query(
        ctx,
        r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "eq", "value": "draft"}}, "columns": ["state"]},
            {"id": "p", "entity": "Project", "node_ids": [20]}
        ],
        "relationships": [{"type": "IN_PROJECT", "from": "mr", "to": "p"}],
        "limit": 10
    }"#,
    )
    .await;
    // No MR with state='draft' → only the pinned Project node, no MRs.
    resp.assert_node_count(0);
    resp.skip_requirement(Requirement::NodeIds);
    resp.skip_requirement(Requirement::Filter {
        field: "state".into(),
    });
    resp.skip_requirement(Requirement::Relationship {
        edge_type: "IN_PROJECT".into(),
    });
}

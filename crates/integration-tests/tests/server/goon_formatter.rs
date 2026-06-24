//! E2E integration tests for the GoonFormatter wire path.
//!
//! Mirrors the pipeline that `gkg-server::grpc::execute_query` runs when a
//! client requests `ResponseFormat::Llm`: compile → execute → redact →
//! hydrate → `GoonFormatter.format_stamped`. Asserts the formatter is wired
//! into the PipelineOutput surface, returns the expected (string, version,
//! name) triple, and emits valid GOON syntax against real ClickHouse data.
//!
//! These tests guard the dispatch in
//! `crates/gkg-server/src/grpc/service.rs` against regressions where the
//! LLM branch silently falls through to the raw `GraphFormatter`.

use std::sync::Arc;

use crate::common::compile;
use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, load_ontology,
    run_redaction, test_security_context,
};
use gkg_server::pipeline::HydrationStage;
use gkg_server::redaction::QueryResult;
use integration_testkit::{run_subtests_shared, t};
use query_engine::compiler::SecurityContext;
use query_engine::formatters::{
    FormatName, GOON_OUTPUT_FORMAT_VERSION, GoonFormatter, GraphFormatter, ResultFormatter,
};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::{PipelineOutput, RedactionOutput};
use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// Data seeding
// ─────────────────────────────────────────────────────────────────────────────

async fn seed(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob \"the Builder\"', 'active', 'human'),
         (3, 'unicode', 'Iñtërnâtiônàlizætiøn 🎉', 'active', 'human')",
        t("gl_user")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add a feature', 'opened', 'feat-a', 'main', '1/100/2000/'),
         (2001, 2, 'Multi-line\\ntitle\\twith escapes', 'merged', 'fix-b', 'main', '1/100/2001/')",
        t("gl_merge_request")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/100/', 3, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/100/2000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest', [], []),
         ('1/100/2001/', 2, 'User', 'AUTHORED', 2001, 'MergeRequest', [], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline helper — returns PipelineOutput so each subtest can format it
// through both formatters and compare.
// ─────────────────────────────────────────────────────────────────────────────

async fn run_pipeline(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> PipelineOutput {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());
    let compiled = Arc::new(compile(json, &ontology, &security_ctx).unwrap());

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
    let (offset, page_size) = compiled.input.response_window();
    let total_rows = query_result.authorized_count();
    let has_more = query_result.apply_cursor(offset, page_size);
    let pagination = Some(query_engine::shared::PaginationMeta {
        has_more,
        total_rows,
        truncated: has_more,
    });

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

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3]);
    svc.allow("group", &[100]);
    svc.allow("merge_request", &[2000, 2001]);
    svc
}

fn goon_str(value: &Value) -> &str {
    value
        .as_str()
        .expect("GoonFormatter must return Value::String, not a JSON object")
}

// ─────────────────────────────────────────────────────────────────────────────
// Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn format_stamped_returns_goon_name_and_version(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "limit": 10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let (formatted, version, name) = GoonFormatter.format_stamped(&output);
    assert_eq!(name, FormatName::Goon);
    assert_eq!(version, GOON_OUTPUT_FORMAT_VERSION.to_string());
    assert!(
        formatted.is_string(),
        "GoonFormatter must wrap its output in Value::String so the gRPC \
         layer routes it as Content::FormattedText, got: {formatted:?}"
    );
}

async fn traversal_emits_header_nodes_and_edges_sections(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username", "name"]},
                {"id": "g", "entity": "Group", "node_ids": [100]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let value = GoonFormatter.format(&output);
    let s = goon_str(&value);

    assert!(s.starts_with("@header\n"), "must lead with @header: {s}");
    assert!(s.contains("query_type:traversal"));
    assert!(
        s.contains(&format!("goon_version:{}", *GOON_OUTPUT_FORMAT_VERSION)),
        "header must declare the GOON wire version: {s}"
    );
    assert!(
        s.contains("@nodes\n"),
        "section markers always emitted: {s}"
    );
    assert!(s.contains("@edges\n"));
    assert!(s.contains("User("), "User group header missing: {s}");
    assert!(s.contains("Group("), "Group group header missing: {s}");
    assert!(s.contains("MEMBER_OF("), "edge type header missing: {s}");
    assert!(
        s.contains("alice"),
        "literal username should pass bare: {s}"
    );
    assert!(
        s.contains("Iñtërnâtiônàlizætiøn 🎉"),
        "unicode characters in `name` must round-trip: {s}"
    );
}

async fn empty_result_still_emits_section_markers(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 99000, "end": 99999}, "columns": ["username"]},
            "limit": 10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let value = GoonFormatter.format(&output);
    let s = goon_str(&value);

    assert!(s.contains("nodes:0"), "must report zero nodes: {s}");
    assert!(s.contains("edges:0"));
    assert!(
        s.contains("@nodes\n"),
        "@nodes marker is required even when empty so parsers stay uniform: {s}"
    );
    assert!(s.contains("@edges\n"));
}

async fn quoting_handles_strings_with_spaces_and_escapes(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "traversal",
            "node": {"id": "u", "entity": "User", "node_ids": [2], "columns": ["name"]},
            "limit": 1}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let value = GoonFormatter.format(&output);
    let s = goon_str(&value);

    // `Bob "the Builder"` contains a space and a literal quote; both must be
    // quoted and the inner quote must be backslash-escaped exactly once.
    assert!(
        s.contains(r#"name="Bob \"the Builder\"""#),
        "embedded double-quote must be `\\\"` (single escape): {s}"
    );
    assert!(
        !s.contains(r#"\\""#),
        "no double-backslash escape — that would break round-trip: {s}"
    );
}

async fn aggregation_node_grouping_lifts_unique_nodes_and_emits_rows(ctx: &TestContext) {
    // Top-level `group_by` with kind=node — group by entity. Encoder
    // dedups the inlined node, surfaces it in @nodes once, and rows
    // reference it as `g=Group:id`.
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [100], "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "node", "node": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "user_count"}],
            "limit": 10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let value = GoonFormatter.format(&output);
    let s = goon_str(&value);

    assert!(s.contains("query_type:aggregation"));
    assert!(
        s.contains("group_by:g(node:Group)"),
        "node-kind group must declare entity: {s}"
    );
    assert!(s.contains("aggregations:user_count(count:u)"));
    assert!(s.contains("@rows\n"), "must emit @rows section: {s}");
    assert!(s.contains("@nodes\n"));
    assert!(
        s.contains("Group(1):"),
        "lifted Group must appear once in @nodes: {s}"
    );
    assert!(
        s.contains("g=Group:100"),
        "row must reference the lifted node by Entity:id: {s}"
    );
}

async fn aggregation_property_grouping_emits_scalar_rows(ctx: &TestContext) {
    // Top-level `group_by` with kind=property — group by ontology-validated
    // node property (User.state). Pure scalar group values, no node lift.
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [100]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "group_by": [{"kind": "property", "node": "u", "property": "state"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "user_count"}],
            "aggregation_sort": {"column": "user_count", "direction": "DESC"},
            "limit": 10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let value = GoonFormatter.format(&output);
    let s = goon_str(&value);

    assert!(s.contains("query_type:aggregation"));
    assert!(
        s.contains("group_by:state(property)"),
        "property group must declare kind=property: {s}"
    );
    assert!(
        s.contains("aggregations:user_count(count:u)"),
        "metric must surface: {s}"
    );
    assert!(s.contains("@rows\n"));
    assert!(
        s.contains("state=active"),
        "active state bucket must appear bare: {s}"
    );
}

async fn ungrouped_aggregation_emits_single_row_no_group_by_line(ctx: &TestContext) {
    // No top-level group_by — single scalar row.
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "node_ids": [100]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "u", "alias": "total"}],
            "limit": 1}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let value = GoonFormatter.format(&output);
    let s = goon_str(&value);

    assert!(s.contains("rows:1"), "single-row scalar agg: {s}");
    assert!(
        !s.contains("group_by:"),
        "ungrouped aggregation must not emit group_by line: {s}"
    );
    assert!(s.contains("aggregations:total(count:u)"));
    assert!(
        s.contains("\ntotal=3\n"),
        "single-row metric value must inline (3 users in Group:100): {s}"
    );
}

async fn graph_and_goon_formatters_agree_on_node_and_edge_counts(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "g", "entity": "Group", "node_ids": [100]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;

    let raw = GraphFormatter.format(&output);
    let goon = GoonFormatter.format(&output);
    let goon_str = goon_str(&goon);

    let raw_node_count = raw["nodes"].as_array().unwrap().len();
    let raw_edge_count = raw["edges"].as_array().unwrap().len();

    assert!(
        goon_str.contains(&format!("nodes:{raw_node_count}")),
        "GOON header must mirror raw node count ({raw_node_count}): {goon_str}"
    );
    assert!(
        goon_str.contains(&format!("edges:{raw_edge_count}")),
        "GOON header must mirror raw edge count ({raw_edge_count}): {goon_str}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Test runner
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn goon_formatter_e2e() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    run_subtests_shared!(
        &ctx,
        format_stamped_returns_goon_name_and_version,
        traversal_emits_header_nodes_and_edges_sections,
        empty_result_still_emits_section_markers,
        quoting_handles_strings_with_spaces_and_escapes,
        aggregation_node_grouping_lifts_unique_nodes_and_emits_rows,
        aggregation_property_grouping_emits_scalar_rows,
        ungrouped_aggregation_emits_single_row_no_group_by_line,
        graph_and_goon_formatters_agree_on_node_and_edge_counts,
    );
}

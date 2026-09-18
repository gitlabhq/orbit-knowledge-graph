use std::sync::Arc;

use crate::common::compile;
use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, load_ontology,
    run_redaction, test_security_context,
};
use integration_testkit::{run_subtests_shared, t};
use orbit_server::pipeline::HydrationStage;
use orbit_server::redaction::QueryResult;
use query_engine::compiler::SecurityContext;
use query_engine::formatters::{
    FormatName, GraphFormatter, ResultFormatter, TOON_OUTPUT_FORMAT_VERSION, ToonFormatter,
};
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::{PipelineOutput, RedactionOutput};
use serde_json::Value;

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
        "INSERT INTO {} (id, note, noteable_type, noteable_id, traversal_path, internal, confidential) VALUES
         (3000, 'internal note', 'MergeRequest', 2000, '1/100/2000/', true, true),
         (3001, 'public note', 'MergeRequest', 2000, '1/100/2000/', false, NULL)",
        t("gl_note")
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

async fn run_pipeline(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> PipelineOutput {
    let ontology = load_ontology();
    let client = Arc::new(ctx.create_client());
    let compiled = Arc::new(
        compile(
            json,
            query_engine::compiler::Frontend::JsonDsl,
            &ontology,
            &security_ctx,
        )
        .unwrap(),
    );

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

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3]);
    svc.allow("group", &[100]);
    svc.allow("merge_request", &[2000, 2001]);
    svc.allow("note", &[3000, 3001]);
    svc
}

fn assert_wire_envelope(output: &PipelineOutput) -> Value {
    let raw = GraphFormatter.format(output);
    let (text, version, name) = ToonFormatter.format_stamped(output);
    assert_eq!(name, FormatName::Toon);
    assert_eq!(version, TOON_OUTPUT_FORMAT_VERSION.to_string());
    let text = text.as_str().unwrap();
    assert!(text.starts_with(&format!(
        "format_version: {}\nquery_type: {}\nnodes",
        raw["format_version"].as_str().unwrap(),
        raw["query_type"].as_str().unwrap()
    )));
    for field in ["nodes", "edges"] {
        let count = raw[field].as_array().unwrap().len();
        let header = if count == 0 {
            format!("{field}: []")
        } else {
            format!("{field}[{count}]")
        };
        assert!(text.lines().any(|line| line.starts_with(&header)), "{text}");
    }
    assert!(!text.ends_with('\n'));
    assert!(!text.lines().any(|line| line.ends_with(' ')));
    raw
}

async fn traversal_and_pagination_payload(ctx: &TestContext) {
    let query = r#"{"query_type":"traversal","nodes":[{"id":"u","entity":"User","id_range":{"start":1,"end":10000},"columns":["username"]}],"order_by":"u.id","cursor":{"page_size":2}}"#;
    let output = run_pipeline(ctx, query, &allow_all(), test_security_context()).await;
    let raw = assert_wire_envelope(&output);
    assert_eq!(raw["query_type"], "traversal");
    assert_eq!(raw["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(raw["pagination"]["has_more"], true);
    assert_eq!(raw["pagination"]["truncated"], true);
    assert!(raw["pagination"]["next_cursor"].is_string());
}

async fn path_finding_payload(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type":"path_finding","nodes":[{"id":"u","entity":"User","node_ids":[1]},{"id":"g","entity":"Group","node_ids":[100]}],"path":{"type":"shortest","from":"u","to":"g","max_depth":2,"rel_types":["MEMBER_OF"]}}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;
    let raw = assert_wire_envelope(&output);
    assert_eq!(raw["query_type"], "path_finding");
    assert!(!raw["nodes"].as_array().unwrap().is_empty());
    assert!(!raw["edges"].as_array().unwrap().is_empty());
}

async fn neighbors_payload(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type":"neighbors","nodes":[{"id":"g","entity":"Group","node_ids":[100]}],"neighbors":{"direction":"incoming"}}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;
    let raw = assert_wire_envelope(&output);
    assert_eq!(raw["query_type"], "neighbors");
    assert!(!raw["nodes"].as_array().unwrap().is_empty());
}

async fn aggregation_metadata_and_rows_payload(ctx: &TestContext) {
    let output = run_pipeline(
        ctx,
        r#"{"query_type":"aggregation","nodes":[{"id":"g","entity":"Group","node_ids":[100],"columns":["name"]},{"id":"u","entity":"User"}],"relationships":[{"type":"MEMBER_OF","from":"u","to":"g"}],"group_by":["g"],"aggregations":[{"count":"u","as":"user_count"}],"limit":10}"#,
        &allow_all(),
        test_security_context(),
    )
    .await;
    let raw = assert_wire_envelope(&output);
    assert_eq!(raw["query_type"], "aggregation");
    assert_eq!(raw["columns"][0]["name"], "user_count");
    assert_eq!(raw["group_columns"][0]["name"], "g");
    assert!(!raw["rows"].as_array().unwrap().is_empty());
    assert!(raw["pagination"].is_object());
}

#[tokio::test]
async fn toon_formatter_e2e() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    run_subtests_shared!(
        &ctx,
        traversal_and_pagination_payload,
        path_finding_payload,
        neighbors_payload,
        aggregation_metadata_and_rows_payload,
    );
}

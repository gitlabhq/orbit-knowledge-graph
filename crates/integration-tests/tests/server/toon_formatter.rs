use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, test_security_context,
};
use integration_testkit::{run_subtests_shared, t};
use query_engine::formatters::{
    FormatName, GraphFormatter, ResultFormatter, TOON_OUTPUT_FORMAT_VERSION, ToonFormatter,
};
use query_engine::shared::PipelineOutput;
use serde_json::{Value, json};

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

async fn run_pipeline(ctx: &TestContext, json: &str) -> PipelineOutput {
    super::graph_formatter::pipeline_output(ctx, json, &allow_all(), test_security_context()).await
}

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3]);
    svc.allow("group", &[100]);
    svc.allow("merge_request", &[2000, 2001]);
    svc.allow("note", &[3000, 3001]);
    svc
}

fn toon_text(output: &PipelineOutput) -> String {
    match ToonFormatter.format(output) {
        Value::String(text) => text,
        other => panic!("ToonFormatter must return Value::String, got {other:?}"),
    }
}

fn decode(text: &str) -> Value {
    let value: Value = toon_format::decode_strict(text)
        .unwrap_or_else(|error| panic!("invalid TOON ({error}): {text}"));
    let canonical = toon_format::encode_default(&value).unwrap() + "\n";
    assert_eq!(
        text, canonical,
        "output must match the crate's canonical encoding"
    );
    value
}

fn toon(output: &PipelineOutput) -> Value {
    decode(&toon_text(output))
}

fn ids(table: &Value) -> Vec<i64> {
    table
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_i64().unwrap())
        .collect()
}

const MEMBERS: &str = r#"{"query_type": "traversal",
    "nodes": [
        {"id": "u", "entity": "User", "columns": ["username", "name"]},
        {"id": "g", "entity": "Group", "node_ids": [100]}
    ],
    "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
    "order_by": "u.id",
    "limit": 10}"#;

async fn format_stamped_returns_toon_name_and_version(ctx: &TestContext) {
    let output = run_pipeline(ctx, MEMBERS).await;
    let (formatted, version, name) = ToonFormatter.format_stamped(&output);
    assert_eq!(name, FormatName::Toon);
    assert_eq!(version, TOON_OUTPUT_FORMAT_VERSION.to_string());
    assert!(formatted.is_string(), "{formatted:?}");
}

async fn traversal_groups_nodes_by_type_into_tables(ctx: &TestContext) {
    let text = toon_text(&run_pipeline(ctx, MEMBERS).await);
    assert!(text.contains("  User[3]{id,name,username}:\n"), "{text}");
    assert!(text.ends_with("truncated: false\n"), "{text:?}");
    assert!(
        text.contains("edges[3]{from,from_id,type,to,to_id}:\n"),
        "{text}"
    );

    let value = decode(&text);
    assert_eq!(value["query_type"], "traversal");
    assert_eq!(ids(&value["nodes"]["User"]), [1, 2, 3]);
    assert_eq!(ids(&value["nodes"]["Group"]), [100]);
    assert_eq!(value["nodes"]["User"][2]["name"], "Iñtërnâtiônàlizætiøn 🎉");
    for edge in value["edges"].as_array().unwrap() {
        assert_eq!(edge["type"], "MEMBER_OF");
        assert_eq!(edge["to_id"], 100);
    }
}

async fn toon_and_raw_agree_on_node_and_edge_counts(ctx: &TestContext) {
    let output = run_pipeline(ctx, MEMBERS).await;
    let raw = GraphFormatter.format(&output);
    let value = toon(&output);
    let toon_nodes: usize = value["nodes"]
        .as_object()
        .unwrap()
        .values()
        .map(|table| table.as_array().unwrap().len())
        .sum();
    assert_eq!(toon_nodes, raw["nodes"].as_array().unwrap().len());
    assert_eq!(
        value["edges"].as_array().unwrap().len(),
        raw["edges"].as_array().unwrap().len()
    );
}

async fn pagination_cursor_pages_forward(ctx: &TestContext) {
    let json = r#"{"query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]}],
        "order_by": "u.id",
        "cursor": {"page_size": 2}}"#;
    let page1 = toon(&run_pipeline(ctx, json).await);
    assert_eq!(page1["pagination"]["has_more"], true);
    assert_eq!(page1["pagination"]["truncated"], true);
    let after = page1["pagination"]["next_cursor"].as_str().unwrap();

    let mut next: Value = serde_json::from_str(json).unwrap();
    next["cursor"]["after"] = json!(after);
    let page2 = toon(&run_pipeline(ctx, &next.to_string()).await);
    assert_eq!(page2["nodes"]["User"][0]["username"], "unicode");
    assert_eq!(page2["pagination"]["has_more"], false);
}

async fn empty_result_omits_nodes_and_edges(ctx: &TestContext) {
    let output = run_pipeline(ctx, r#"{"query_type": "traversal",
            "nodes": [{"id": "u", "entity": "User", "id_range": {"start": 99000, "end": 99999}, "columns": ["username"]}],
            "limit": 10}"#)
    .await;
    let value = toon(&output);
    assert_eq!(value["query_type"], "traversal");
    assert!(
        value.get("nodes").is_none() && value.get("edges").is_none(),
        "{value}"
    );
}

async fn strings_and_booleans_round_trip(ctx: &TestContext) {
    let users = toon(
        &run_pipeline(
            ctx,
            r#"{"query_type": "traversal",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [2], "columns": ["name"]}],
                "limit": 1}"#,
        )
        .await,
    );
    assert_eq!(users["nodes"]["User"][0]["name"], r#"Bob "the Builder""#);

    let notes = toon(
        &run_pipeline(ctx, r#"{"query_type": "traversal",
                "nodes": [{"id": "n", "entity": "Note", "node_ids": [3000, 3001], "columns": ["note", "internal", "confidential"]}],
                "order_by": "n.id",
                "limit": 10}"#)
        .await,
    );
    let notes = &notes["nodes"]["Note"];
    assert_eq!(ids(notes), [3000, 3001]);
    assert_eq!(notes[0]["internal"], true);
    assert_eq!(notes[0]["confidential"], true);
    assert_eq!(notes[1]["internal"], false);
}

async fn path_edges_carry_path_and_step(ctx: &TestContext) {
    let value = toon(
        &run_pipeline(ctx, r#"{"query_type": "path_finding",
                "nodes": [{"id": "u", "entity": "User", "node_ids": [1]}, {"id": "g", "entity": "Group", "node_ids": [100]}],
                "path": {"type": "shortest", "from": "u", "to": "g", "max_depth": 2, "rel_types": ["MEMBER_OF"]}}"#)
        .await,
    );
    assert_eq!(value["query_type"], "path_finding");
    assert_eq!(
        value["edges"][0],
        json!({"from": "User", "from_id": 1, "type": "MEMBER_OF", "to": "Group", "to_id": 100, "path_id": 0, "step": 0})
    );
}

async fn aggregation_lifts_group_nodes_and_references_them_by_id(ctx: &TestContext) {
    let value = toon(
        &run_pipeline(
            ctx,
            r#"{"query_type": "aggregation",
                "nodes": [
                    {"id": "g", "entity": "Group", "node_ids": [100], "columns": ["name"]},
                    {"id": "u", "entity": "User"}
                ],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "group_by": ["g"],
                "aggregations": [{"count": "u", "as": "user_count"}],
                "limit": 10}"#,
        )
        .await,
    );
    assert_eq!(
        value["nodes"]["Group"],
        json!([{"id": 100, "name": "Public Group"}])
    );
    assert_eq!(value["group_columns"][0]["entity"], "Group");
    assert_eq!(value["rows"], json!([{"g": 100, "user_count": 3}]));
}

async fn aggregation_without_node_groups_emits_scalar_rows(ctx: &TestContext) {
    let by_state = toon(
        &run_pipeline(ctx, r#"{"query_type": "aggregation",
                "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}, {"id": "u", "entity": "User"}],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "group_by": ["u.state"],
                "aggregations": [{"count": "u", "as": "user_count"}],
                "limit": 10}"#)
        .await,
    );
    assert!(by_state.get("nodes").is_none(), "{by_state}");
    assert_eq!(
        by_state["rows"],
        json!([{"u_state": "active", "user_count": 3}])
    );

    let total = toon(
        &run_pipeline(ctx, r#"{"query_type": "aggregation",
                "nodes": [{"id": "g", "entity": "Group", "node_ids": [100]}, {"id": "u", "entity": "User"}],
                "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
                "aggregations": [{"count": "u", "as": "total"}],
                "limit": 1}"#)
        .await,
    );
    assert!(total.get("group_columns").is_none(), "{total}");
    assert_eq!(total["rows"], json!([{"total": 3}]));
}

#[tokio::test]
async fn toon_formatter_e2e() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    run_subtests_shared!(
        &ctx,
        format_stamped_returns_toon_name_and_version,
        traversal_groups_nodes_by_type_into_tables,
        toon_and_raw_agree_on_node_and_edge_counts,
        pagination_cursor_pages_forward,
        empty_result_omits_nodes_and_edges,
        strings_and_booleans_round_trip,
        path_edges_carry_path_and_step,
        aggregation_lifts_group_nodes_and_references_them_by_id,
        aggregation_without_node_groups_emits_scalar_rows,
    );
}

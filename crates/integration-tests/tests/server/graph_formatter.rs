//! E2E integration tests for the GraphFormatter.
//!
//! Tests the full compile → execute → redact → hydrate → format flow against
//! a real ClickHouse instance with seeded data and mock redaction service.
//! Every test validates output against the JSON Schema and checks exact values.

use std::collections::HashSet;
use std::sync::Arc;

use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, admin_security_context,
    load_ontology, run_redaction, test_security_context,
};
use gkg_server::pipeline::HydrationStage;
use gkg_server::redaction::QueryResult;
use integration_testkit::{run_subtests, run_subtests_shared, t};
use query_engine::compiler::{SecurityContext, compile};
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

fn assert_valid(value: &Value) {
    let errors: Vec<_> = RESPONSE_SCHEMA.iter_errors(value).collect();
    assert!(errors.is_empty(), "Schema validation failed: {errors:?}");
}

fn find_node<'a>(nodes: &'a [Value], entity_type: &str, id: i64) -> &'a Value {
    let id_str = id.to_string();
    nodes
        .iter()
        .find(|n| n["type"] == entity_type && n["id"].as_str() == Some(&id_str))
        .unwrap_or_else(|| panic!("node {entity_type}:{id} not found in {nodes:?}"))
}

fn node_ids(nodes: &[Value], entity_type: &str) -> HashSet<i64> {
    nodes
        .iter()
        .filter(|n| n["type"] == entity_type)
        .filter_map(|n| n["id"].as_str().and_then(|s| s.parse().ok()))
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Data seeding
// ─────────────────────────────────────────────────────────────────────────────

// Topology:
//
//   User 1 (alice)   →MEMBER_OF→ Group 100 (Public)     →CONTAINS→ Project 1000
//   User 1 (alice)   →MEMBER_OF→ Group 101 (Private)    →CONTAINS→ Project 1001
//   User 2 (bob)     →MEMBER_OF→ Group 101 (Private)
//   User 3 (charlie) →MEMBER_OF→ Group 102 (Internal)   →CONTAINS→ Project 1002
//   User 4 (diana)   →MEMBER_OF→ Group 102 (Internal)
//   User 5 (unicode) →MEMBER_OF→ Group 100 (Public)
//
//   Group 100 →MEMBER_OF→ Group 200 (Depth2)           -- group hierarchy for multi-hop
//   Group 200 →MEMBER_OF→ Group 300 (Depth3)           -- 3-hop chain: User→100→200→300
//
//   User 1 →AUTHORED→ MR 2000   →HAS_NOTE→ Note 3000, 3002, 3003
//   User 2 →AUTHORED→ MR 2001   →HAS_NOTE→ Note 3001 (giant string)
//   User 3 →AUTHORED→ MR 2002
//
async fn seed(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human'),
         (4, 'diana', 'Diana Dev', 'blocked', 'project_bot'),
         (5, '用户_émoji_🎉', 'Unicode Name ñ', 'active', 'human')",
        t("gl_user")
    ))
    .await;

    // Groups: 100-102 are direct, 200-300 form a depth chain reachable only via multi-hop
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/'),
         (200, 'Depth2 Group', 'public', '1/200/'),
         (300, 'Depth3 Group', 'public', '1/300/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/102/1002/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'merged', 'fix-b', 'main', '1/101/1001/'),
         (2002, 3, 'Update C', 'closed', 'update-c', 'main', '1/102/1002/')",
        t("gl_merge_request")
    ))
    .await;

    let giant = "A".repeat(10_000);
    ctx.execute(&format!(
        "INSERT INTO {} (id, note, noteable_type, noteable_id, traversal_path) VALUES
         (3000, 'Normal note', 'MergeRequest', 2000, '1/100/1000/'),
         (3001, '{giant}', 'MergeRequest', 2001, '1/101/1001/'),
         (3002, 'SQL injection attempt: \\'; DROP TABLE gl_user; --', 'MergeRequest', 2000, '1/100/1000/'),
         (3003, 'Backslash\\\\quote\\\"newline\\n\\ttab', 'MergeRequest', 2000, '1/100/1000/')",
        t("gl_note")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, source_tags, target_tags) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/101/', 1, 'User', 'MEMBER_OF', 101, 'Group', ['state:active', 'user_type:human'], ['visibility_level:private']),
         ('1/101/', 2, 'User', 'MEMBER_OF', 101, 'Group', ['state:active', 'user_type:human'], ['visibility_level:private']),
         ('1/102/', 3, 'User', 'MEMBER_OF', 102, 'Group', ['state:active', 'user_type:human'], ['visibility_level:internal']),
         ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group', ['state:blocked', 'user_type:project_bot'], ['visibility_level:internal']),
         ('1/100/', 5, 'User', 'MEMBER_OF', 100, 'Group', ['state:active', 'user_type:human'], ['visibility_level:public']),
         ('1/200/', 100, 'Group', 'MEMBER_OF', 200, 'Group', ['visibility_level:public'], ['visibility_level:public']),
         ('1/300/', 200, 'Group', 'MEMBER_OF', 300, 'Group', ['visibility_level:public'], ['visibility_level:public']),
         ('1/100/', 100, 'Group', 'CONTAINS', 1000, 'Project', [], []),
         ('1/101/', 101, 'Group', 'CONTAINS', 1001, 'Project', [], []),
         ('1/102/', 102, 'Group', 'CONTAINS', 1002, 'Project', [], []),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest', [], []),
         ('1/101/1001/', 2, 'User', 'AUTHORED', 2001, 'MergeRequest', [], []),
         ('1/102/1002/', 3, 'User', 'AUTHORED', 2002, 'MergeRequest', [], []),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3000, 'Note', [], []),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3002, 'Note', [], []),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3003, 'Note', [], []),
         ('1/101/1001/', 2001, 'MergeRequest', 'HAS_NOTE', 3001, 'Note', [], [])",
        t("gl_edge")
    ))
    .await;

    ctx.optimize_all().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Pipeline helper
// ─────────────────────────────────────────────────────────────────────────────

async fn run_pipeline(ctx: &TestContext, json: &str, svc: &MockRedactionService) -> Value {
    run_pipeline_with_security(ctx, json, svc, test_security_context()).await
}

async fn run_pipeline_with_security(
    ctx: &TestContext,
    json: &str,
    svc: &MockRedactionService,
    security_ctx: SecurityContext,
) -> Value {
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
    value
}

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5]);
    svc.allow("group", &[100, 101, 102, 200, 300]);
    svc.allow("project", &[1000, 1001, 1002]);
    svc.allow("merge_request", &[2000, 2001, 2002]);
    svc.allow("note", &[3000, 3001, 3002, 3003, 9000]);
    svc
}

// ─────────────────────────────────────────────────────────────────────────────
// Search
// ─────────────────────────────────────────────────────────────────────────────

async fn search_exact_properties(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state", "name"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");
    assert!(value["edges"].as_array().unwrap().is_empty());

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 5);
    assert!(nodes.iter().all(|n| n["type"] == "User"));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    assert_eq!(alice["state"].as_str().unwrap(), "active");
    assert_eq!(alice["name"].as_str().unwrap(), "Alice Admin");
    assert!(alice["id"].is_string());
    assert!(alice["username"].is_string());
    assert!(
        alice.get("_gkg_u_id").is_none(),
        "internal columns must not leak"
    );

    let bob = find_node(nodes, "User", 2);
    assert_eq!(bob["username"].as_str().unwrap(), "bob");
    assert_eq!(bob["state"].as_str().unwrap(), "active");
    assert_eq!(bob["name"].as_str().unwrap(), "Bob Builder");

    let charlie = find_node(nodes, "User", 3);
    assert_eq!(charlie["username"].as_str().unwrap(), "charlie");
    assert_eq!(charlie["name"].as_str().unwrap(), "Charlie Private");

    let diana = find_node(nodes, "User", 4);
    assert_eq!(diana["username"].as_str().unwrap(), "diana");
    assert_eq!(diana["state"].as_str().unwrap(), "blocked");
    assert_eq!(diana["name"].as_str().unwrap(), "Diana Dev");

    let unicode = find_node(nodes, "User", 5);
    assert_eq!(unicode["username"].as_str().unwrap(), "用户_émoji_🎉");
}

async fn search_unicode_properties(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "name"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let unicode_user = find_node(nodes, "User", 5);
    assert_eq!(unicode_user["username"].as_str().unwrap(), "用户_émoji_🎉");
    assert_eq!(unicode_user["name"].as_str().unwrap(), "Unicode Name ñ");
}

async fn search_redaction_exact(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3, 5]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3);
    let ids = node_ids(nodes, "User");
    assert_eq!(ids, HashSet::from([1, 3, 5]));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    let charlie = find_node(nodes, "User", 3);
    assert_eq!(charlie["username"].as_str().unwrap(), "charlie");
    let unicode = find_node(nodes, "User", 5);
    assert_eq!(unicode["username"].as_str().unwrap(), "用户_émoji_🎉");

    assert!(!ids.contains(&2), "bob should be redacted");
    assert!(!ids.contains(&4), "diana should be redacted");
}

async fn search_no_authorization_returns_empty(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "limit": 10
        }"#,
        &MockRedactionService::new(),
    )
    .await;

    assert!(value["nodes"].as_array().unwrap().is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal
// ─────────────────────────────────────────────────────────────────────────────

async fn traversal_single_hop_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let user_ids = node_ids(nodes, "User");
    let group_ids = node_ids(nodes, "Group");
    assert_eq!(user_ids, HashSet::from([1, 2, 3, 4, 5]));
    assert_eq!(group_ids, HashSet::from([100, 101, 102]));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    assert!(alice["id"].is_string());
    let bob = find_node(nodes, "User", 2);
    assert_eq!(bob["username"].as_str().unwrap(), "bob");
    let group100 = find_node(nodes, "Group", 100);
    assert_eq!(group100["name"].as_str().unwrap(), "Public Group");
    let group101 = find_node(nodes, "Group", 101);
    assert_eq!(group101["name"].as_str().unwrap(), "Private Group");
    let group102 = find_node(nodes, "Group", 102);
    assert_eq!(group102["name"].as_str().unwrap(), "Internal Group");

    let edges = value["edges"].as_array().unwrap();
    assert!(edges.iter().all(|e| e["type"] == "MEMBER_OF"));

    let e_alice_pub = edges
        .iter()
        .find(|e| e["from_id"] == "1" && e["to_id"] == "100")
        .unwrap();
    assert_eq!(e_alice_pub["from"], "User");
    assert_eq!(e_alice_pub["to"], "Group");
    assert_eq!(e_alice_pub["type"], "MEMBER_OF");

    let e_alice_priv = edges
        .iter()
        .find(|e| e["from_id"] == "1" && e["to_id"] == "101")
        .unwrap();
    assert_eq!(e_alice_priv["from"], "User");
    assert_eq!(e_alice_priv["to"], "Group");

    assert!(
        edges
            .iter()
            .any(|e| e["from_id"] == "2" && e["to_id"] == "101"),
        "bob->group101 edge"
    );
    assert!(
        edges
            .iter()
            .any(|e| e["from_id"] == "5" && e["to_id"] == "100"),
        "unicode->group100 edge"
    );

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(
            edge.get("path_id").is_none(),
            "traversal edges should not have path_id"
        );
        assert!(
            edge.get("step").is_none(),
            "traversal edges should not have step"
        );
        assert!(
            edge.get("depth").is_none(),
            "traversal edges should not have depth"
        );
    }
}

async fn traversal_redaction_removes_unauthorized_paths(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5]);
    svc.allow("group", &[100]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 50
        }"#,
        &svc,
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    assert!(group_ids.contains(&100));
    assert!(!group_ids.contains(&101), "group 101 should be redacted");
    assert!(!group_ids.contains(&102), "group 102 should be redacted");

    let group100 = find_node(nodes, "Group", 100);
    assert_eq!(group100["name"].as_str().unwrap(), "Public Group");

    let user_ids = node_ids(nodes, "User");
    assert!(user_ids.contains(&1), "alice is in group 100");
    assert!(user_ids.contains(&5), "unicode user is in group 100");

    let edges = value["edges"].as_array().unwrap();
    for edge in edges {
        assert_eq!(
            edge["to_id"], "100",
            "only edges to authorized group 100 should remain"
        );
    }
}

async fn traversal_deduplicates_shared_nodes(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let all_keys: Vec<(String, i64)> = nodes
        .iter()
        .map(|n| {
            (
                n["type"].as_str().unwrap().to_string(),
                n["id"].as_str().unwrap().parse::<i64>().unwrap(),
            )
        })
        .collect();
    let unique: HashSet<_> = all_keys.iter().collect();
    assert_eq!(all_keys.len(), unique.len(), "nodes should be deduplicated");
}

async fn traversal_with_filter(ctx: &TestContext) {
    // Only active users → groups
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username", "state"], "filters": {"state": "blocked"}},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let users: Vec<&Value> = nodes.iter().filter(|n| n["type"] == "User").collect();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["id"], "4");
    assert_eq!(users[0]["username"].as_str().unwrap(), "diana");
    assert_eq!(users[0]["state"].as_str().unwrap(), "blocked");
    assert!(users[0]["id"].is_string());

    let group_ids = node_ids(nodes, "Group");
    assert!(group_ids.contains(&102), "diana is in Internal Group");
    let int_group = find_node(nodes, "Group", 102);
    assert_eq!(int_group["name"].as_str().unwrap(), "Internal Group");

    let edges = value["edges"].as_array().unwrap();
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0]["from_id"], "4");
    assert_eq!(edges[0]["to_id"], "102");
    assert_eq!(edges[0]["type"], "MEMBER_OF");
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation
// ─────────────────────────────────────────────────────────────────────────────

async fn aggregation_count_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "g", "group_by": "u", "alias": "group_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "aggregation");
    assert!(value["edges"].as_array().unwrap().is_empty());

    let nodes = value["nodes"].as_array().unwrap();
    assert!(nodes.iter().all(|n| n["type"] == "User"));
    assert!(nodes.iter().all(|n| n.get("group_count").is_some()));
    assert!(nodes.iter().all(|n| n["group_count"].is_i64()));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(
        alice["group_count"].as_i64().unwrap(),
        2,
        "alice is in groups 100 and 101"
    );
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    assert!(alice["id"].is_string());

    let bob = find_node(nodes, "User", 2);
    assert_eq!(
        bob["group_count"].as_i64().unwrap(),
        1,
        "bob is in group 101 only"
    );
    assert_eq!(bob["username"].as_str().unwrap(), "bob");

    let charlie = find_node(nodes, "User", 3);
    assert_eq!(
        charlie["group_count"].as_i64().unwrap(),
        1,
        "charlie is in group 102 only"
    );

    let unicode = find_node(nodes, "User", 5);
    assert_eq!(
        unicode["group_count"].as_i64().unwrap(),
        1,
        "unicode user is in group 100 only"
    );
}

async fn aggregation_redaction(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2]);
    svc.allow("group", &[100, 101, 102]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "g", "group_by": "u", "alias": "group_count"}],
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let ids = node_ids(nodes, "User");
    assert!(ids.contains(&1));
    assert!(ids.contains(&2));
    assert!(!ids.contains(&3), "user 3 should be redacted");
    assert!(!ids.contains(&4), "user 4 should be redacted");
    assert!(!ids.contains(&5), "user 5 should be redacted");

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    assert!(alice["group_count"].is_i64());
    let bob = find_node(nodes, "User", 2);
    assert_eq!(bob["username"].as_str().unwrap(), "bob");
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_exact_path(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "path_finding");

    let nodes = value["nodes"].as_array().unwrap();
    assert!(!nodes.is_empty());
    assert!(node_ids(nodes, "User").contains(&1));
    assert!(node_ids(nodes, "Group").contains(&100));
    assert!(node_ids(nodes, "Project").contains(&1000));

    for node in nodes {
        assert!(node["id"].is_string());
        assert!(node["type"].is_string());
        assert!(
            node.get("_gkg_path").is_none(),
            "internal path column must not leak"
        );
    }

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    for edge in edges {
        assert!(edge["path_id"].is_i64(), "path edges must have path_id");
        assert!(edge["step"].is_i64(), "path edges must have step");
        assert!(edge["from"].is_string());
        assert!(edge["to"].is_string());
        assert!(edge["type"].is_string());
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
    }

    let path_0_edges: Vec<&Value> = edges.iter().filter(|e| e["path_id"] == 0).collect();
    assert_eq!(
        path_0_edges.len(),
        2,
        "path User→Group→Project needs exactly 2 edges"
    );

    let steps: Vec<i64> = path_0_edges
        .iter()
        .map(|e| e["step"].as_i64().unwrap())
        .collect();
    assert_eq!(steps, vec![0, 1], "steps must be sequential starting at 0");

    let hop0 = &path_0_edges[0];
    assert_eq!(hop0["from"], "User");
    assert_eq!(hop0["from_id"], "1");
    assert_eq!(hop0["to"], "Group");
    assert_eq!(hop0["to_id"], "100");
    assert_eq!(hop0["type"], "MEMBER_OF");

    let hop1 = &path_0_edges[1];
    assert_eq!(hop1["from"], "Group");
    assert_eq!(hop1["from_id"], "100");
    assert_eq!(hop1["to"], "Project");
    assert_eq!(hop1["to_id"], "1000");
    assert_eq!(hop1["type"], "CONTAINS");
}

async fn path_finding_redaction_blocks_path(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("project", &[1000]);
    // group 100 NOT authorized → path through it is blocked

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &svc,
    )
    .await;

    assert!(
        value["nodes"].as_array().unwrap().is_empty(),
        "path through unauthorized group should be blocked"
    );
    assert!(value["edges"].as_array().unwrap().is_empty());
}

async fn path_finding_max_depth(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Note", "node_ids": [3000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "path_finding");
    let edges = value["edges"].as_array().unwrap();
    if !edges.is_empty() {
        let max_step = edges
            .iter()
            .map(|e| e["step"].as_i64().unwrap())
            .max()
            .unwrap_or(0);
        assert!(max_step <= 2, "max 3 hops = max step 2 (0-indexed)");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors
// ─────────────────────────────────────────────────────────────────────────────

async fn neighbors_outgoing_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing"}
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "neighbors");

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "User", 1);
    assert!(center["id"].is_string());

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    for edge in edges {
        assert_eq!(edge["from"], "User");
        assert_eq!(edge["from_id"], "1");
        assert!(edge["to"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(edge["type"].is_string());
        assert!(
            edge.get("path_id").is_none(),
            "neighbor edges should not have path_id"
        );
        assert!(
            edge.get("step").is_none(),
            "neighbor edges should not have step"
        );
    }

    let edge_types: HashSet<&str> = edges.iter().filter_map(|e| e["type"].as_str()).collect();
    assert!(edge_types.contains("MEMBER_OF"));
    assert!(edge_types.contains("AUTHORED"));

    let member_targets: HashSet<i64> = edges
        .iter()
        .filter(|e| e["type"] == "MEMBER_OF")
        .filter_map(|e| e["to_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(
        member_targets.contains(&100),
        "alice is MEMBER_OF group 100"
    );
    assert!(
        member_targets.contains(&101),
        "alice is MEMBER_OF group 101"
    );

    let authored_targets: HashSet<i64> = edges
        .iter()
        .filter(|e| e["type"] == "AUTHORED")
        .filter_map(|e| e["to_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(authored_targets.contains(&2000), "alice AUTHORED MR 2000");
}

async fn neighbors_incoming_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [101]},
            "neighbors": {"node": "g", "direction": "incoming"}
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    for edge in edges {
        assert_eq!(edge["to"], "Group");
        assert_eq!(edge["to_id"], "101");
        assert_eq!(edge["type"], "MEMBER_OF");
        assert!(edge["from_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "Group", 101);
    assert!(center["id"].is_string());

    let neighbor_ids = node_ids(nodes, "User");
    assert!(neighbor_ids.contains(&1), "alice is MEMBER_OF group 101");
    assert!(neighbor_ids.contains(&2), "bob is MEMBER_OF group 101");
    assert!(!neighbor_ids.contains(&3), "charlie is NOT in group 101");
    assert!(!neighbor_ids.contains(&4), "diana is NOT in group 101");
    assert!(
        !neighbor_ids.contains(&5),
        "unicode user is NOT in group 101"
    );

    let from_ids: HashSet<i64> = edges
        .iter()
        .filter_map(|e| e["from_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(from_ids.contains(&1), "alice edge from_id");
    assert!(from_ids.contains(&2), "bob edge from_id");
}

async fn neighbors_both_exact(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "Group", 100);
    assert!(center["id"].is_string());

    let user_ids = node_ids(nodes, "User");
    assert!(user_ids.contains(&1), "alice is MEMBER_OF group 100");
    assert!(user_ids.contains(&5), "unicode user is MEMBER_OF group 100");

    let project_ids = node_ids(nodes, "Project");
    assert!(
        project_ids.contains(&1000),
        "group 100 CONTAINS project 1000"
    );

    let edges = value["edges"].as_array().unwrap();
    let edge_types: HashSet<&str> = edges.iter().filter_map(|e| e["type"].as_str()).collect();
    assert!(
        edge_types.contains("MEMBER_OF"),
        "should have MEMBER_OF edges"
    );
    assert!(
        edge_types.contains("CONTAINS"),
        "should have CONTAINS edges"
    );

    // MEMBER_OF edges: User→Group (incoming to center), so from=User, to=Group
    assert!(edges.iter().any(|e| {
        e["from"] == "User"
            && e["from_id"] == "1"
            && e["to"] == "Group"
            && e["to_id"] == "100"
            && e["type"] == "MEMBER_OF"
    }));
    assert!(edges.iter().any(|e| {
        e["from"] == "User"
            && e["from_id"] == "5"
            && e["to"] == "Group"
            && e["to_id"] == "100"
            && e["type"] == "MEMBER_OF"
    }));
    // CONTAINS edge: Group→Project (outgoing from center), so from=Group, to=Project
    assert!(edges.iter().any(|e| {
        e["from"] == "Group"
            && e["from_id"] == "100"
            && e["to"] == "Project"
            && e["to_id"] == "1000"
            && e["type"] == "CONTAINS"
    }));

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }
}

async fn neighbors_both_direction_edges_correct(ctx: &TestContext) {
    // User 1 is MEMBER_OF Group 100 (User→Group edge in gl_edge)
    // Query neighbors of User 1 in both directions
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty(), "should have neighbor edges");

    // User 1 is the source of MEMBER_OF edges (outgoing from center)
    // so from=User, to=Group
    for edge in edges {
        if edge["type"] == "MEMBER_OF" {
            assert_eq!(edge["from"], "User", "MEMBER_OF is outgoing from User");
            assert_eq!(edge["from_id"], "1");
        }
    }
}

async fn neighbors_both_direction_mixed_entity(ctx: &TestContext) {
    // MR 2000 has incoming AUTHORED from User 1 and outgoing HAS_NOTE to Notes 3000, 3002, 3003
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "mr", "entity": "MergeRequest", "node_ids": [2000]},
            "neighbors": {"node": "mr", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty(), "should have neighbor edges");

    // AUTHORED: User→MergeRequest (incoming to center), so from=User, to=MergeRequest
    assert!(
        edges.iter().any(|e| {
            e["from"] == "User"
                && e["from_id"] == "1"
                && e["to"] == "MergeRequest"
                && e["to_id"] == "2000"
                && e["type"] == "AUTHORED"
        }),
        "AUTHORED edge should show User as source"
    );

    // HAS_NOTE: MergeRequest→Note (outgoing from center), so from=MergeRequest, to=Note
    let has_note_edges: Vec<_> = edges.iter().filter(|e| e["type"] == "HAS_NOTE").collect();
    assert!(!has_note_edges.is_empty(), "should have HAS_NOTE edges");
    for edge in &has_note_edges {
        assert_eq!(edge["from"], "MergeRequest", "HAS_NOTE is outgoing from MR");
        assert_eq!(edge["from_id"], "2000");
        assert_eq!(edge["to"], "Note");
    }

    let note_ids: HashSet<i64> = has_note_edges
        .iter()
        .filter_map(|e| e["to_id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert!(note_ids.contains(&3000));
    assert!(note_ids.contains(&3002));
    assert!(note_ids.contains(&3003));
}

async fn neighbors_redaction(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[100]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing"}
        }"#,
        &svc,
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    assert!(group_ids.contains(&100));
    assert!(!group_ids.contains(&101), "group 101 should be redacted");
    assert!(!group_ids.contains(&102), "group 102 should be redacted");

    let center = find_node(nodes, "User", 1);
    assert!(center["id"].is_string());

    let edges = value["edges"].as_array().unwrap();
    for edge in edges {
        if edge["type"] == "MEMBER_OF" {
            let to_id: i64 = edge["to_id"].as_str().unwrap().parse().unwrap();
            assert_eq!(
                to_id, 100,
                "only authorized group 100 should remain in edges"
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Attack vectors & edge cases
// ─────────────────────────────────────────────────────────────────────────────

async fn giant_string_survives_pipeline(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [2]},
                {"id": "end", "entity": "Note", "node_ids": [3001]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    if let Some(text) = nodes
        .iter()
        .find(|n| n["type"] == "Note" && n["id"] == "3001")
        .and_then(|note| note.get("note"))
    {
        assert_eq!(text.as_str().unwrap().len(), 10_000);
    }
}

async fn sql_injection_string_preserved(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "MergeRequest", "node_ids": [2000]},
                {"id": "end", "entity": "Note", "node_ids": [3002]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2}
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    if let Some(text) = nodes
        .iter()
        .find(|n| n["type"] == "Note" && n["id"] == "3002")
        .and_then(|note| note.get("note"))
    {
        let s = text.as_str().unwrap();
        assert!(
            s.contains("DROP TABLE"),
            "injection string should be preserved as data, not executed"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation — all functions
// ─────────────────────────────────────────────────────────────────────────────

async fn aggregation_sum(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "sum", "target": "u", "group_by": "g", "property": "id", "alias": "id_sum"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert!(nodes.iter().all(|n| n["type"] == "Group"));
    assert!(nodes.iter().all(|n| n["id_sum"].is_i64()));

    let g100 = find_node(nodes, "Group", 100);
    assert_eq!(g100["name"].as_str().unwrap(), "Public Group");
    assert_eq!(
        g100["id_sum"].as_i64().unwrap(),
        1 + 5,
        "group 100 has users 1 and 5"
    );

    let g101 = find_node(nodes, "Group", 101);
    assert_eq!(g101["name"].as_str().unwrap(), "Private Group");
    assert_eq!(
        g101["id_sum"].as_i64().unwrap(),
        1 + 2,
        "group 101 has users 1 and 2"
    );

    let g102 = find_node(nodes, "Group", 102);
    assert_eq!(
        g102["id_sum"].as_i64().unwrap(),
        3 + 4,
        "group 102 has users 3 and 4"
    );

    assert!(value["edges"].as_array().unwrap().is_empty());
}

async fn aggregation_avg(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "avg", "target": "u", "group_by": "g", "property": "id", "alias": "avg_id"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert!(nodes.iter().all(|n| n["type"] == "Group"));
    assert!(
        nodes.iter().all(|n| n["avg_id"].is_f64()),
        "AVG must produce floating point values"
    );

    let g101 = find_node(nodes, "Group", 101);
    assert_eq!(g101["name"].as_str().unwrap(), "Private Group");
    assert!(
        (g101["avg_id"].as_f64().unwrap() - 1.5).abs() < 0.01,
        "avg of users 1,2 = 1.5"
    );

    let g102 = find_node(nodes, "Group", 102);
    assert!(
        (g102["avg_id"].as_f64().unwrap() - 3.5).abs() < 0.01,
        "avg of users 3,4 = 3.5"
    );

    assert!(value["edges"].as_array().unwrap().is_empty());
}

async fn aggregation_min_max(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [
                {"function": "min", "target": "u", "group_by": "g", "property": "id", "alias": "min_id"},
                {"function": "max", "target": "u", "group_by": "g", "property": "id", "alias": "max_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    for node in nodes {
        let min_val = node["min_id"].as_i64().unwrap();
        let max_val = node["max_id"].as_i64().unwrap();
        assert!(min_val <= max_val, "min must be <= max");
    }

    // Group 101 has users 1,2 → min=1, max=2
    let priv_group = find_node(nodes, "Group", 101);
    assert_eq!(priv_group["min_id"].as_i64().unwrap(), 1);
    assert_eq!(priv_group["max_id"].as_i64().unwrap(), 2);
}

async fn aggregation_min_string(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [
                {"function": "min", "target": "u", "group_by": "g", "property": "username", "alias": "min_username"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert!(nodes.iter().all(|n| n["min_username"].is_string()));

    let public_group = find_node(nodes, "Group", 100);
    assert_eq!(public_group["min_username"].as_str().unwrap(), "alice");
    let private_group = find_node(nodes, "Group", 101);
    assert_eq!(private_group["min_username"].as_str().unwrap(), "alice");
    let internal_group = find_node(nodes, "Group", 102);
    assert_eq!(internal_group["min_username"].as_str().unwrap(), "charlie");
}

async fn aggregation_multiple_functions(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [
                {"function": "count", "target": "u", "group_by": "g", "alias": "member_count"},
                {"function": "avg", "target": "u", "group_by": "g", "property": "id", "alias": "avg_id"},
                {"function": "min", "target": "u", "group_by": "g", "property": "id", "alias": "min_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert!(nodes.iter().all(|n| n["type"] == "Group"));
    for node in nodes {
        assert!(node["member_count"].is_i64());
        assert!(node["avg_id"].is_f64());
        assert!(node["min_id"].is_i64());
        assert!(node["id"].is_string());
    }

    let g100 = find_node(nodes, "Group", 100);
    assert_eq!(
        g100["member_count"].as_i64().unwrap(),
        2,
        "group 100 has users 1, 5"
    );
    assert_eq!(
        g100["min_id"].as_i64().unwrap(),
        1,
        "min user id in group 100"
    );

    let g101 = find_node(nodes, "Group", 101);
    assert_eq!(
        g101["member_count"].as_i64().unwrap(),
        2,
        "group 101 has users 1, 2"
    );
    assert_eq!(
        g101["min_id"].as_i64().unwrap(),
        1,
        "min user id in group 101"
    );

    assert!(value["edges"].as_array().unwrap().is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Ungrouped aggregation
// ─────────────────────────────────────────────────────────────────────────────

async fn ungrouped_count_emits_aggregates(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [{"function": "count", "target": "g", "alias": "total"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "aggregation");
    assert!(value["edges"].as_array().unwrap().is_empty());
    assert!(
        value["nodes"].as_array().unwrap().is_empty(),
        "ungrouped aggregation should have no nodes"
    );

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "total");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[0]["target"], "g");
    assert_eq!(
        columns[0]["value"].as_i64().unwrap(),
        5,
        "should count all 5 groups (100, 101, 102, 200, 300)"
    );
}

async fn ungrouped_multiple_functions_emits_aggregates(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [
                {"function": "count", "target": "g", "alias": "total"},
                {"function": "min", "target": "g", "property": "id", "alias": "min_id"},
                {"function": "max", "target": "g", "property": "id", "alias": "max_id"}
            ],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert!(value["nodes"].as_array().unwrap().is_empty());

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 3);
    assert_eq!(columns[0]["name"], "total");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[0]["value"].as_i64().unwrap(), 5);
    assert_eq!(columns[1]["name"], "min_id");
    assert_eq!(columns[1]["function"], "min");
    assert_eq!(columns[1]["property"], "id");
    assert_eq!(columns[1]["value"].as_i64().unwrap(), 100);
    assert_eq!(columns[2]["name"], "max_id");
    assert_eq!(columns[2]["function"], "max");
    assert_eq!(columns[2]["property"], "id");
    assert_eq!(columns[2]["value"].as_i64().unwrap(), 300);
}

async fn grouped_aggregation_uses_entity_nodes(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "aggregations": [{"function": "count", "target": "g", "group_by": "u", "alias": "group_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert!(
        !nodes.is_empty(),
        "grouped aggregation should have entity nodes"
    );
    assert!(
        nodes.iter().all(|n| n["type"] == "User"),
        "grouped aggregation should only have entity nodes"
    );

    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "group_count");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[0]["target"], "g");
    assert!(
        columns[0].get("value").is_none() || columns[0]["value"].is_null(),
        "grouped columns should not carry values"
    );
}

async fn ungrouped_count_with_redaction(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("group", &[100, 101]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [{"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}}],
            "aggregations": [{"function": "count", "target": "g", "alias": "total"}],
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    assert!(value["nodes"].as_array().unwrap().is_empty());

    // Count is SQL-level (pre-redaction), so it reflects all 5 groups under the
    // traversal_path allowlist regardless of the MockRedactionService policy.
    let columns = value["columns"].as_array().unwrap();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0]["name"], "total");
    assert_eq!(columns[0]["function"], "count");
    assert_eq!(columns[0]["value"].as_i64().unwrap(), 5);
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal — direction variants
// ─────────────────────────────────────────────────────────────────────────────

async fn traversal_incoming_direction(ctx: &TestContext) {
    // Incoming: Group ← User via MEMBER_OF (reversed: "from": "g", "to": "u", direction: incoming)
    // This finds which users are members of groups, but from the group's perspective
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "u", "entity": "User", "columns": ["username"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "g", "to": "u", "direction": "incoming"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    let user_ids = node_ids(nodes, "User");
    assert!(!group_ids.is_empty());
    assert!(!user_ids.is_empty());
    assert!(group_ids.contains(&100));
    assert!(user_ids.contains(&1));

    let g100 = find_node(nodes, "Group", 100);
    assert_eq!(g100["name"].as_str().unwrap(), "Public Group");
    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(edges.iter().all(|e| e["type"] == "MEMBER_OF"));

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }
}

async fn traversal_both_direction(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "g1", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]},
                {"id": "g2", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "g1", "to": "g2", "direction": "both"}],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(edges.iter().all(|e| e["type"] == "MEMBER_OF"));

    let nodes = value["nodes"].as_array().unwrap();
    assert!(
        node_ids(nodes, "Group").contains(&100),
        "group 100 in hierarchy chain"
    );

    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
    }
}

async fn traversal_shared_target_node(ctx: &TestContext) {
    seed(ctx).await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 1, 'User', 'AUTHORED', 3000, 'Note'),
         ('1/100/1000/', 1000, 'Project', 'CONTAINS', 3000, 'Note')",
        t("gl_edge")
    ))
    .await;

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "n", "entity": "Note", "columns": ["note"]},
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "n"},
                {"type": "CONTAINS", "from": "p", "to": "n"}
            ],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let user_ids = node_ids(nodes, "User");
    let note_ids = node_ids(nodes, "Note");
    let project_ids = node_ids(nodes, "Project");
    assert!(user_ids.contains(&1), "User 1 should be present");
    assert!(note_ids.contains(&3000), "Note 3000 should be present");
    assert!(
        project_ids.contains(&1000),
        "Project 1000 should be present"
    );

    let edges = value["edges"].as_array().unwrap();
    assert!(
        edges.iter().any(|e| e["type"] == "AUTHORED"),
        "AUTHORED edge should exist"
    );
    assert!(
        edges.iter().any(|e| e["type"] == "CONTAINS"),
        "CONTAINS edge should exist"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Column types — Bool, DateTime, Nullable
// ─────────────────────────────────────────────────────────────────────────────

async fn search_boolean_columns(ctx: &TestContext) {
    // Note.confidential is Bool, Note.internal is Bool
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "id_range": {"start": 1, "end": 10000}, "columns": ["note", "confidential", "internal"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 4, "4 notes seeded");
    assert!(nodes.iter().all(|n| n["type"] == "Note"));
    assert!(nodes.iter().all(|n| n["id"].is_string()));

    let n3000 = find_node(nodes, "Note", 3000);
    assert_eq!(n3000["note"].as_str().unwrap(), "Normal note");

    for node in nodes {
        if let Some(conf) = node.get("confidential") {
            assert!(
                conf.is_string() || conf.is_null(),
                "boolean column should be string or null, got: {conf}"
            );
        }
    }
}

async fn search_datetime_columns(ctx: &TestContext) {
    seed(ctx).await;

    // Insert a note with an explicit created_at
    ctx.execute(&format!(
        "INSERT INTO {} (id, note, noteable_type, traversal_path, created_at) VALUES
         (9000, 'timestamped', 'MergeRequest', '1/100/1000/', '2024-06-15 12:30:00')",
        t("gl_note")
    ))
    .await;

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "columns": ["note", "created_at"], "node_ids": [9000]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    let node = &nodes[0];
    assert_eq!(node["type"], "Note");
    assert_eq!(node["id"], "9000");
    assert!(node["id"].is_string());
    assert_eq!(node["note"].as_str().unwrap(), "timestamped");

    let created = node.get("created_at");
    assert!(created.is_some(), "created_at column should be present");
    let ts = created.unwrap();
    assert!(ts.is_string(), "datetime should serialize as string");
    let s = ts.as_str().unwrap();
    assert!(s.contains("2024"), "timestamp should contain year: {s}");
    assert!(
        s.contains("06") || s.contains("15") || s.contains("12:30"),
        "timestamp should contain expected date components: {s}"
    );
}

async fn search_nullable_columns(ctx: &TestContext) {
    // Note 3000 has no created_at → should be null
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "n", "entity": "Note", "columns": ["note", "created_at"], "node_ids": [3000]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    let node = &nodes[0];
    assert_eq!(node["type"], "Note");
    assert_eq!(node["id"], "3000");
    assert!(node["id"].is_string());
    assert_eq!(node["note"].as_str().unwrap(), "Normal note");
    assert!(node["note"].is_string());
    if let Some(ts) = node.get("created_at") {
        assert!(ts.is_null(), "unset datetime should be null, got: {ts}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Wildcard columns
// ─────────────────────────────────────────────────────────────────────────────

async fn search_wildcard_columns(ctx: &TestContext) {
    // Admin context so the wildcard expansion includes admin_only columns
    // (email, first_name, last_name, etc.). The non-admin behavior is covered
    // by the compiler-level RestrictPass tests.
    let value = run_pipeline_with_security(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": "*", "node_ids": [1]},
            "limit": 10
        }"#,
        &allow_all(),
        admin_security_context(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    let alice = &nodes[0];
    assert_eq!(alice["type"], "User");
    assert_eq!(alice["id"], "1");
    assert!(alice["id"].is_string());

    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    assert_eq!(alice["state"].as_str().unwrap(), "active");
    assert_eq!(alice["name"].as_str().unwrap(), "Alice Admin");
    assert!(
        alice.get("email").is_some(),
        "wildcard should include email for admin"
    );

    assert!(
        alice.get("_gkg_u_id").is_none(),
        "internal columns must not leak in wildcard mode"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Path finding — type variations
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_all_shortest(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "all_shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "path_finding");

    let nodes = value["nodes"].as_array().unwrap();
    assert!(!nodes.is_empty());
    assert!(node_ids(nodes, "User").contains(&1));
    assert!(node_ids(nodes, "Project").contains(&1000));

    for node in nodes {
        assert!(node["id"].is_string());
        assert!(node["type"].is_string());
    }

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    for edge in edges {
        assert!(edge["path_id"].is_i64());
        assert!(edge["step"].is_i64());
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
    }
}

async fn path_finding_any(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "any", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "path_finding");

    let nodes = value["nodes"].as_array().unwrap();
    if !nodes.is_empty() {
        assert!(node_ids(nodes, "User").contains(&1));
        assert!(node_ids(nodes, "Project").contains(&1000));

        let edges = value["edges"].as_array().unwrap();
        for edge in edges {
            assert!(edge["path_id"].is_i64());
            assert!(edge["step"].is_i64());
            assert!(edge["from_id"].is_string());
            assert!(edge["to_id"].is_string());
        }
    }
}

async fn path_finding_with_rel_types(ctx: &TestContext) {
    // Only follow MEMBER_OF edges — should find User→Group but not Group→Project (CONTAINS)
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3, "rel_types": ["MEMBER_OF"]}
        }"#,
        &allow_all(),
    )
    .await;

    assert!(
        value["nodes"].as_array().unwrap().is_empty(),
        "path should be unreachable with only MEMBER_OF edges"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors — rel_types filter and dynamic columns
// ─────────────────────────────────────────────────────────────────────────────

async fn neighbors_with_rel_types_filter(ctx: &TestContext) {
    // User 1 has outgoing: MEMBER_OF (to groups) and AUTHORED (to MRs)
    // Filter to only AUTHORED → should only see MergeRequest neighbors
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["AUTHORED"]}
        }"#,
        &allow_all(),
    )
    .await;

    let edges = value["edges"].as_array().unwrap();
    assert!(!edges.is_empty());
    assert!(
        edges.iter().all(|e| e["type"] == "AUTHORED"),
        "only AUTHORED edges should appear"
    );

    for edge in edges {
        assert_eq!(edge["from"], "User");
        assert_eq!(edge["from_id"], "1");
        assert!(edge["to_id"].is_string());
        assert!(edge.get("path_id").is_none());
    }

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "User", 1);
    assert!(center["id"].is_string());

    let neighbor_types: HashSet<&str> = nodes
        .iter()
        .filter(|n| n["type"] != "User")
        .filter_map(|n| n["type"].as_str())
        .collect();
    assert!(neighbor_types.contains("MergeRequest"));
    assert!(
        !neighbor_types.contains("Group"),
        "MEMBER_OF edges should be filtered out"
    );

    let mr_ids = node_ids(nodes, "MergeRequest");
    assert!(mr_ids.contains(&2000), "alice authored MR 2000");
}

async fn neighbors_dynamic_columns_all(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing", "rel_types": ["MEMBER_OF"]},
            "options": {"dynamic_columns": "*"}
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let center = find_node(nodes, "User", 1);
    assert!(center["id"].is_string());

    let groups: Vec<&Value> = nodes.iter().filter(|n| n["type"] == "Group").collect();
    assert!(!groups.is_empty());

    for group in &groups {
        assert!(
            group.get("name").is_some(),
            "group should have name with dynamic_columns:*"
        );
        assert!(group["id"].is_string());
    }

    let group_ids = node_ids(nodes, "Group");
    assert!(group_ids.contains(&100), "alice is MEMBER_OF group 100");
    assert!(group_ids.contains(&101), "alice is MEMBER_OF group 101");

    let g100 = find_node(nodes, "Group", 100);
    assert_eq!(g100["name"].as_str().unwrap(), "Public Group");

    let edges = value["edges"].as_array().unwrap();
    assert!(edges.iter().all(|e| e["type"] == "MEMBER_OF"));
    for edge in edges {
        assert_eq!(edge["from"], "User");
        assert_eq!(edge["from_id"], "1");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Filter operators
// ─────────────────────────────────────────────────────────────────────────────

async fn filter_in_operator(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": {"op": "in", "value": ["alice", "charlie"]}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    let ids = node_ids(nodes, "User");
    assert_eq!(ids, HashSet::from([1, 3]));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
    let charlie = find_node(nodes, "User", 3);
    assert_eq!(charlie["username"].as_str().unwrap(), "charlie");

    assert!(!ids.contains(&2), "bob should not be in result");
}

async fn filter_contains_operator(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": {"op": "contains", "value": "lic"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    let names: Vec<&str> = nodes
        .iter()
        .filter_map(|n| n["username"].as_str())
        .collect();
    assert!(names.contains(&"alice"), "alice contains 'lic'");

    let ids = node_ids(nodes, "User");
    assert_eq!(ids, HashSet::from([1]));
}

async fn filter_starts_with_operator(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "filters": {"username": {"op": "starts_with", "value": "ali"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0]["type"], "User");
    assert_eq!(nodes[0]["id"], "1");
    assert!(nodes[0]["id"].is_string());
    assert_eq!(nodes[0]["username"].as_str().unwrap(), "alice");
}

async fn filter_is_null_operator(ctx: &TestContext) {
    // Users without created_at (all seeded users have no created_at)
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username", "created_at"], "filters": {"created_at": {"op": "is_null"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 5, "all 5 seeded users have no created_at");
    assert!(nodes.iter().all(|n| n["type"] == "User"));

    let ids = node_ids(nodes, "User");
    assert_eq!(ids, HashSet::from([1, 2, 3, 4, 5]));

    for node in nodes {
        assert!(node["id"].is_string());
        assert!(node["username"].is_string());
        if let Some(ts) = node.get("created_at") {
            assert!(ts.is_null(), "created_at should be null for matched rows");
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Search — node_ids filtering and order_by
// ─────────────────────────────────────────────────────────────────────────────

async fn search_node_ids_filtering(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [2, 4]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    let ids = node_ids(nodes, "User");
    assert_eq!(ids, HashSet::from([2, 4]));

    let bob = find_node(nodes, "User", 2);
    assert_eq!(bob["username"].as_str().unwrap(), "bob");
    assert!(bob["id"].is_string());

    let diana = find_node(nodes, "User", 4);
    assert_eq!(diana["username"].as_str().unwrap(), "diana");

    assert!(!ids.contains(&1), "alice not requested");
    assert!(!ids.contains(&3), "charlie not requested");
}

async fn search_with_order_by(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "order_by": {"node": "u", "property": "username", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 5);
    assert!(nodes.iter().all(|n| n["type"] == "User"));

    let ids = node_ids(nodes, "User");
    assert_eq!(ids, HashSet::from([1, 2, 3, 4, 5]));

    let alice = find_node(nodes, "User", 1);
    assert_eq!(alice["username"].as_str().unwrap(), "alice");
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge cases (continued)
// ─────────────────────────────────────────────────────────────────────────────

async fn empty_result_all_fields_present(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "columns": ["username"], "node_ids": [99999]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");
    assert!(value["nodes"].is_array());
    assert!(value["edges"].is_array());
    assert!(value["nodes"].as_array().unwrap().is_empty());
    assert!(value["edges"].as_array().unwrap().is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal — variable-length (multi-hop)
// ─────────────────────────────────────────────────────────────────────────────

async fn traversal_variable_length_reaches_depth_2(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "g",
                "min_hops": 1,
                "max_hops": 2
            }],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    assert!(
        group_ids.contains(&200),
        "depth-2 group (200) must be reachable via User→Group100→Group200"
    );
    assert!(group_ids.contains(&100), "depth-1 groups must still appear");

    let edges = value["edges"].as_array().unwrap();
    assert!(
        !edges.is_empty(),
        "multi-hop traversal should produce edges"
    );
    for edge in edges {
        assert!(edge["from_id"].is_string());
        assert!(edge["to_id"].is_string());
    }
}

async fn traversal_variable_length_min_hops_skips_shallow(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "g",
                "min_hops": 2,
                "max_hops": 3
            }],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    assert!(
        !group_ids.contains(&100) && !group_ids.contains(&101) && !group_ids.contains(&102),
        "depth-1 groups (100, 101, 102) must be excluded by min_hops=2"
    );
    assert!(
        group_ids.contains(&200),
        "depth-2 group (200) should be reachable"
    );
}

async fn traversal_variable_length_with_redaction_at_depth(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5]);
    svc.allow("group", &[100, 101, 102]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{
                "type": "MEMBER_OF",
                "from": "u",
                "to": "g",
                "min_hops": 1,
                "max_hops": 3
            }],
            "limit": 50
        }"#,
        &svc,
    )
    .await;

    let nodes = value["nodes"].as_array().unwrap();
    let group_ids = node_ids(nodes, "Group");
    assert!(
        !group_ids.contains(&200),
        "group 200 should be redacted (not in allow list)"
    );
    assert!(
        !group_ids.contains(&300),
        "group 300 should be redacted (not in allow list)"
    );
    assert!(
        group_ids.contains(&100),
        "group 100 should survive redaction"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal chains (no traversal_path prefix in joins)
// ─────────────────────────────────────────────────────────────────────────────

async fn traversal_chain_user_group_project(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name"]}
            ],
            "relationships": [
                {"type": "MEMBER_OF", "from": "u", "to": "g"},
                {"type": "CONTAINS", "from": "g", "to": "p"}
            ],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_valid(&value);
    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let user_ids = node_ids(nodes, "User");
    let group_ids = node_ids(nodes, "Group");
    let project_ids = node_ids(nodes, "Project");

    assert!(user_ids.contains(&1), "alice should be present");
    assert!(group_ids.contains(&100), "group 100 should be present");
    assert!(
        project_ids.contains(&1000),
        "project 1000 should be present"
    );

    let edges = value["edges"].as_array().unwrap();
    assert!(
        edges.iter().any(|e| e["type"] == "MEMBER_OF"),
        "MEMBER_OF edge should exist"
    );
    assert!(
        edges.iter().any(|e| e["type"] == "CONTAINS"),
        "CONTAINS edge should exist"
    );
}

async fn traversal_chain_user_mr_note(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title"]},
                {"id": "n", "entity": "Note", "id_range": {"start": 1, "end": 10000}, "columns": ["note"]}
            ],
            "relationships": [
                {"type": "AUTHORED", "from": "u", "to": "mr"},
                {"type": "HAS_NOTE", "from": "mr", "to": "n"}
            ],
            "limit": 50
        }"#,
        &allow_all(),
    )
    .await;

    assert_valid(&value);
    assert_eq!(value["query_type"], "traversal");

    let nodes = value["nodes"].as_array().unwrap();
    let user_ids = node_ids(nodes, "User");
    let mr_ids = node_ids(nodes, "MergeRequest");
    let note_ids = node_ids(nodes, "Note");

    assert!(user_ids.contains(&1), "alice should be present");
    assert!(mr_ids.contains(&2000), "MR 2000 should be present");
    assert!(note_ids.contains(&3000), "note 3000 should be present");

    let edges = value["edges"].as_array().unwrap();
    assert!(
        edges.iter().any(|e| e["type"] == "AUTHORED"),
        "AUTHORED edge should exist"
    );
    assert!(
        edges.iter().any(|e| e["type"] == "HAS_NOTE"),
        "HAS_NOTE edge should exist"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Pagination in formatted output
// ─────────────────────────────────────────────────────────────────────────────

async fn pagination_present_in_response(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 2}
        }"#,
        &allow_all(),
    )
    .await;

    assert!(
        value.get("pagination").is_some(),
        "response should include pagination when cursor is present"
    );
    let pagination = &value["pagination"];
    assert_eq!(
        pagination["has_more"], true,
        "5 users, page_size=2 → has_more"
    );
    assert_eq!(pagination["total_rows"], 5, "5 authorized users total");

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2, "cursor should slice to 2 nodes");
}

async fn pagination_absent_without_cursor(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    assert!(
        value.get("pagination").is_none(),
        "response should not include pagination when no cursor"
    );
}

async fn pagination_last_page_has_more_false(ctx: &TestContext) {
    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "limit": 100,
            "cursor": {"offset": 4, "page_size": 10}
        }"#,
        &allow_all(),
    )
    .await;

    let pagination = &value["pagination"];
    assert_eq!(
        pagination["has_more"], false,
        "offset=4, 5 users → last page"
    );
    assert_eq!(pagination["total_rows"], 5);

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 1, "only 1 user left on last page");
}

async fn pagination_with_redaction(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3, 5]);

    let value = run_pipeline(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 100,
            "cursor": {"offset": 0, "page_size": 2}
        }"#,
        &svc,
    )
    .await;

    let pagination = &value["pagination"];
    assert_eq!(
        pagination["total_rows"], 3,
        "3 authorized users after redaction"
    );
    assert_eq!(
        pagination["has_more"], true,
        "3 authorized, page_size=2 → has_more"
    );

    let nodes = value["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);
    let ids: Vec<i64> = nodes
        .iter()
        .filter_map(|n| n["id"].as_str().and_then(|s| s.parse().ok()))
        .collect();
    assert_eq!(ids, vec![1, 3], "first page of authorized users");
}

// ─────────────────────────────────────────────────────────────────────────────
// Test runner
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn graph_formatter_e2e() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    // Read-only subtests share one database (seed once, query many).
    run_subtests_shared!(
        &ctx,
        // Search — properties and redaction
        search_exact_properties,
        search_unicode_properties,
        search_redaction_exact,
        search_no_authorization_returns_empty,
        search_wildcard_columns,
        search_node_ids_filtering,
        search_with_order_by,
        // Search — column types
        search_boolean_columns,
        search_nullable_columns,
        // Traversal — basic
        traversal_single_hop_exact,
        traversal_with_filter,
        traversal_deduplicates_shared_nodes,
        traversal_redaction_removes_unauthorized_paths,
        // Traversal — variable-length (multi-hop)
        traversal_variable_length_reaches_depth_2,
        traversal_variable_length_min_hops_skips_shallow,
        traversal_variable_length_with_redaction_at_depth,
        // Traversal — direction
        traversal_incoming_direction,
        traversal_both_direction,
        // Traversal — chains (no traversal_path prefix in joins)
        traversal_chain_user_group_project,
        traversal_chain_user_mr_note,
        // Aggregation — all functions
        aggregation_count_exact,
        aggregation_sum,
        aggregation_avg,
        aggregation_min_max,
        aggregation_min_string,
        aggregation_multiple_functions,
        aggregation_redaction,
        // Ungrouped aggregation
        ungrouped_count_emits_aggregates,
        ungrouped_multiple_functions_emits_aggregates,
        grouped_aggregation_uses_entity_nodes,
        ungrouped_count_with_redaction,
        // Path finding — type variations
        path_finding_exact_path,
        path_finding_all_shortest,
        path_finding_any,
        path_finding_with_rel_types,
        path_finding_redaction_blocks_path,
        path_finding_max_depth,
        // Neighbors — full coverage
        neighbors_outgoing_exact,
        neighbors_incoming_exact,
        neighbors_both_exact,
        neighbors_both_direction_edges_correct,
        neighbors_both_direction_mixed_entity,
        neighbors_with_rel_types_filter,
        neighbors_dynamic_columns_all,
        neighbors_redaction,
        // Filter operators
        filter_in_operator,
        filter_contains_operator,
        filter_starts_with_operator,
        filter_is_null_operator,
        // Edge cases
        giant_string_survives_pipeline,
        sql_injection_string_preserved,
        empty_result_all_fields_present,
        // Pagination
        pagination_present_in_response,
        pagination_absent_without_cursor,
        pagination_last_page_has_more_false,
        pagination_with_redaction,
    );

    // Mutating subtests need their own forked databases.
    run_subtests!(&ctx, traversal_shared_target_node, search_datetime_columns,);
}

// Schema-negative tests — no ClickHouse required. These hand-craft JSON values
// and assert the schema validator rejects responses missing `format_version`
// or carrying an invalid version string.

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

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

use std::collections::HashSet;
use std::sync::Arc;

use crate::common::{
    DummyClaims, GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext,
    load_ontology, run_redaction, test_security_context,
};
use gkg_server::query_pipeline::{
    GraphFormatter, HydrationStage, PipelineObserver, PipelineRequest, PipelineStage,
    QueryPipelineContext, RedactionOutput, ResultFormatter,
};
use gkg_server::redaction::QueryResult;
use integration_testkit::run_subtests_shared;
use integration_testkit::visitor::{NodeExt, ResponseView};
use query_engine::compile;
use serde_json::Value;

// ─────────────────────────────────────────────────────────────────────────────
// Schema validation
// ─────────────────────────────────────────────────────────────────────────────

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

// ─────────────────────────────────────────────────────────────────────────────
// Infrastructure
// ─────────────────────────────────────────────────────────────────────────────

async fn run_query(ctx: &TestContext, json: &str, svc: &MockRedactionService) -> ResponseView {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());
    let security_ctx = test_security_context();
    let compiled = Arc::new(compile(json, &ontology, &security_ctx).unwrap());

    let batches = ctx.query_parameterized(&compiled.base).await;
    
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);
    let redacted_count = run_redaction(&mut result, svc);

    let mut pipeline_ctx = QueryPipelineContext {
        compiled: Some(Arc::clone(&compiled)),
        ontology: Arc::clone(&ontology),
        client,
        security_context: Some(security_ctx),
    };
    let claims = gkg_server::auth::Claims::dummy();
    let mut req = PipelineRequest::<gkg_server::proto::ExecuteQueryMessage> {
        claims: &claims,
        query_json: "",
        tx: None,
        stream: None,
    };
    let mut obs = PipelineObserver::start();

    let output = HydrationStage
        .execute(
            RedactionOutput {
                query_result: result,
                redacted_count,
            },
            &mut pipeline_ctx,
            &mut req,
            &mut obs,
        )
        .await
        .expect("pipeline should succeed");

    let value = GraphFormatter.format(&output.query_result, &output.result_context, &pipeline_ctx);
    assert_valid(&value);
    let response =
        serde_json::from_value(value).expect("response should deserialize to GraphResponse");
    ResponseView::for_query(&compiled.input, response)
}

fn allow_all() -> MockRedactionService {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 2, 3, 4, 5, 6]);
    svc.allow("group", &[100, 101, 102, 200, 300]);
    svc.allow("project", &[1000, 1001, 1002, 1003, 1004]);
    svc.allow("merge_request", &[2000, 2001, 2002, 2003]);
    svc.allow("note", &[3000, 3001, 3002, 3003]);
    svc
}

// ─────────────────────────────────────────────────────────────────────────────
// Seed data
// ─────────────────────────────────────────────────────────────────────────────
//
// Topology:
//
//   Users:
//     1 alice   (active,  human)
//     2 bob     (active,  human)
//     3 charlie (active,  human)
//     4 diana   (active,  project_bot)
//     5 eve     (blocked, service_account)
//     6 用户_émoji_🎉 (active, human) — unicode stress test
//
//   Groups:
//     100 Public Group   (public,   path 1/100/)
//     101 Private Group  (private,  path 1/101/)
//     102 Internal Group (internal, path 1/102/)
//     200 Deep Group A   (public,   path 1/100/200/)
//     300 Deep Group B   (public,   path 1/100/200/300/)
//
//   Projects:
//     1000 Public Project   (public,   path 1/100/1000/)
//     1001 Private Project  (private,  path 1/101/1001/)
//     1002 Internal Project (internal, path 1/100/1002/)
//     1003 Secret Project   (private,  path 1/101/1003/)
//     1004 Shared Project   (public,   path 1/102/1004/)
//
//   MergeRequests:
//     2000 Add feature A (opened, path 1/100/1000/)
//     2001 Fix bug B     (opened, path 1/100/1000/)
//     2002 Refactor C    (merged, path 1/101/1001/)
//     2003 Update D      (closed, path 1/102/1004/)
//
//   Notes:
//     3000 Normal note           (MR 2000, not confidential, not internal)
//     3001 Confidential note     (MR 2001, confidential=true)
//     3002 Giant string note     (MR 2000, 10000 chars)
//     3003 SQL injection note    (MR 2000, DROP TABLE payload)
//
//   MEMBER_OF edges:
//     User 1 → Group 100, User 1 → Group 102
//     User 2 → Group 100, User 3 → Group 101
//     User 4 → Group 101, User 4 → Group 102, User 5 → Group 101
//     User 6 → Group 100, User 6 → Group 101
//
//   CONTAINS edges:
//     Group 100 → Project 1000, Group 100 → Project 1002
//     Group 100 → Group 200 (subgroup)
//     Group 200 → Group 300 (subgroup depth 2)
//     Group 101 → Project 1001, Group 101 → Project 1003
//     Group 102 → Project 1004
//
//   AUTHORED edges:
//     User 1 → MR 2000, User 1 → MR 2001
//     User 2 → MR 2002, User 3 → MR 2003
//     User 1 → Note 3000
//
//   HAS_NOTE edges:
//     MR 2000 → Note 3000, MR 2000 → Note 3002, MR 2000 → Note 3003
//     MR 2001 → Note 3001

async fn seed(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human'),
         (4, 'diana', 'Diana Developer', 'active', 'project_bot'),
         (5, 'eve', 'Eve External', 'blocked', 'service_account'),
         (6, '用户_émoji_🎉', 'Ünïcödé Üser', 'active', 'human')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/'),
         (200, 'Deep Group A', 'public', '1/100/200/'),
         (300, 'Deep Group B', 'public', '1/100/200/300/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1003, 'Secret Project', 'private', '1/101/1003/'),
         (1004, 'Shared Project', 'public', '1/102/1004/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'opened', 'fix-b', 'main', '1/100/1000/'),
         (2002, 3, 'Refactor C', 'merged', 'refactor-c', 'main', '1/101/1001/'),
         (2003, 4, 'Update D', 'closed', 'update-d', 'main', '1/102/1004/')",
    )
    .await;

    let giant_string = "x".repeat(10_000);
    ctx.execute(&format!(
        "INSERT INTO gl_note (id, note, noteable_type, noteable_id, confidential, internal, traversal_path) VALUES
         (3000, 'Normal note on feature A', 'MergeRequest', 2000, false, false, '1/100/1000/'),
         (3001, 'Confidential feedback on bug B', 'MergeRequest', 2001, true, false, '1/100/1000/'),
         (3002, '{giant_string}', 'MergeRequest', 2000, false, false, '1/100/1000/'),
         (3003, 'Robert''); DROP TABLE gl_note;--', 'MergeRequest', 2000, false, false, '1/100/1000/')",
    ))
    .await;

    ctx.execute(
        "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/', 6, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 6, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/200/', 100, 'Group', 'CONTAINS', 200, 'Group'),
         ('1/100/200/300/', 200, 'Group', 'CONTAINS', 300, 'Group'),
         ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
         ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project'),
         ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project'),
         ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project'),
         ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2000, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 2001, 'MergeRequest'),
         ('1/101/1001/', 2, 'User', 'AUTHORED', 2002, 'MergeRequest'),
         ('1/102/1004/', 3, 'User', 'AUTHORED', 2003, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 3000, 'Note'),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3000, 'Note'),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3002, 'Note'),
         ('1/100/1000/', 2000, 'MergeRequest', 'HAS_NOTE', 3003, 'Note'),
         ('1/100/1000/', 2001, 'MergeRequest', 'HAS_NOTE', 3001, 'Note')",
    )
    .await;

    ctx.optimize_all().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Search: Column Value Correctness
// ─────────────────────────────────────────────────────────────────────────────

async fn search_returns_correct_user_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "name", "state", "user_type"]},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_node_order("User", &[1, 2, 3, 4, 5, 6]);

    let alice = resp.find_node("User", 1).unwrap();
    alice.assert_str("username", "alice");
    alice.assert_str("name", "Alice Admin");
    alice.assert_str("state", "active");
    alice.assert_str("user_type", "human");

    let bob = resp.find_node("User", 2).unwrap();
    bob.assert_str("username", "bob");
    bob.assert_str("name", "Bob Builder");

    let eve = resp.find_node("User", 5).unwrap();
    eve.assert_str("username", "eve");
    eve.assert_str("state", "blocked");
    eve.assert_str("user_type", "service_account");
}

async fn search_returns_correct_project_properties(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(5);

    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project")
            && n.prop_str("visibility_level") == Some("public")
    });
    resp.assert_node("Project", 1003, |n| {
        n.prop_str("name") == Some("Secret Project")
            && n.prop_str("visibility_level") == Some("private")
    });
}

async fn search_filter_eq_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "state"],
                     "filters": {"state": "blocked"}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "state", |n| n.prop_str("state") == Some("blocked"));
    let eve = resp.find_node("User", 5).unwrap();
    eve.assert_str("username", "eve");
}

async fn search_filter_in_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"],
                     "filters": {"visibility_level": {"op": "in", "value": ["public", "internal"]}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("Project", &[1000, 1002, 1004]);

    resp.assert_filter("Project", "visibility_level", |n| {
        let vis = n.prop_str("visibility_level").unwrap_or("");
        vis == "public" || vis == "internal"
    });
}

async fn search_filter_starts_with_returns_matching_rows(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "starts_with", "value": "a"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(1);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.starts_with("a"))
    });
    resp.find_node("User", 1)
        .unwrap()
        .assert_str("username", "alice");
}

async fn search_node_ids_returns_only_specified(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "g", "entity": "Group", "columns": ["name"], "node_ids": [100, 102]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("Group", &[100, 102]);
    resp.find_node("Group", 100)
        .unwrap()
        .assert_str("name", "Public Group");
    resp.find_node("Group", 102)
        .unwrap()
        .assert_str("name", "Internal Group");
    resp.assert_node_absent("Group", 101);
}

async fn search_filter_contains_returns_substring_matches(ctx: &TestContext) {
    
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"],
                     "filters": {"username": {"op": "contains", "value": "li"}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("User", &[1, 3]);
    resp.assert_filter("User", "username", |n| {
        n.prop_str("username").is_some_and(|u| u.contains("li"))
    });
}

async fn search_filter_is_null_matches_unset_columns(ctx: &TestContext) {
    // avatar_url is Nullable(String) in ClickHouse, so IS NULL matches
    // rows where no avatar has been set (our seed data never sets it).
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "avatar_url"],
                     "filters": {"avatar_url": {"op": "is_null", "value": true}}},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_filter("User", "avatar_url", |n| {
        n.prop_str("username").is_some() && n.prop("avatar_url").is_none()
    });
}

async fn search_with_order_by_desc(ctx: &TestContext) {
    
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_count(6);
    resp.assert_node_order("User", &[6, 5, 4, 3, 2, 1]);
}

async fn search_no_auth_returns_empty(ctx: &TestContext) {
    
    let svc = MockRedactionService::new();
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_count(0);
}

async fn search_redaction_returns_only_allowed_ids(ctx: &TestContext) {
    
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1, 3]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username"]},
            "limit": 10
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_ids("User", &[1, 3]);
    resp.assert_node_absent("User", 2);
    resp.assert_node_absent("User", 5);
}

async fn search_unicode_properties_survive_pipeline(ctx: &TestContext) {
    
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "search",
            "node": {"id": "u", "entity": "User", "columns": ["username", "name"],
                     "node_ids": [6]},
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("User", &[6]);
    resp.assert_node("User", 6, |n| {
        n.prop_str("username") == Some("用户_émoji_🎉")
            && n.prop_str("name") == Some("Ünïcödé Üser")
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Traversal: Join Correctness + Edge Data
// ─────────────────────────────────────────────────────────────────────────────

async fn traversal_user_group_returns_correct_pairs_and_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_edge_set(
        "MEMBER_OF",
        &[
            (1, 100),
            (1, 102),
            (2, 100),
            (3, 101),
            (4, 101),
            (4, 102),
            (5, 101),
            (6, 100),
            (6, 101),
        ],
    );

    resp.assert_referential_integrity();

    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_node("Group", 100, |n| n.prop_str("name") == Some("Public Group"));
    resp.assert_node("Group", 101, |n| {
        n.prop_str("name") == Some("Private Group")
    });
}

async fn traversal_three_hop_returns_all_user_group_project_paths(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "p", "entity": "Project", "columns": ["name"]}
            ],
            "relationships": [
                {"type": "MEMBER_OF", "from": "u", "to": "g"},
                {"type": "CONTAINS", "from": "g", "to": "p"}
            ],
            "limit": 30
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_referential_integrity();

    let member_of: HashSet<(i64, i64)> = resp
        .edges_of_type("MEMBER_OF")
        .iter()
        .map(|e| (e.from_id, e.to_id))
        .collect();
    let contains: HashSet<(i64, i64)> = resp
        .edges_of_type("CONTAINS")
        .iter()
        .map(|e| (e.from_id, e.to_id))
        .collect();

    assert!(member_of.contains(&(1, 100)));
    assert!(member_of.contains(&(1, 102)));
    assert!(member_of.contains(&(2, 100)));
    assert!(contains.contains(&(100, 1000)));
    assert!(contains.contains(&(100, 1002)));
    assert!(contains.contains(&(101, 1001)));
    assert!(contains.contains(&(102, 1004)));

    resp.assert_node("User", 1, |n| n.prop_str("username") == Some("alice"));
    resp.assert_node("Project", 1000, |n| {
        n.prop_str("name") == Some("Public Project")
    });
}

async fn traversal_user_authored_mr_returns_correct_edges(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest", "columns": ["title", "state"]}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "limit": 20
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_referential_integrity();

    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
    resp.assert_edge_exists("User", 1, "MergeRequest", 2001, "AUTHORED");
    resp.assert_edge_exists("User", 2, "MergeRequest", 2002, "AUTHORED");
    resp.assert_edge_exists("User", 3, "MergeRequest", 2003, "AUTHORED");

    resp.assert_node("MergeRequest", 2000, |n| {
        n.prop_str("title") == Some("Add feature A") && n.prop_str("state") == Some("opened")
    });
    resp.assert_node("MergeRequest", 2002, |n| {
        n.prop_str("title") == Some("Refactor C") && n.prop_str("state") == Some("merged")
    });
}

async fn traversal_redaction_removes_unauthorized_data(ctx: &TestContext) {
    let mut svc = MockRedactionService::new();
    svc.allow("user", &[1]);
    svc.allow("group", &[100]);

    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "g", "entity": "Group", "columns": ["name"]}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 20
        }"#,
        &svc,
    )
    .await;

    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100]);
    resp.assert_node_absent("User", 2);
    resp.assert_node_absent("Group", 102);
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_absent("User", 1, "Group", 102, "MEMBER_OF");
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregation: Result Value Correctness
// ─────────────────────────────────────────────────────────────────────────────

async fn aggregation_count_returns_correct_values(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "u", "entity": "User", "columns": ["username"]},
                {"id": "mr", "entity": "MergeRequest"}
            ],
            "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
            "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("User", 1, |n| {
        n.prop_str("username") == Some("alice") && n.prop_i64("mr_count") == Some(2)
    });

    resp.assert_node("User", 2, |n| {
        n.prop_str("username") == Some("bob") && n.prop_i64("mr_count") == Some(1)
    });

    resp.assert_node("User", 3, |n| {
        n.prop_str("username") == Some("charlie") && n.prop_i64("mr_count") == Some(1)
    });
}

async fn aggregation_count_group_contains_projects(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "aggregation",
            "nodes": [
                {"id": "g", "entity": "Group", "columns": ["name"]},
                {"id": "p", "entity": "Project"}
            ],
            "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
            "aggregations": [{"function": "count", "target": "p", "group_by": "g", "alias": "project_count"}],
            "limit": 10
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node("Group", 100, |n| {
        n.prop_str("name") == Some("Public Group") && n.prop_i64("project_count") == Some(2)
    });
    resp.assert_node("Group", 101, |n| {
        n.prop_str("name") == Some("Private Group") && n.prop_i64("project_count") == Some(2)
    });
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group") && n.prop_i64("project_count") == Some(1)
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding: Path Composition Correctness
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_returns_valid_complete_paths(ctx: &TestContext) {
    let resp = run_query(
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

    resp.assert_referential_integrity();

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        1,
        "exactly one shortest path from User 1 to Project 1000"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert_eq!(path.len(), 2, "path {pid}: User→Group→Project = 2 edges");

        let first = path[0];
        assert_eq!(first.from, "User");
        assert_eq!(first.from_id, 1);
        assert_eq!(first.edge_type, "MEMBER_OF");
        assert_eq!(first.step, Some(0));

        let last = path.last().unwrap();
        assert_eq!(last.to, "Project");
        assert_eq!(last.to_id, 1000);
        assert_eq!(last.edge_type, "CONTAINS");

        for edge in &path {
            assert_eq!(edge.path_id, Some(pid), "edge should belong to path {pid}");
            assert!(edge.step.is_some(), "path_finding edges must have step");
        }
    }

    resp.assert_node_exists("User", 1);
    resp.assert_node_exists("Group", 100);
    resp.assert_node_exists("Project", 1000);
}

async fn path_finding_multiple_destinations_returns_distinct_paths(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        3,
        "exactly 3 paths: to 1000 (via 100), 1002 (via 100), 1004 (via 102)"
    );

    let destinations: HashSet<i64> = pids
        .iter()
        .filter_map(|&pid| resp.path(pid).last().map(|e| e.to_id))
        .collect();
    assert_eq!(
        destinations,
        HashSet::from([1000, 1002, 1004]),
        "each path should reach exactly one of the requested projects"
    );

    resp.assert_referential_integrity();
}

async fn path_finding_consecutive_edges_connect(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [1]},
                {"id": "end", "entity": "Project", "node_ids": [1000, 1004]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
        &allow_all(),
    )
    .await;

    let pids = resp.path_ids();
    assert_eq!(
        pids.len(),
        2,
        "exactly 2 paths: to 1000 (via 100) and 1004 (via 102)"
    );

    for &pid in pids.iter() {
        let path = resp.path(pid);
        assert_eq!(path.len(), 2, "path {pid}: User→Group→Project = 2 edges");
        for window in path.windows(2) {
            let prev = window[0];
            let next = window[1];
            assert_eq!(
                (prev.to.as_str(), prev.to_id),
                (next.from.as_str(), next.from_id),
                "consecutive path edges must connect: {prev:?} → {next:?}",
            );
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors: Relationship Metadata Correctness
// ─────────────────────────────────────────────────────────────────────────────

async fn neighbors_outgoing_returns_correct_targets(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "u", "entity": "User", "node_ids": [1]},
            "neighbors": {"node": "u", "direction": "outgoing"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_referential_integrity();

    resp.assert_node_ids("User", &[1]);
    resp.assert_node_ids("Group", &[100, 102]);
    resp.assert_node_ids("MergeRequest", &[2000, 2001]);
    resp.assert_node_ids("Note", &[3000]);

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "Group", 102, "MEMBER_OF");
    resp.assert_edge_exists("User", 1, "MergeRequest", 2000, "AUTHORED");
    resp.assert_edge_exists("User", 1, "Note", 3000, "AUTHORED");

    resp.assert_node("Group", 100, |n| n.prop_str("name") == Some("Public Group"));
    resp.assert_node("Group", 102, |n| {
        n.prop_str("name") == Some("Internal Group")
    });
}

async fn neighbors_incoming_returns_correct_sources(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "incoming", "rel_types": ["MEMBER_OF"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("Group", &[100]);
    resp.assert_node_ids("User", &[1, 2, 6]);

    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 2, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("User", 6, "Group", 100, "MEMBER_OF");
}

async fn neighbors_rel_types_filter_works(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "outgoing", "rel_types": ["CONTAINS"]}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_node_ids("Project", &[1000, 1002]);
    resp.assert_edge_count("CONTAINS", 3);
}

async fn neighbors_both_direction_returns_all_connected(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "neighbors",
            "node": {"id": "g", "entity": "Group", "node_ids": [100]},
            "neighbors": {"node": "g", "direction": "both"}
        }"#,
        &allow_all(),
    )
    .await;

    resp.assert_node_ids("Group", &[100, 200]);
    resp.assert_node_ids("User", &[1, 2, 6]);
    resp.assert_node_ids("Project", &[1000, 1002]);

    resp.assert_referential_integrity();
    resp.assert_edge_exists("User", 1, "Group", 100, "MEMBER_OF");
    resp.assert_edge_exists("Group", 100, "Group", 200, "CONTAINS");
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-cutting: Referential Integrity
// ─────────────────────────────────────────────────────────────────────────────

async fn traversal_referential_integrity_on_complex_query(ctx: &TestContext) {
    let resp = run_query(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User"},
                {"id": "g", "entity": "Group"},
                {"id": "p", "entity": "Project"}
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

    resp.assert_referential_integrity();

    let member_of = resp.edges_of_type("MEMBER_OF");
    assert!(!member_of.is_empty(), "should have MEMBER_OF edges");
    let contains = resp.edges_of_type("CONTAINS");
    assert!(!contains.is_empty(), "should have CONTAINS edges");
}

// ─────────────────────────────────────────────────────────────────────────────
// Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn data_correctness() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    // All subtests are read-only against the same seed data, so they share
    // one database instead of forking a copy per subtest.
    run_subtests_shared!(
        &ctx,
        // search
        search_returns_correct_user_properties,
        search_returns_correct_project_properties,
        search_filter_eq_returns_matching_rows,
        search_filter_in_returns_matching_rows,
        search_filter_starts_with_returns_matching_rows,
        search_filter_contains_returns_substring_matches,
        search_filter_is_null_matches_unset_columns,
        search_node_ids_returns_only_specified,
        search_with_order_by_desc,
        search_no_auth_returns_empty,
        search_redaction_returns_only_allowed_ids,
        search_unicode_properties_survive_pipeline,
        // traversal
        traversal_user_group_returns_correct_pairs_and_edges,
        traversal_three_hop_returns_all_user_group_project_paths,
        traversal_user_authored_mr_returns_correct_edges,
        traversal_redaction_removes_unauthorized_data,
        // aggregation
        aggregation_count_returns_correct_values,
        aggregation_count_group_contains_projects,
        // path finding
        path_finding_returns_valid_complete_paths,
        path_finding_multiple_destinations_returns_distinct_paths,
        path_finding_consecutive_edges_connect,
        // neighbors
        neighbors_outgoing_returns_correct_targets,
        neighbors_incoming_returns_correct_sources,
        neighbors_rel_types_filter_works,
        neighbors_both_direction_returns_all_connected,
        // referential integrity
        traversal_referential_integrity_on_complex_query,
    );
}

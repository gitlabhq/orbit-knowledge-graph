//! E2E integration tests for the redaction flow.
//!
//! Each `#[tokio::test]` starts a single ClickHouse container and runs all
//! subtests sequentially, truncating tables between them to avoid cross-test
//! contamination while eliminating per-test container startup overhead.

use std::collections::HashSet;

use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, compile_and_execute,
    load_ontology, run_redaction, test_security_context,
};
use gkg_server::redaction::QueryResult;
use integration_testkit::{run_subtests, run_subtests_shared, t};
use query_engine::compiler::{build_entity_auth, compile};

fn table_users() -> String {
    t("gl_user")
}
fn table_groups() -> String {
    t("gl_group")
}
fn table_projects() -> String {
    t("gl_project")
}
fn table_merge_requests() -> String {
    t("gl_merge_request")
}
fn table_edges() -> String {
    t("gl_edge")
}

fn edge_table_for(relationship: &str) -> String {
    let ontology = load_ontology();
    // load_ontology() already applies the schema version prefix.
    ontology
        .edge_table_for_relationship(relationship)
        .to_string()
}

const ALL_USER_IDS: &[i64] = &[1, 2, 3, 4, 5];
const ALL_GROUP_IDS: &[i64] = &[100, 101, 102];
const ALL_PROJECT_IDS: &[i64] = &[1000, 1001, 1002, 1003, 1004];
const ALL_MR_IDS: &[i64] = &[2000, 2001, 2002, 2003];

async fn setup_test_data(ctx: &TestContext) {
    // User.state is string-based enum (enum_type: string), User.user_type is int-based enum
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human'),
         (4, 'diana', 'Diana Developer', 'active', 'project_bot'),
         (5, 'eve', 'Eve External', 'blocked', 'service_account')",
        table_users()
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/')",
        table_groups()
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1003, 'Secret Project', 'private', '1/101/1003/'),
         (1004, 'Shared Project', 'public', '1/102/1004/')",
        table_projects()
    ))
    .await;

    // MergeRequest.state is int-based enum (no enum_type in ontology)
    // Values: 1=opened, 2=closed, 3=merged, 4=locked
    ctx.execute(&format!(
        "INSERT INTO {} (id, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 'Fix bug B', 'opened', 'fix-b', 'main', '1/100/1000/'),
         (2002, 'Refactor C', 'merged', 'refactor-c', 'main', '1/101/1001/'),
         (2003, 'Update D', 'closed', 'update-d', 'main', '1/102/1004/')",
        table_merge_requests()
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/102/', 1, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/100/', 2, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/101/', 4, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/102/', 4, 'User', 'MEMBER_OF', 102, 'Group'),
         ('1/101/', 5, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/1000/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
         ('1/100/1002/', 100, 'Group', 'CONTAINS', 1002, 'Project'),
         ('1/101/1001/', 101, 'Group', 'CONTAINS', 1001, 'Project'),
         ('1/101/1003/', 101, 'Group', 'CONTAINS', 1003, 'Project'),
         ('1/102/1004/', 102, 'Group', 'CONTAINS', 1004, 'Project')",
        table_edges()
    ))
    .await;

    ctx.optimize_all().await;
}

// ─────────────────────────────────────────────────────────────────────────────
// Fail-Closed / Core Redaction Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn fail_closed_no_authorization_returns_nothing(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&u)).collect();
    assert_eq!(
        raw_ids,
        ALL_USER_IDS.iter().copied().collect::<HashSet<_>>(),
        "before redaction, all 5 users should be present"
    );

    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 5, "all 5 rows must be redacted");
    assert_eq!(result.authorized_count(), 0, "no rows should be authorized");
}

async fn fail_closed_partial_authorization_denies_unknown_ids(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    assert_eq!(result.len(), 5);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        redacted, 3,
        "users 3, 4, 5 should be redacted (not in allow list)"
    );
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2]));
    assert!(!authorized_ids.contains(&3));
    assert!(!authorized_ids.contains(&4));
    assert!(!authorized_ids.contains(&5));
}

async fn fail_closed_explicit_deny_filters_row(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2, 3, 4]);
    mock_service.deny("user", &[5]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "only user 5 should be redacted");
    assert_eq!(result.authorized_count(), 4);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2, 3, 4]));
    assert!(!authorized_ids.contains(&5));
}

async fn single_hop_user_group_verifies_both_nodes(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let raw_pairs: HashSet<(i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?)))
        .collect();

    let expected_raw = HashSet::from([
        (1, 100),
        (1, 102),
        (2, 100),
        (3, 101),
        (4, 101),
        (4, 102),
        (5, 101),
    ]);
    assert_eq!(
        raw_pairs, expected_raw,
        "raw data should match edge definitions"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);
    mock_service.allow("group", &[100]);

    run_redaction(&mut result, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?)))
        .collect();

    let expected_authorized = HashSet::from([(1, 100), (2, 100)]);
    assert_eq!(
        authorized_pairs, expected_authorized,
        "only user 1,2 with group 100 should pass"
    );

    for (u, g) in &raw_pairs {
        let should_be_authorized = (*u == 1 || *u == 2) && *g == 100;
        assert_eq!(
            authorized_pairs.contains(&(*u, *g)),
            should_be_authorized,
            "pair ({u}, {g}) authorization mismatch"
        );
    }
}

async fn two_hop_denying_intermediate_group_filters_all_paths_through_it(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);
    mock_service.allow("group", &[100]);
    mock_service.deny("group", &[101, 102]);

    run_redaction(&mut result, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?)))
        .collect();

    assert_eq!(
        authorized_pairs,
        HashSet::from([(1, 100), (2, 100)]),
        "only paths through group 100 should remain"
    );

    for row in result.authorized_rows() {
        let group_id = row.get_id(&g).unwrap();
        assert_eq!(group_id, 100, "no denied groups should appear");
    }
}

async fn three_hop_user_group_project_verifies_all_paths(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}}
        ],
        "relationships": [
            {"type": "MEMBER_OF", "from": "u", "to": "g"},
            {"type": "CONTAINS", "from": "g", "to": "p"}
        ],
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let raw_paths: HashSet<(i64, i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    let expected_raw = HashSet::from([
        (1, 100, 1000),
        (1, 100, 1002),
        (1, 102, 1004),
        (2, 100, 1000),
        (2, 100, 1002),
        (3, 101, 1001),
        (3, 101, 1003),
        (4, 101, 1001),
        (4, 101, 1003),
        (4, 102, 1004),
        (5, 101, 1001),
        (5, 101, 1003),
    ]);
    assert_eq!(
        raw_paths, expected_raw,
        "raw 3-hop paths should match edge joins"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);
    mock_service.allow("group", &[100]);
    mock_service.allow("project", &[1000]);

    run_redaction(&mut result, &mock_service);

    let authorized_paths: HashSet<(i64, i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    assert_eq!(
        authorized_paths,
        HashSet::from([(1, 100, 1000), (2, 100, 1000)]),
        "only fully authorized paths should remain"
    );
}

async fn three_hop_denying_one_project_removes_only_those_paths(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}}
        ],
        "relationships": [
            {"type": "MEMBER_OF", "from": "u", "to": "g"},
            {"type": "CONTAINS", "from": "g", "to": "p"}
        ],
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);
    mock_service.allow("group", ALL_GROUP_IDS);
    mock_service.allow("project", &[1000, 1002, 1004]);
    mock_service.deny("project", &[1001, 1003]);

    run_redaction(&mut result, &mock_service);

    let authorized_paths: HashSet<(i64, i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    let expected = HashSet::from([
        (1, 100, 1000),
        (1, 100, 1002),
        (1, 102, 1004),
        (2, 100, 1000),
        (2, 100, 1002),
        (4, 102, 1004),
    ]);
    assert_eq!(authorized_paths, expected);

    for row in result.authorized_rows() {
        let project_id = row.get_id(&p).unwrap();
        assert!(
            project_id != 1001 && project_id != 1003,
            "denied projects 1001, 1003 must not appear"
        );
    }

    for (u, g, p) in &expected {
        assert!(
            authorized_paths.contains(&(*u, *g, *p)),
            "expected path ({u}, {g}, {p}) should be authorized"
        );
    }
}

async fn group_project_two_hop_verifies_exact_pairs(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();

    let raw_pairs: HashSet<(i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    let expected_raw = HashSet::from([
        (100, 1000),
        (100, 1002),
        (101, 1001),
        (101, 1003),
        (102, 1004),
    ]);
    assert_eq!(raw_pairs, expected_raw);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", &[100, 102]);
    mock_service.deny("group", &[101]);
    mock_service.allow("project", &[1000, 1002, 1004]);
    mock_service.deny("project", &[1001, 1003]);

    run_redaction(&mut result, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    let expected_authorized = HashSet::from([(100, 1000), (100, 1002), (102, 1004)]);
    assert_eq!(authorized_pairs, expected_authorized);

    assert!(!authorized_pairs.iter().any(|(g, _)| *g == 101));
    assert!(
        !authorized_pairs
            .iter()
            .any(|(_, p)| *p == 1001 || *p == 1003)
    );
}

async fn single_node_project_query_verifies_all_projects(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let p = result.ctx().get("p").unwrap().clone();

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&p)).collect();
    assert_eq!(
        raw_ids,
        ALL_PROJECT_IDS.iter().copied().collect::<HashSet<_>>()
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("project", &[1000, 1004]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&p))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1000, 1004]));
    assert!(!authorized_ids.contains(&1001));
    assert!(!authorized_ids.contains(&1002));
    assert!(!authorized_ids.contains(&1003));
}

async fn all_nodes_have_required_type_columns(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}}
        ],
        "relationships": [
            {"type": "MEMBER_OF", "from": "u", "to": "g"},
            {"type": "CONTAINS", "from": "g", "to": "p"}
        ],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    for col in [
        "_gkg_u_id",
        "_gkg_u_type",
        "_gkg_g_id",
        "_gkg_g_type",
        "_gkg_p_id",
        "_gkg_p_type",
    ] {
        assert!(
            query.base.sql.contains(&format!("AS {col}")),
            "SQL must include {col}"
        );
    }

    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    for row in result.iter() {
        assert_eq!(row.get_type(&u), Some("User"));
        assert_eq!(row.get_type(&g), Some("Group"));
        assert_eq!(row.get_type(&p), Some("Project"));
        assert!(row.get_id(&u).is_some());
        assert!(row.get_id(&g).is_some());
        assert!(row.get_id(&p).is_some());
    }
}

async fn empty_query_result_stays_empty(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "filters": {"username": "nonexistent"}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(result.len(), 0);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 0);
}

async fn all_authorized_preserves_all_data(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", ALL_GROUP_IDS);
    mock_service.allow("project", ALL_PROJECT_IDS);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        redacted, 0,
        "nothing should be redacted when all authorized"
    );
    assert_eq!(result.authorized_count(), raw_count);
}

async fn all_columns_preserved_after_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();

    let columns_before: HashSet<String> = result
        .rows()
        .first()
        .map(|r| {
            ["_gkg_g_id", "_gkg_g_type", "_gkg_p_id", "_gkg_p_type"]
                .iter()
                .filter(|c| r.get(c).is_some())
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();

    assert!(
        columns_before.contains("_gkg_g_id"),
        "_gkg_g_id should exist before redaction"
    );
    assert!(
        columns_before.contains("_gkg_g_type"),
        "_gkg_g_type should exist before redaction"
    );
    assert!(
        columns_before.contains("_gkg_p_id"),
        "_gkg_p_id should exist before redaction"
    );
    assert!(
        columns_before.contains("_gkg_p_type"),
        "_gkg_p_type should exist before redaction"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", &[100]);
    mock_service.deny("group", &[101, 102]);
    mock_service.allow("project", &[1000, 1002]);
    mock_service.deny("project", &[1001, 1003, 1004]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        2,
        "only group 100 -> projects 1000, 1002 should pass"
    );

    for row in result.authorized_rows() {
        assert!(
            row.get("_gkg_g_id").is_some(),
            "_gkg_g_id preserved after redaction"
        );
        assert!(
            row.get("_gkg_g_type").is_some(),
            "_gkg_g_type preserved after redaction"
        );
        assert!(
            row.get("_gkg_p_id").is_some(),
            "_gkg_p_id preserved after redaction"
        );
        assert!(
            row.get("_gkg_p_type").is_some(),
            "_gkg_p_type preserved after redaction"
        );
    }

    let authorized: Vec<_> = result.authorized_rows().collect();

    let row_1000 = authorized
        .iter()
        .find(|r| r.get_id(&p) == Some(1000))
        .unwrap();
    assert_eq!(row_1000.get_id(&g), Some(100));
    assert_eq!(row_1000.get_type(&g), Some("Group"));
    assert_eq!(row_1000.get_type(&p), Some("Project"));

    let row_1002 = authorized
        .iter()
        .find(|r| r.get_id(&p) == Some(1002))
        .unwrap();
    assert_eq!(row_1002.get_id(&g), Some(100));
    assert_eq!(row_1002.get_type(&g), Some("Group"));
    assert_eq!(row_1002.get_type(&p), Some("Project"));
}

async fn all_columns_preserved_on_three_hop_traversal(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}}
        ],
        "relationships": [
            {"type": "MEMBER_OF", "from": "u", "to": "g"},
            {"type": "CONTAINS", "from": "g", "to": "p"}
        ],
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);
    mock_service.allow("project", &[1000]);

    run_redaction(&mut result, &mock_service);

    assert!(result.authorized_count() > 0);

    for row in result.authorized_rows() {
        assert_eq!(row.get_id(&u), Some(1));
        assert_eq!(row.get_id(&g), Some(100));
        assert_eq!(row.get_id(&p), Some(1000));

        assert_eq!(row.get_type(&u), Some("User"));
        assert_eq!(row.get_type(&g), Some("Group"));
        assert_eq!(row.get_type(&p), Some("Project"));
    }
}

async fn redacted_rows_filtered_from_authorized_iterator(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    let all_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&u)).collect();
    assert_eq!(
        all_ids,
        ALL_USER_IDS.iter().copied().collect::<HashSet<_>>()
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2]));
    assert!(!authorized_ids.contains(&3));
    assert!(!authorized_ids.contains(&4));
    assert!(!authorized_ids.contains(&5));

    let unauthorized_ids: HashSet<i64> = result
        .rows()
        .iter()
        .filter(|r| !r.is_authorized())
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(unauthorized_ids, HashSet::from([3, 4, 5]));
}

/// Verifies fail-closed behavior: rows with NULL entity IDs must be denied.
///
/// This can occur with outer joins or data inconsistencies. Unverifiable rows
/// must never pass authorization since we cannot confirm access rights.
#[test]
fn fail_closed_null_id_denies_row() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::compiler::ResultContext;
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    let ontology = load_ontology();

    let batch = make_batch(vec![
        (
            "_gkg_u_id",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as Arc<dyn Array>,
        ),
        (
            "_gkg_u_type",
            Arc::new(StringArray::from(vec!["User", "User", "User"])) as Arc<dyn Array>,
        ),
        (
            "_gkg_p_id",
            Arc::new(Int64Array::from(vec![100, 200, 300])) as Arc<dyn Array>,
        ),
        (
            "_gkg_p_type",
            Arc::new(StringArray::from(vec!["Project", "Project", "Project"])) as Arc<dyn Array>,
        ),
    ]);

    let mut ctx = ResultContext::new();
    ctx.add_node("u", "User");
    ctx.add_node("p", "Project");
    for (entity, config) in build_entity_auth(&ontology) {
        ctx.add_entity_auth(entity, config);
    }

    let mut result = QueryResult::from_batches(&[batch], &ctx);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 3]);
    mock_service.allow("project", &[100, 200, 300]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "row with NULL user ID must be redacted");
    assert_eq!(result.authorized_count(), 2);
    assert!(
        result.rows()[0].is_authorized(),
        "user 1 should be authorized"
    );
    assert!(
        !result.rows()[1].is_authorized(),
        "NULL ID row must be denied (fail-closed)"
    );
    assert!(
        result.rows()[2].is_authorized(),
        "user 3 should be authorized"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_extracts_all_nodes_from_path(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Find paths from user 1 to project 1000 (path: User 1 -> Group 100 -> Project 1000)
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert!(!result.is_empty(), "should find at least one path");

    // Each row should have path_nodes containing (User 1, Group 100, Project 1000)
    for row in result.iter() {
        let path_nodes = row.path_nodes();
        assert!(
            path_nodes.len() >= 2,
            "path should have at least start and end nodes"
        );

        // First node should be User 1
        assert_eq!(path_nodes[0].id, 1);
        assert_eq!(path_nodes[0].entity_type, "User");

        // Last node should be Project 1000
        let last = path_nodes.last().unwrap();
        assert_eq!(last.id, 1000);
        assert_eq!(last.entity_type, "Project");

        // edge_kinds should have one entry per hop (nodes - 1)
        let edge_kinds = row.edge_kinds();
        assert_eq!(
            edge_kinds.len(),
            path_nodes.len() - 1,
            "edge_kinds should have one entry per hop"
        );

        // User(1) --MEMBER_OF--> Group(100) --CONTAINS--> Project(1000)
        if path_nodes.len() == 3 {
            assert_eq!(edge_kinds[0], "MEMBER_OF");
            assert_eq!(edge_kinds[1], "CONTAINS");
        }
    }
}

async fn path_finding_no_authorization_returns_nothing(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should find paths before redaction");

    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, raw_count, "all paths should be redacted");
    assert_eq!(result.authorized_count(), 0);
}

async fn path_finding_denying_intermediate_node_filters_path(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Find paths from user 1 to any project in group 100 or 102
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should find paths");

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]); // Only allow group 100, deny 102
    mock_service.deny("group", &[102]);
    mock_service.allow("project", &[1000, 1002, 1004]);

    run_redaction(&mut result, &mock_service);

    // Only paths through group 100 should remain
    for row in result.authorized_rows() {
        let path_nodes = row.path_nodes();
        for node in path_nodes {
            assert_ne!(
                node.id, 102,
                "denied group 102 should not appear in authorized paths"
            );
        }
    }

    // Paths to 1000 and 1002 (via group 100) should be authorized
    // Path to 1004 (via group 102) should be denied
    let authorized_ends: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().last().map(|n| n.id))
        .collect();

    assert!(
        authorized_ends.contains(&1000) || authorized_ends.contains(&1002),
        "paths through group 100 should be authorized"
    );
    assert!(
        !authorized_ends.contains(&1004),
        "path through denied group 102 should be filtered"
    );

    // Surviving paths must have valid edge_kinds
    for row in result.authorized_rows() {
        let path_nodes = row.path_nodes();
        let edge_kinds = row.edge_kinds();
        assert_eq!(
            edge_kinds.len(),
            path_nodes.len() - 1,
            "edge_kinds length must match hops in surviving paths"
        );
        for kind in edge_kinds {
            assert!(
                !kind.is_empty(),
                "surviving path must not have empty edge kinds"
            );
        }
    }
}

async fn path_finding_all_nodes_authorized_preserves_paths(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1, 2]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);
    mock_service.allow("group", ALL_GROUP_IDS);
    mock_service.allow("project", ALL_PROJECT_IDS);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        redacted, 0,
        "nothing should be redacted when all authorized"
    );
    assert_eq!(result.authorized_count(), raw_count);

    // Verify edge_kinds survive redaction intact
    for row in result.authorized_rows() {
        let path_nodes = row.path_nodes();
        let edge_kinds = row.edge_kinds();
        assert_eq!(
            edge_kinds.len(),
            path_nodes.len() - 1,
            "edge_kinds length must equal hops after redaction"
        );
        // Every hop should have a non-empty relationship kind
        for (i, kind) in edge_kinds.iter().enumerate() {
            assert!(!kind.is_empty(), "edge_kinds[{}] should not be empty", i);
        }
    }
}

async fn path_finding_denying_start_node_filters_all_paths(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert!(!result.is_empty());

    let mut mock_service = MockRedactionService::new();
    mock_service.deny("user", &[1]); // Deny the start node
    mock_service.allow("group", ALL_GROUP_IDS);
    mock_service.allow("project", ALL_PROJECT_IDS);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, result.len(), "all paths should be redacted");
    assert_eq!(result.authorized_count(), 0);
}

async fn path_finding_denying_end_node_filters_those_paths(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", ALL_GROUP_IDS);
    mock_service.allow("project", &[1000]);
    mock_service.deny("project", &[1002]); // Deny one end node

    run_redaction(&mut result, &mock_service);

    // Only paths to project 1000 should remain
    let authorized_ends: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().last().map(|n| n.id))
        .collect();

    assert!(authorized_ends.contains(&1000));
    assert!(!authorized_ends.contains(&1002));
}

/// Path finding with multiple valid paths to same destination - authorization
/// must check ALL nodes in EACH path independently. Denying a node in one path
/// should not affect other paths that don't traverse that node.
async fn path_finding_multiple_paths_independent_authorization(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // User 1 can reach project 1000 via group 100
    // User 1 can also reach project 1002 via group 100
    // These are independent paths that share intermediate nodes
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count >= 2, "should find paths to both projects");

    // Authorize the path through group 100 to project 1000 only
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);
    mock_service.allow("project", &[1000]);
    mock_service.deny("project", &[1002]); // Deny one destination

    run_redaction(&mut result, &mock_service);

    // Only paths ending at 1000 should remain
    let authorized_ends: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().last().map(|n| n.id))
        .collect();

    assert!(
        authorized_ends.contains(&1000),
        "path to 1000 should be authorized"
    );
    assert!(
        !authorized_ends.contains(&1002),
        "path to denied project 1002 should be filtered"
    );
}

/// Verify that path finding correctly handles the case where the same node
/// appears at different depths. Each path instance is checked independently.
async fn path_finding_shared_intermediate_node_authorization(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Multiple users can reach the same projects through group 100
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1, 2]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count >= 2, "should find paths from both users");

    // Authorize user 1's path but deny user 2
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[2]);
    mock_service.allow("group", &[100]);
    mock_service.allow("project", &[1000]);

    run_redaction(&mut result, &mock_service);

    // Only user 1's path should remain
    let authorized_starts: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().first().map(|n| n.id))
        .collect();

    assert_eq!(
        authorized_starts,
        HashSet::from([1]),
        "only user 1's path should be authorized"
    );
}

/// Path finding with max depth traversal - verifies that authorization
/// is checked for ALL nodes in paths, not just start/end.
async fn path_finding_deep_traversal_all_nodes_verified(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Path: User -> Group -> Project (depth 2 is realistic for our data, max allowed is 3)
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should find some paths");

    // Authorize everything except intermediate group 102
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]); // Only group 100
    mock_service.deny("group", &[102]); // Deny group 102
    mock_service.allow("project", ALL_PROJECT_IDS);

    run_redaction(&mut result, &mock_service);

    // Verify no paths go through group 102
    for row in result.authorized_rows() {
        for node in row.path_nodes() {
            if node.entity_type == "Group" {
                assert_ne!(
                    node.id, 102,
                    "denied group 102 should never appear in authorized paths"
                );
            }
        }
    }

    // Paths through group 100 (to 1000, 1002) should work
    let authorized_ends: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().last().map(|n| n.id))
        .collect();

    assert!(
        authorized_ends.contains(&1000) || authorized_ends.contains(&1002),
        "at least one path through group 100 should be authorized"
    );
    assert!(
        !authorized_ends.contains(&1004),
        "path to 1004 (via group 102) should be filtered"
    );
}

/// Verify path finding with zero valid paths after authorization returns empty.
async fn path_finding_all_paths_denied_returns_empty(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert!(!result.is_empty(), "should have paths before redaction");

    // Deny ALL intermediate nodes - paths cannot complete
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("group", ALL_GROUP_IDS); // Deny all groups
    mock_service.allow("project", &[1000]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        0,
        "all paths should be denied when intermediates are denied"
    );
}

/// Verifies that edge_kinds are correctly populated and preserved through redaction.
///
/// Tests:
/// - edge_kinds array length equals path_nodes length - 1
/// - edge_kinds contain the correct relationship types in path order
/// - edge_kinds are preserved in authorized paths after partial redaction
/// - edge_kinds are not leaked through redacted paths
async fn path_finding_edge_kinds_preserved_through_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Query paths from users to projects via groups.
    // User 1 -> Group 100 -> Project 1000 (MEMBER_OF, CONTAINS)
    // User 1 -> Group 100 -> Project 1002 (MEMBER_OF, CONTAINS)
    // User 1 -> Group 102 -> Project 1004 (MEMBER_OF, CONTAINS)
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1002, 1004]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(raw_count >= 3, "should find paths to all 3 projects");

    // Verify edge_kinds before redaction
    for row in result.iter() {
        let path_nodes = row.path_nodes();
        let edge_kinds = row.edge_kinds();
        assert_eq!(
            edge_kinds.len(),
            path_nodes.len() - 1,
            "pre-redaction: edge_kinds must have one entry per hop"
        );

        // All paths are 3 nodes: User -> Group -> Project
        if path_nodes.len() == 3 {
            assert_eq!(
                edge_kinds[0], "MEMBER_OF",
                "first hop should be MEMBER_OF (User->Group)"
            );
            assert_eq!(
                edge_kinds[1], "CONTAINS",
                "second hop should be CONTAINS (Group->Project)"
            );
        }
    }

    // Authorize only paths through group 100, deny group 102
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);
    mock_service.deny("group", &[102]);
    mock_service.allow("project", &[1000, 1002, 1004]);

    run_redaction(&mut result, &mock_service);

    // Paths to 1000 and 1002 (via group 100) should survive
    // Path to 1004 (via group 102) should be denied
    assert!(
        result.authorized_count() >= 2,
        "at least 2 paths through group 100 should survive"
    );

    // Verify edge_kinds are preserved in authorized paths
    for row in result.authorized_rows() {
        let path_nodes = row.path_nodes();
        let edge_kinds = row.edge_kinds();

        assert_eq!(
            edge_kinds.len(),
            path_nodes.len() - 1,
            "post-redaction: edge_kinds length must still match hops"
        );

        // All surviving paths go User(1) -> Group(100) -> Project
        assert_eq!(path_nodes[0].id, 1);
        assert_eq!(path_nodes[0].entity_type, "User");
        assert_eq!(path_nodes[1].id, 100);
        assert_eq!(path_nodes[1].entity_type, "Group");

        assert_eq!(edge_kinds[0], "MEMBER_OF");
        assert_eq!(edge_kinds[1], "CONTAINS");

        // The end project must not be 1004 (denied group 102 path)
        let end_id = path_nodes.last().unwrap().id;
        assert_ne!(
            end_id, 1004,
            "path to project 1004 (via denied group 102) must not survive"
        );
    }

    // Collect surviving end-project IDs
    let authorized_ends: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().last().map(|n| n.id))
        .collect();
    assert!(
        authorized_ends.contains(&1000),
        "path to 1000 should survive"
    );
    assert!(
        authorized_ends.contains(&1002),
        "path to 1002 should survive"
    );
    assert!(
        !authorized_ends.contains(&1004),
        "path to 1004 must be denied"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Search Query Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn search_with_complex_filters_and_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for active users whose names start with a letter in the first half of
    // the alphabet, using multiple filter operators simultaneously
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "filters": {
                "state": {"op": "eq", "value": "active"},
                "username": {"op": "in", "value": ["alice", "bob", "charlie", "diana"]}
            }
        },
        "order_by": {"node": "u", "property": "username", "direction": "ASC"},
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Verify search queries don't generate JOINs
    assert!(
        !query.base.sql.contains("JOIN"),
        "search queries should not produce JOINs, got: {}",
        query.base.sql
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // Should find alice, bob, charlie, diana (all active and in the username list)
    // eve is blocked so filtered out by the state filter
    let raw_usernames: Vec<i64> = result.iter().filter_map(|r| r.get_id(&u)).collect();
    assert_eq!(
        raw_usernames.len(),
        4,
        "should find 4 active users matching filters"
    );

    // Now apply redaction: only allow users 1 (alice) and 2 (bob)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);
    mock_service.deny("user", &[3, 4]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 2, "charlie and diana should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2]));
}

async fn search_projects_with_visibility_and_path_filters(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for projects that are either public or internal
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "p",
            "entity": "Project",
            "filters": {
                "visibility_level": {"op": "in", "value": ["public", "internal"]}
            }
        },
        "order_by": {"node": "p", "property": "id", "direction": "ASC"},
        "limit": 50
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let p = result.ctx().get("p").unwrap().clone();

    // Should find: 1000 (public), 1002 (internal), 1004 (public)
    // Not: 1001, 1003 (private)
    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&p)).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([1000, 1002, 1004]),
        "should find only public and internal projects"
    );

    // Redaction: allow only project 1000
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("project", &[1000]);
    mock_service.deny("project", &[1002, 1004]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&p))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1000]));
}

async fn search_groups_with_traversal_path_starts_with(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for all groups by ID range (traversal_path is not user-filterable)
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "g",
            "entity": "Group",
            "node_ids": [100, 101, 102]
        },
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();

    // All our test groups have paths starting with "1/"
    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&g)).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([100, 101, 102]),
        "should find all groups under root"
    );

    // Partial authorization
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", &[100, 102]);
    mock_service.deny("group", &[101]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&g))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([100, 102]));
}

async fn search_with_id_range_filter(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for users with IDs in a specific range
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "id_range": {"start": 2, "end": 4}
        },
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&u)).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([2, 3, 4]),
        "should find users 2, 3, 4 within ID range"
    );

    // Full authorization for this range
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[2, 3, 4]);

    let redacted = run_redaction(&mut result, &mock_service);
    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 3);
}

async fn search_with_specific_node_ids(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for specific projects by ID
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "p",
            "entity": "Project",
            "node_ids": [1000, 1003]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let p = result.ctx().get("p").unwrap().clone();

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&p)).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([1000, 1003]),
        "should find only the specified projects"
    );

    // Allow one, deny the other
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("project", &[1000]);
    mock_service.deny("project", &[1003]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&p))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1000]));
}

async fn search_no_results_with_impossible_filter(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for a user that doesn't exist
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "filters": {
                "username": {"op": "eq", "value": "definitely_does_not_exist_12345"}
            }
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(result.len(), 0, "should find no users");

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);

    let redacted = run_redaction(&mut result, &mock_service);
    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 0);
}

async fn search_fail_closed_no_authorization(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "g",
            "entity": "Group",
            "id_range": {"start": 1, "end": 10000}
        },
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert_eq!(raw_count, 3, "should find all 3 groups");

    // No authorizations at all - fail closed
    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 3, "all groups should be redacted");
    assert_eq!(
        result.authorized_count(),
        0,
        "fail-closed: nothing authorized"
    );
}

async fn search_preserves_metadata_columns_after_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "filters": {
                "state": "active"
            }
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Verify the SQL includes the required metadata columns
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "SQL should include _gkg_u_id"
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "SQL should include _gkg_u_type"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // Check columns exist before redaction
    for row in result.iter() {
        assert!(row.get_id(&u).is_some(), "ID should exist before redaction");
        assert_eq!(row.get_type(&u), Some("User"), "type should be User");
    }

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);

    run_redaction(&mut result, &mock_service);

    // Check columns still exist after redaction
    for row in result.authorized_rows() {
        assert_eq!(row.get_id(&u), Some(1));
        assert_eq!(row.get_type(&u), Some("User"));
    }
}

/// Verifies fail-closed behavior: rows with NULL entity type must be denied.
#[test]
fn fail_closed_null_type_denies_row() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::compiler::ResultContext;
    use std::sync::Arc;

    fn make_batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, arr)| Field::new(*name, arr.data_type().clone(), true))
            .collect();
        let schema = Arc::new(Schema::new(fields));
        let arrays: Vec<Arc<dyn Array>> = columns.into_iter().map(|(_, arr)| arr).collect();
        RecordBatch::try_new(schema, arrays).unwrap()
    }

    let ontology = load_ontology();

    let batch = make_batch(vec![
        (
            "_gkg_u_id",
            Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>,
        ),
        (
            "_gkg_u_type",
            Arc::new(StringArray::from(vec![Some("User"), None])) as Arc<dyn Array>,
        ),
    ]);

    let mut ctx = ResultContext::new();
    ctx.add_node("u", "User");
    for (entity, config) in build_entity_auth(&ontology) {
        ctx.add_entity_auth(entity, config);
    }

    let mut result = QueryResult::from_batches(&[batch], &ctx);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "row with NULL type must be redacted");
    assert!(result.rows()[0].is_authorized());
    assert!(
        !result.rows()[1].is_authorized(),
        "NULL type row must be denied (fail-closed)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Column Selection Integration Subtests
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests verify the column selection feature introduced in the query DSL.
// Users can now specify which columns to return:
//   - `"columns": "*"` - all columns for the entity
//   - `"columns": ["username", "state"]` - specific columns
//   - omitted - only mandatory columns (_gkg_*_id, _gkg_*_type) for redaction
//
// CRITICAL: Mandatory columns must ALWAYS be present for redaction to work.

/// Verify mandatory columns (`_gkg_*_id`, `_gkg_*_type`) are present when
/// requesting specific columns, and redaction works correctly.
async fn column_selection_specific_columns_includes_mandatory_columns(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Request only username and state, but mandatory columns must still appear
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "id_range": {"start": 1, "end": 10000},
            "columns": ["username", "state"]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // The generated SQL MUST contain the mandatory redaction columns
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "SQL must include _gkg_u_id for redaction. Got: {}",
        query.base.sql
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "SQL must include _gkg_u_type for redaction. Got: {}",
        query.base.sql
    );

    // Also verify the requested columns are present
    assert!(
        query.base.sql.contains("u_username"),
        "SQL must include requested column u_username"
    );
    assert!(
        query.base.sql.contains("u_state"),
        "SQL must include requested column u_state"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    assert_eq!(result.len(), 5, "should have all 5 users before redaction");

    // Run redaction with partial authorization
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2, 3]);
    mock_service.deny("user", &[4, 5]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 2, "users 4 and 5 should be redacted");
    assert_eq!(result.authorized_count(), 3);

    // Verify authorized rows have correct IDs and types
    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([1, 2, 3]));

    for row in result.authorized_rows() {
        assert_eq!(row.get_type(&u), Some("User"));
    }
}

/// Verify wildcard `"*"` returns all entity columns plus mandatory columns,
/// and redaction works correctly with all columns selected.
/// Uses Group entity which has all ontology columns present in the test schema.
async fn column_selection_wildcard_returns_all_columns_plus_mandatory(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Use Group entity - all its ontology columns exist in gl_group
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "g",
            "entity": "Group",
            "id_range": {"start": 1, "end": 10000},
            "columns": "*"
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // CRITICAL: Mandatory columns must be present for redaction
    assert!(
        query.base.sql.contains("_gkg_g_id"),
        "wildcard must include _gkg_g_id for redaction"
    );
    assert!(
        query.base.sql.contains("_gkg_g_type"),
        "wildcard must include _gkg_g_type for redaction"
    );

    // Group entity columns from ontology
    assert!(
        query.base.sql.contains("g_id"),
        "wildcard should include g_id column"
    );
    assert!(
        query.base.sql.contains("g_name"),
        "wildcard should include g_name column"
    );
    assert!(
        query.base.sql.contains("g_visibility_level"),
        "wildcard should include g_visibility_level column"
    );
    assert!(
        query.base.sql.contains("g_traversal_path"),
        "wildcard should include g_traversal_path column"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();

    assert_eq!(result.len(), 3, "should have all 3 groups before redaction");

    // Run redaction - allow only group 100 (Public Group)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", &[100]);
    mock_service.deny("group", &[101, 102]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 2, "groups 101 and 102 should be redacted");
    assert_eq!(result.authorized_count(), 1);

    // Verify the authorized row is group 100
    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized.len(), 1);
    assert_eq!(authorized[0].get_id(&g), Some(100));
    assert_eq!(authorized[0].get_type(&g), Some("Group"));
}

/// Verify omitting `columns` entirely still includes mandatory columns
/// and redaction works correctly.
async fn column_selection_omitted_includes_mandatory_columns(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // No columns specified - should still work for redaction
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Mandatory columns MUST be present even when columns is omitted
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "mandatory _gkg_u_id must be present when columns omitted"
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "mandatory _gkg_u_type must be present when columns omitted"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    assert_eq!(result.len(), 5, "should have all 5 users");

    // Run redaction - allow users 1, 2; deny the rest
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);
    mock_service.deny("user", &[3, 4, 5]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 3, "users 3, 4, 5 should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([1, 2]));

    for row in result.authorized_rows() {
        assert_eq!(row.get_type(&u), Some("User"));
    }
}

/// Deep test: Verify multi-hop traversal with different column selections
/// per node still includes mandatory columns for ALL nodes, and redaction
/// works correctly across the entire path.
///
/// This is the most complex case: User -> Group -> Project with different
/// column selections on each node. Redaction must verify authorization
/// for every node in the path.
async fn column_selection_multi_hop_traversal_all_nodes_have_mandatory_columns(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Three-hop traversal with mixed column selections:
    // - User: specific columns
    // - Group: specific columns (not wildcard to avoid missing columns)
    // - Project: specific columns
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            {"id": "g", "entity": "Group", "columns": ["name"]},
            {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "visibility_level"]}
        ],
        "relationships": [
            {"type": "MEMBER_OF", "from": "u", "to": "g"},
            {"type": "CONTAINS", "from": "g", "to": "p"}
        ],
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // CRITICAL: ALL nodes must have mandatory columns for redaction
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "User node must have _gkg_u_id. SQL: {}",
        query.base.sql
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "User node must have _gkg_u_type"
    );
    assert!(
        query.base.sql.contains("_gkg_g_id"),
        "Group node must have _gkg_g_id"
    );
    assert!(
        query.base.sql.contains("_gkg_g_type"),
        "Group node must have _gkg_g_type"
    );
    assert!(
        query.base.sql.contains("_gkg_p_id"),
        "Project node must have _gkg_p_id"
    );
    assert!(
        query.base.sql.contains("_gkg_p_type"),
        "Project node must have _gkg_p_type"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let raw_count = result.len();
    assert!(raw_count > 0, "should have traversal results");

    // Run redaction: allow specific path (user 1 -> group 100 -> project 1000)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[2, 3, 4, 5]);
    mock_service.allow("group", &[100]);
    mock_service.deny("group", &[101, 102]);
    mock_service.allow("project", &[1000]);
    mock_service.deny("project", &[1001, 1002, 1003, 1004]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert!(redacted > 0, "some paths should be redacted");

    // Only one path should remain: user 1 -> group 100 -> project 1000
    assert_eq!(
        result.authorized_count(),
        1,
        "only one path should be authorized"
    );

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized[0].get_id(&u), Some(1));
    assert_eq!(authorized[0].get_id(&g), Some(100));
    assert_eq!(authorized[0].get_id(&p), Some(1000));
    assert_eq!(authorized[0].get_type(&u), Some("User"));
    assert_eq!(authorized[0].get_type(&g), Some("Group"));
    assert_eq!(authorized[0].get_type(&p), Some("Project"));
}

/// Deep test: Verify redaction works correctly when using specific column selection.
/// Authorization checks depend on mandatory columns - if they were missing,
/// redaction would fail or behave incorrectly.
async fn column_selection_redaction_works_with_specific_columns(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "state"]},
            {"id": "g", "entity": "Group", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    let raw_count = result.len();
    assert!(raw_count > 0, "should have raw results");

    // Authorize only user 1 and group 100
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);
    mock_service.deny("user", &[2, 3, 4, 5]);
    mock_service.deny("group", &[101, 102]);

    let redacted = run_redaction(&mut result, &mock_service);

    // Should have filtered out unauthorized rows
    assert!(redacted > 0, "some rows should be redacted");
    assert!(
        result.authorized_count() < raw_count,
        "authorized count should be less than raw"
    );

    // Verify only authorized combinations remain
    for row in result.authorized_rows() {
        let user_id = row.get_id(&u).expect("user ID must exist after redaction");
        let group_id = row.get_id(&g).expect("group ID must exist after redaction");

        assert_eq!(user_id, 1, "only user 1 should be authorized");
        assert_eq!(group_id, 100, "only group 100 should be authorized");

        // Verify types are correct (used for redaction lookup)
        assert_eq!(row.get_type(&u), Some("User"));
        assert_eq!(row.get_type(&g), Some("Group"));
    }
}

/// Deep test: Verify that denying ANY node in a path filters the entire row,
/// even when using column selection. This ensures fail-closed behavior.
async fn column_selection_fail_closed_on_any_unauthorized_node(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Three-hop query with column selection
    let json = r#"{
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
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // Authorize user and group, but DENY the project
    // This should filter ALL rows because fail-closed
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);
    mock_service.allow("group", ALL_GROUP_IDS);
    mock_service.deny("project", ALL_PROJECT_IDS); // Deny all projects

    let redacted = run_redaction(&mut result, &mock_service);

    // All rows should be filtered because projects are denied
    assert_eq!(
        result.authorized_count(),
        0,
        "all rows should be filtered when any node is unauthorized"
    );
    assert!(
        redacted > 0,
        "redaction should have removed rows: redacted {}",
        redacted
    );
}

/// Deep test: Verify column values are preserved correctly through
/// the entire query and redaction pipeline.
async fn column_selection_data_values_preserved_through_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "id_range": {"start": 1, "end": 10000},
            "columns": ["username", "name", "state"],
            "filters": {"username": {"op": "in", "value": ["alice", "bob"]}}
        },
        "order_by": {"node": "u", "property": "username", "direction": "ASC"},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // Before redaction, verify we have data
    assert_eq!(result.len(), 2, "should find alice and bob");

    // Allow both users
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(result.authorized_count(), 2);

    // Collect authorized rows and verify data integrity
    let authorized: Vec<_> = result.authorized_rows().collect();

    // Find alice (user 1) and verify her data
    let alice = authorized.iter().find(|r| r.get_id(&u) == Some(1)).unwrap();
    assert_eq!(alice.get_id(&u), Some(1));
    assert_eq!(alice.get_type(&u), Some("User"));

    // Find bob (user 2) and verify his data
    let bob = authorized.iter().find(|r| r.get_id(&u) == Some(2)).unwrap();
    assert_eq!(bob.get_id(&u), Some(2));
    assert_eq!(bob.get_type(&u), Some("User"));
}

/// Deep test: Verify that requesting the same column as a mandatory column
/// (e.g., "id" in the columns list) doesn't cause duplicates or errors,
/// and redaction still works correctly.
async fn column_selection_id_in_list_no_duplication(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Explicitly request "id" alongside other columns
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "p",
            "entity": "Project",
            "id_range": {"start": 1, "end": 10000},
            "columns": ["id", "name", "visibility_level"]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Should have mandatory columns plus requested columns (no duplicates)
    assert!(
        query.base.sql.contains("_gkg_p_id"),
        "mandatory _gkg_p_id must exist"
    );
    assert!(
        query.base.sql.contains("_gkg_p_type"),
        "mandatory _gkg_p_type must exist"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let p = result.ctx().get("p").unwrap().clone();

    assert_eq!(result.len(), 5, "should have all 5 projects");

    // Run redaction - allow only public projects (1000, 1004)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("project", &[1000, 1004]);
    mock_service.deny("project", &[1001, 1002, 1003]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 3, "3 projects should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&p))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([1000, 1004]));

    for row in result.authorized_rows() {
        assert_eq!(row.get_type(&p), Some("Project"));
    }
}

/// Deep test: Verify aggregation queries properly handle column selection
/// and redaction works on the group_by node.
/// Aggregations only add mandatory columns for the group_by node, not the target.
async fn column_selection_aggregation_only_group_by_node_has_mandatory_columns(ctx: &TestContext) {
    setup_test_data(ctx).await;

    // Insert some additional data for aggregation
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, traversal_path) VALUES
         (10001, 1, 'MR 1', 'merged', '1/100/1000/'),
         (10002, 2, 'MR 2', 'merged', '1/100/1000/'),
         (10003, 3, 'MR 3', 'open', '1/100/1000/')",
        table_merge_requests()
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 1, 'User', 'AUTHORED', 10001, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 10002, 'MergeRequest'),
         ('1/100/1000/', 2, 'User', 'AUTHORED', 10003, 'MergeRequest')",
        table_edges()
    ))
    .await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            {"id": "mr", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // User (group_by node) should have mandatory columns
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "group_by node must have _gkg_u_id"
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "group_by node must have _gkg_u_type"
    );

    // MergeRequest (target node, being aggregated) should NOT have mandatory columns
    // because it doesn't appear as individual rows
    assert!(
        !query.base.sql.contains("_gkg_mr_id"),
        "aggregated target node should not have _gkg_mr_id"
    );
    assert!(
        !query.base.sql.contains("_gkg_mr_type"),
        "aggregated target node should not have _gkg_mr_type"
    );

    // Should have the aggregation
    assert!(
        query.base.sql.contains("COUNT"),
        "should have COUNT aggregation"
    );
    assert!(
        query.base.sql.contains("GROUP BY"),
        "should have GROUP BY clause"
    );

    // User's requested columns should be in SELECT and GROUP BY
    assert!(
        query.base.sql.contains("u_username"),
        "group_by node requested columns should be in SELECT: {}",
        query.base.sql
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // Should have 2 rows (user 1 with 2 MRs, user 2 with 1 MR)
    assert_eq!(result.len(), 2, "should have 2 aggregation rows");

    // Run redaction - only allow user 1
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[2]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "user 2's row should be redacted");
    assert_eq!(result.authorized_count(), 1);

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized[0].get_id(&u), Some(1));
    assert_eq!(authorized[0].get_type(&u), Some("User"));
}

/// Deep test: Verify aggregation with wildcard columns returns all entity fields
/// for the group_by node.
async fn column_selection_aggregation_with_wildcard_columns(ctx: &TestContext) {
    setup_test_data(ctx).await;

    // Insert MRs for aggregation
    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, traversal_path) VALUES
         (10001, 1, 'MR 1', 'merged', '1/100/1000/'),
         (10002, 2, 'MR 2', 'merged', '1/100/1000/'),
         (10003, 3, 'MR 3', 'open', '1/100/1000/')",
        table_merge_requests()
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 1, 'User', 'AUTHORED', 10001, 'MergeRequest'),
         ('1/100/1000/', 1, 'User', 'AUTHORED', 10002, 'MergeRequest'),
         ('1/100/1000/', 2, 'User', 'AUTHORED', 10003, 'MergeRequest')",
        table_edges()
    ))
    .await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Use wildcard columns to get all user fields
    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": "*"},
            {"id": "mr", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // User (group_by node) should have mandatory columns
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "group_by node must have _gkg_u_id"
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "group_by node must have _gkg_u_type"
    );

    // Wildcard should expand to include user columns from ontology
    assert!(
        query.base.sql.contains("u_id"),
        "wildcard should include u_id: {}",
        query.base.sql
    );
    assert!(
        query.base.sql.contains("u_username"),
        "wildcard should include u_username: {}",
        query.base.sql
    );

    // The aggregation query should contain a GROUP BY clause.
    // Dedup subqueries also add GROUP BY, so just verify presence.
    assert!(
        query.base.sql.contains("GROUP BY"),
        "aggregation query must have a GROUP BY clause"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // Should have 2 rows (user 1 with 2 MRs, user 2 with 1 MR)
    assert_eq!(result.len(), 2, "should have 2 aggregation rows");

    // Run redaction - only allow user 1
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[2]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "user 2's row should be redacted");
    assert_eq!(result.authorized_count(), 1);

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized[0].get_id(&u), Some(1));
    assert_eq!(authorized[0].get_type(&u), Some("User"));
}

/// Deep test: Verify that column selection with traversal maintains proper
/// JOIN semantics. Rows should still match correctly across relationships.
async fn column_selection_traversal_join_semantics_preserved(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Two-hop traversal with specific columns
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "visibility_level"]},
            {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();

    // Verify raw data matches expected relationships
    let raw_pairs: HashSet<(i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    let expected_pairs = HashSet::from([
        (100, 1000), // Public Group -> Public Project
        (100, 1002), // Public Group -> Internal Project
        (101, 1001), // Private Group -> Private Project
        (101, 1003), // Private Group -> Secret Project
        (102, 1004), // Internal Group -> Shared Project
    ]);

    assert_eq!(
        raw_pairs, expected_pairs,
        "column selection should not affect JOIN results"
    );

    // Apply redaction and verify it still works
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", &[100]);
    mock_service.allow("project", &[1000, 1002]);

    run_redaction(&mut result, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&g)?, r.get_id(&p)?)))
        .collect();

    assert_eq!(
        authorized_pairs,
        HashSet::from([(100, 1000), (100, 1002)]),
        "redaction should work correctly with column selection"
    );
}

/// Deep test: Verify filters work correctly with column selection.
/// Even if a column is used in a filter, it must be explicitly requested
/// or only mandatory columns appear.
async fn column_selection_filters_work_with_columns(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Filter by state, but only select username
    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "id_range": {"start": 1, "end": 10000},
            "columns": ["username"],
            "filters": {"state": "active"}
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Should have mandatory columns and requested column
    assert!(query.base.sql.contains("_gkg_u_id"));
    assert!(query.base.sql.contains("_gkg_u_type"));
    assert!(query.base.sql.contains("u_username"));

    // Filter by state should be in WHERE clause
    assert!(
        query.base.sql.contains("state") || query.base.sql.contains("WHERE"),
        "query should filter by state"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // Should find 4 active users (eve is blocked)
    assert_eq!(result.len(), 4, "should find 4 active users");

    // Redaction should work
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 2, 3, 4]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(result.authorized_count(), 4);
    for row in result.authorized_rows() {
        assert!(row.get_id(&u).is_some());
        assert_eq!(row.get_type(&u), Some("User"));
    }
}

/// Deep test: Ensure that column selection with no authorization
/// still exhibits fail-closed behavior.
async fn column_selection_fail_closed_no_authorization(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {
            "id": "u",
            "entity": "User",
            "id_range": {"start": 1, "end": 10000},
            "columns": ["username", "name", "state"]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert_eq!(raw_count, 5, "should have all 5 users");

    // No authorizations - fail closed
    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 5, "all users should be redacted (fail-closed)");
    assert_eq!(result.authorized_count(), 0, "nothing should be authorized");
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors Query Subtests
// ─────────────────────────────────────────────────────────────────────────────
//
// Neighbors queries discover connected nodes dynamically. Unlike traversals where
// node types are known at query time, neighbors could be any entity type. This
// requires checking authorization for both the center node AND each neighbor.

/// Comprehensive test for neighbors queries with redaction.
///
/// Tests:
/// - Neighbors query returns expected columns (_gkg_neighbor_id, _gkg_neighbor_type, _gkg_relationship_type)
/// - Center node has mandatory redaction columns (_gkg_*_id, _gkg_*_type)
/// - Both center node AND neighbor authorization is required (fail-closed)
/// - Different directions (outgoing, incoming) work correctly
/// - Relationship type filtering works with redaction
async fn neighbors_query_comprehensive(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // --- Test 1: Verify SQL structure and query execution ---
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Verify center node mandatory columns for redaction
    assert!(
        query.base.sql.contains("_gkg_u_id"),
        "neighbors query must include _gkg_u_id for center node. SQL: {}",
        query.base.sql
    );
    assert!(
        query.base.sql.contains("_gkg_u_type"),
        "neighbors query must include _gkg_u_type"
    );

    // Verify neighbor columns are present
    assert!(
        query.base.sql.contains("_gkg_neighbor_id"),
        "must include _gkg_neighbor_id"
    );
    assert!(
        query.base.sql.contains("_gkg_neighbor_type"),
        "must include _gkg_neighbor_type"
    );
    assert!(
        query.base.sql.contains("_gkg_relationship_type"),
        "must include _gkg_relationship_type"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    // User 1 is member of groups 100 and 102
    assert_eq!(
        result.len(),
        2,
        "user 1 should have 2 outgoing neighbors (groups 100, 102)"
    );

    // Verify center node metadata
    for row in result.iter() {
        assert_eq!(row.get_id(&u), Some(1));
        assert_eq!(row.get_type(&u), Some("User"));
        assert!(
            row.neighbor_node().is_some(),
            "neighbor node should be extracted"
        );
    }

    // --- Test 2: Fail-closed when NO authorization provided ---
    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        0,
        "fail-closed with no authorization"
    );
    assert_eq!(redacted, 2, "all rows should be redacted");

    // --- Test 3: Fail-closed when only center node authorized (neighbors not authorized) ---
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]); // Only authorize center node

    let redacted = run_redaction(&mut result, &mock_service);

    // Neighbors (groups 100, 102) are NOT authorized, so rows should be redacted
    assert_eq!(
        result.authorized_count(),
        0,
        "neighbors must also be authorized (fail-closed)"
    );
    assert_eq!(redacted, 2);

    // --- Test 4: Both center node AND neighbors authorized ---
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100, 102]); // Authorize both neighbor groups

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 0, "nothing redacted when all nodes authorized");
    assert_eq!(result.authorized_count(), 2);

    // Verify neighbor data is accessible
    let neighbor_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert_eq!(neighbor_ids, HashSet::from([100, 102]));

    // --- Test 5: Partial neighbor authorization filters specific rows ---
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]); // Only authorize group 100
    mock_service.deny("group", &[102]); // Deny group 102

    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        1,
        "only one neighbor should pass"
    );

    let authorized_neighbor = result
        .authorized_rows()
        .next()
        .and_then(|r| r.neighbor_node())
        .expect("should have authorized neighbor");
    assert_eq!(authorized_neighbor.id, 100);
    assert_eq!(authorized_neighbor.entity_type, "Group");
}

/// Tests that denying the center node filters ALL its neighbors.
async fn neighbors_query_center_node_denied_filters_all(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(result.len(), 2, "should have 2 neighbors before redaction");

    // Authorize neighbors but DENY center node
    let mut mock_service = MockRedactionService::new();
    mock_service.deny("user", &[1]);
    mock_service.allow("group", ALL_GROUP_IDS);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 2, "all rows redacted when center node denied");
    assert_eq!(result.authorized_count(), 0);
}

/// Tests neighbors query with multiple center nodes and mixed authorization.
async fn neighbors_query_multiple_center_nodes_mixed_authorization(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Query neighbors for users 1 and 3
    // User 1 -> groups 100, 102
    // User 3 -> group 101
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1, 3]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let u = result.ctx().get("u").unwrap().clone();

    let raw_count = result.len();
    assert_eq!(
        raw_count, 3,
        "should have 3 total neighbors (2 for user 1, 1 for user 3)"
    );

    // Authorize user 1 and its neighbors, deny user 3
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[3]);
    mock_service.allow("group", &[100, 102]); // User 1's neighbors
    mock_service.deny("group", &[101]); // User 3's neighbor

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 1, "user 3's neighbor row should be redacted");
    assert_eq!(result.authorized_count(), 2);

    // Verify only user 1's neighbors remain
    let authorized_center_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(authorized_center_ids, HashSet::from([1]));
}

/// Tests incoming direction with neighbor authorization.
async fn neighbors_query_incoming_with_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Find users who are members of group 100 (incoming MEMBER_OF edges)
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "g", "entity": "Group", "node_ids": [100]},
        "neighbors": {"node": "g", "direction": "incoming", "rel_types": ["MEMBER_OF"]}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // Group 100 has incoming MEMBER_OF from users 1 and 2
    assert_eq!(result.len(), 2, "group 100 should have 2 incoming members");

    // Authorize center (group 100) and one neighbor (user 1)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("group", &[100]);
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[2]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        1,
        "only user 1's row should pass"
    );

    let neighbor = result
        .authorized_rows()
        .next()
        .and_then(|r| r.neighbor_node())
        .unwrap();
    assert_eq!(neighbor.id, 1);
    assert_eq!(neighbor.entity_type, "User");
}

// ─────────────────────────────────────────────────────────────────────────────
// Indirect Auth (Dynamic Nodes) Subtests
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests verify redaction for entities that authorize via an owner entity
// (e.g. Definition authorizes via its owning Project, MergeRequestDiff via
// its owning MergeRequest). The auth ID must be resolved from the owner entity
// in the same row, not from the entity's own ID.

fn table_files() -> String {
    t("gl_file")
}
fn table_definitions() -> String {
    t("gl_definition")
}

/// Insert code entities (File, Definition) with edges to an existing Project.
/// Requires setup_test_data() to have been called first.
async fn setup_indirect_auth_data(ctx: &TestContext) {
    // Files belonging to Project 1000 (Public Project, traversal_path '1/100/1000/')
    ctx.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, path, name, extension, language) VALUES
         (3000, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'lib.rs', 'rs', 'Rust'),
         (3001, '1/100/1000/', 1000, 'main', 'src/main.rs', 'main.rs', 'rs', 'Rust')",
        table_files()
    ))
    .await;

    // Definitions in those files, also belonging to Project 1000
    ctx.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, file_path, fqn, name, definition_type, start_line, end_line, start_byte, end_byte) VALUES
         (5000, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'crate::MyStruct', 'MyStruct', 'class', 10, 50, 100, 500),
         (5001, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'crate::my_func', 'my_func', 'function', 60, 80, 600, 900),
         (5002, '1/100/1000/', 1000, 'main', 'src/main.rs', 'crate::main', 'main', 'function', 1, 20, 0, 200)",
        table_definitions()
    ))
    .await;

    // File belonging to Project 1001 (Private Project, traversal_path '1/101/1001/')
    ctx.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, path, name, extension, language) VALUES
         (3002, '1/101/1001/', 1001, 'main', 'src/secret.rs', 'secret.rs', 'rs', 'Rust')",
        table_files()
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, file_path, fqn, name, definition_type, start_line, end_line, start_byte, end_byte) VALUES
         (5003, '1/101/1001/', 1001, 'main', 'src/secret.rs', 'crate::Secret', 'Secret', 'class', 1, 30, 0, 300)",
        table_definitions()
    ))
    .await;

    // Edges: File --DEFINES--> Definition (table derived from ontology)
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 3000, 'File', 'DEFINES', 5000, 'Definition'),
         ('1/100/1000/', 3000, 'File', 'DEFINES', 5001, 'Definition'),
         ('1/100/1000/', 3001, 'File', 'DEFINES', 5002, 'Definition'),
         ('1/101/1001/', 3002, 'File', 'DEFINES', 5003, 'Definition')",
        edge_table_for("DEFINES")
    ))
    .await;
}

/// Neighbors query where neighbor is an indirect-auth entity (Definition).
/// Center = File (also indirect auth, owner: Project via project_id).
/// Authorization checks for both must go through the owning Project.
///
/// Tests:
/// - Allowing the owning Project authorizes the File center and Definition neighbors
/// - Denying the owning Project denies everything (fail-closed)
/// - Mixed: allow one project, deny another — only authorized project's entities pass
async fn neighbors_indirect_auth_definition_via_project(ctx: &TestContext) {
    setup_test_data(ctx).await;
    setup_indirect_auth_data(ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // File 3000's outgoing neighbors: Definition 5000 and 5001 (via DEFINES edges)
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "f", "entity": "File", "node_ids": [3000]},
        "neighbors": {"node": "f", "direction": "outgoing", "rel_types": ["DEFINES"]}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(
        result.len(),
        2,
        "File 3000 should have 2 outgoing DEFINES neighbors (Definitions 5000, 5001)"
    );

    // --- Test 1: Authorize the owning Project 1000 → both neighbors pass ---
    let f = result.ctx().get("f").unwrap().clone();

    // Verify center node metadata before redaction.
    // File uses indirect auth — _gkg_f_id holds the owning Project ID (1000),
    // not the File's own ID (3000), because redaction resolves through the owner.
    for row in result.iter() {
        assert_eq!(
            row.get_id(&f),
            Some(1000),
            "center _gkg_f_id should be the owning Project ID for indirect-auth entities"
        );
        assert_eq!(row.get_type(&f), Some("File"), "center type should be File");
        let neighbor = row.neighbor_node().expect("neighbor should be present");
        assert_eq!(
            neighbor.entity_type, "Definition",
            "neighbor type should be Definition"
        );
    }

    let mut mock_service = MockRedactionService::new();
    // File and Definition both have resource_type "project", ability "read_code".
    // Only the owning Project ID is authorized — NOT the File/Definition IDs themselves.
    // This proves authorization resolves through the indirect owner.
    mock_service.allow("project", &[1000]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        redacted, 0,
        "all neighbors should pass when owning Project is authorized"
    );

    // Verify both neighbors survived with correct identity
    let authorized_neighbors: Vec<(i64, &str)> = result
        .authorized_rows()
        .filter_map(|r| r.neighbor_node().map(|n| (n.id, n.entity_type.as_str())))
        .collect();
    assert_eq!(authorized_neighbors.len(), 2);
    let neighbor_ids: HashSet<i64> = authorized_neighbors.iter().map(|(id, _)| *id).collect();
    assert_eq!(neighbor_ids, HashSet::from([5000, 5001]));
    for (_, entity_type) in &authorized_neighbors {
        assert_eq!(*entity_type, "Definition");
    }

    // --- Test 2: Deny the owning Project → everything denied ---
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // Confirm both Definitions are present before redaction
    let pre_deny_ids: HashSet<i64> = result
        .iter()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert_eq!(
        pre_deny_ids,
        HashSet::from([5000, 5001]),
        "before deny, both Definition neighbors should be present"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.deny("project", &[1000]);

    run_redaction(&mut result, &mock_service);

    // Both Definition neighbors should be denied — their owning Project is denied
    let post_deny_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert!(
        post_deny_ids.is_empty(),
        "Definitions 5000/5001 must not survive when owning Project 1000 is denied; got: {:?}",
        post_deny_ids
    );
    // Verify every row was individually marked unauthorized
    for row in result.rows() {
        assert!(
            !row.is_authorized(),
            "row with neighbor {:?} should be unauthorized",
            row.neighbor_node().map(|n| n.id)
        );
    }

    // --- Test 3: No authorization at all → fail-closed ---
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // Capture neighbor IDs before redaction so we can verify they're gone afterward
    let pre_redaction_neighbor_ids: HashSet<i64> = result
        .iter()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert_eq!(
        pre_redaction_neighbor_ids,
        HashSet::from([5000, 5001]),
        "before redaction, both Definition neighbors should be present"
    );

    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        redacted, 2,
        "fail-closed: no auth → all denied for indirect-auth entities"
    );
    assert_eq!(result.authorized_count(), 0);

    // Verify none of the pre-redaction neighbors survived
    let post_redaction_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert!(
        post_redaction_ids.is_empty(),
        "fail-closed must not leak any neighbor data; got: {:?}",
        post_redaction_ids
    );
}

/// PathFinding with indirect-auth entities where the owner is NOT in the path.
///
/// Path: File 3000 → (DEFINES) → Definition 5000.
/// Both authorize via Project 1000, but Project is not a node in the path.
/// PathFinding has no static _gkg_* nodes (enforce.rs line 107), so all nodes
/// are dynamic (from _gkg_path array) with their own entity IDs. find_owner_id
/// can only find the owner if it's actually in the path. Since File and
/// Definition store their own IDs (3000, 5000), not project_id, the owner
/// cannot be resolved → fail-closed.
async fn path_finding_indirect_auth_fail_closed_no_owner_in_path(ctx: &TestContext) {
    setup_test_data(ctx).await;
    setup_indirect_auth_data(ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "File", "node_ids": [3000]},
            {"id": "end", "entity": "Definition", "node_ids": [5000, 5001]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let raw_count = result.len();
    assert!(
        raw_count > 0,
        "should find paths from File 3000 to Definitions"
    );

    // Even though Project 1000 is authorized, the path nodes are File and
    // Definition — their dynamic node IDs (3000, 5000/5001) don't match any
    // authorized project ID, and find_owner_id can't locate the Project owner
    // because it's not in the path. Result: fail-closed.
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("project", &[1000]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(
        redacted, raw_count,
        "all paths must be denied: owner Project is not in the path, \
         so indirect-auth entities cannot resolve their auth ID"
    );
    assert_eq!(result.authorized_count(), 0);
}

/// Mixed indirect auth: neighbors from two different projects.
/// File 3000 (Project 1000) and File 3002 (Project 1001) both have
/// Definition neighbors. Authorizing only Project 1000 should filter
/// out Project 1001's definitions.
async fn neighbors_indirect_auth_mixed_projects(ctx: &TestContext) {
    setup_test_data(ctx).await;
    setup_indirect_auth_data(ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Query neighbors for both files
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "f", "entity": "File", "node_ids": [3000, 3002]},
        "neighbors": {"node": "f", "direction": "outgoing", "rel_types": ["DEFINES"]}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // File 3000 → Def 5000, 5001 (Project 1000)
    // File 3002 → Def 5003 (Project 1001)
    assert_eq!(
        result.len(),
        3,
        "should have 3 total neighbors across both files"
    );

    // Allow Project 1000, deny Project 1001
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("project", &[1000]);
    mock_service.deny("project", &[1001]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        2,
        "only Project 1000's definitions should pass"
    );

    // Verify the surviving neighbors are from Project 1000
    let neighbor_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert_eq!(
        neighbor_ids,
        HashSet::from([5000, 5001]),
        "only Definitions from authorized Project 1000 should remain"
    );

    // Definition 5003 (Project 1001) must not appear
    assert!(
        !neighbor_ids.contains(&5003),
        "Definition from denied Project 1001 must not appear"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge Column Subtests
// ─────────────────────────────────────────────────────────────────────────────
//
// These tests verify that edge columns (relationship metadata) are correctly
// returned in query results and preserved through the redaction flow.

/// Verifies edge columns are present and preserved through redaction.
///
/// Tests:
/// - Edge columns (e0_type, e0_src, e0_src_type, e0_dst, e0_dst_type) are in SQL
/// - Edge values correctly reflect the relationship data
/// - Edge columns are preserved in authorized rows after redaction
/// - Redacted rows still had valid edge data before being filtered
async fn traversal_edge_columns_preserved_through_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Verify edge columns are in the SQL
    assert!(
        query.base.sql.contains("e0_type"),
        "SQL must contain e0_type. SQL: {}",
        query.base.sql
    );
    assert!(query.base.sql.contains("e0_src"), "SQL must contain e0_src");
    assert!(
        query.base.sql.contains("e0_src_type"),
        "SQL must contain e0_src_type"
    );
    assert!(query.base.sql.contains("e0_dst"), "SQL must contain e0_dst");
    assert!(
        query.base.sql.contains("e0_dst_type"),
        "SQL must contain e0_dst_type"
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    // We have 7 MEMBER_OF edges in test data
    assert_eq!(result.len(), 7, "should have 7 user-group memberships");

    // Verify edge columns are present and correct BEFORE redaction
    for row in result.iter() {
        let user_id = row.get_id(&u).expect("user id should be present");
        let group_id = row.get_id(&g).expect("group id should be present");

        assert_eq!(
            row.get("e0_type")
                .and_then(|v| v.as_string().map(|s| s.as_str())),
            Some("MEMBER_OF"),
            "edge type should be MEMBER_OF"
        );
        assert_eq!(
            row.get("e0_src").and_then(|v| v.as_int64().copied()),
            Some(user_id),
            "edge source should match user id"
        );
        assert_eq!(
            row.get("e0_src_type")
                .and_then(|v| v.as_string().map(|s| s.as_str())),
            Some("User"),
            "edge source type should be User"
        );
        assert_eq!(
            row.get("e0_dst").and_then(|v| v.as_int64().copied()),
            Some(group_id),
            "edge target should match group id"
        );
        assert_eq!(
            row.get("e0_dst_type")
                .and_then(|v| v.as_string().map(|s| s.as_str())),
            Some("Group"),
            "edge target type should be Group"
        );
    }

    // Now apply redaction - allow only user 1 and group 100
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);

    let redacted = run_redaction(&mut result, &mock_service);

    // User 1 is member of groups 100 and 102, but only 100 is allowed
    assert_eq!(redacted, 6, "6 rows should be redacted");
    assert_eq!(result.authorized_count(), 1, "only 1 row should pass");

    // Verify unauthorized data is NOT present in authorized results
    let authorized_user_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    let authorized_group_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&g))
        .collect();

    // Unauthorized users (2, 3, 4, 5) must NOT appear
    for unauthorized_user in [2, 3, 4, 5] {
        assert!(
            !authorized_user_ids.contains(&unauthorized_user),
            "unauthorized user {} should NOT be in results",
            unauthorized_user
        );
    }

    // Unauthorized groups (101, 102) must NOT appear
    for unauthorized_group in [101, 102] {
        assert!(
            !authorized_group_ids.contains(&unauthorized_group),
            "unauthorized group {} should NOT be in results",
            unauthorized_group
        );
    }

    // Verify edge columns are preserved in the authorized row
    let authorized_row = result.authorized_rows().next().expect("should have 1 row");
    assert_eq!(authorized_row.get_id(&u), Some(1));
    assert_eq!(authorized_row.get_id(&g), Some(100));
    assert_eq!(
        authorized_row
            .get("e0_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("MEMBER_OF"),
        "edge type should be preserved after redaction"
    );
    assert_eq!(
        authorized_row
            .get("e0_src")
            .and_then(|v| v.as_int64().copied()),
        Some(1),
        "edge source should be user 1"
    );
    assert_eq!(
        authorized_row
            .get("e0_dst")
            .and_then(|v| v.as_int64().copied()),
        Some(100),
        "edge target should be group 100"
    );

    // Verify edge data for unauthorized entities is also not exposed
    let authorized_edge_sources: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get("e0_src").and_then(|v| v.as_int64().copied()))
        .collect();
    let authorized_edge_targets: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get("e0_dst").and_then(|v| v.as_int64().copied()))
        .collect();

    // Edge sources should only contain authorized user IDs
    assert_eq!(
        authorized_edge_sources,
        HashSet::from([1]),
        "edge sources should only contain authorized user 1"
    );

    // Edge targets should only contain authorized group IDs
    assert_eq!(
        authorized_edge_targets,
        HashSet::from([100]),
        "edge targets should only contain authorized group 100"
    );
}

/// Verifies multi-hop traversals have edge columns for each relationship,
/// and that edge data is correctly associated with its hop after redaction.
async fn multi_hop_edge_columns_survive_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project", "id_range": {"start": 1, "end": 10000}}
        ],
        "relationships": [
            {"type": "MEMBER_OF", "from": "u", "to": "g"},
            {"type": "CONTAINS", "from": "g", "to": "p"}
        ],
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Verify both edge column sets are in SQL
    assert!(
        query.base.sql.contains("e0_type"),
        "SQL must contain e0_type"
    );
    assert!(query.base.sql.contains("e0_src"), "SQL must contain e0_src");
    assert!(
        query.base.sql.contains("e1_type"),
        "SQL must contain e1_type"
    );
    assert!(query.base.sql.contains("e1_src"), "SQL must contain e1_src");

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let g = result.ctx().get("g").unwrap().clone();
    let p = result.ctx().get("p").unwrap().clone();
    let u = result.ctx().get("u").unwrap().clone();

    // Should have 12 paths total (see three_hop test for breakdown)
    assert_eq!(
        result.len(),
        12,
        "should have 12 user->group->project paths"
    );

    // Allow specific path: user 1 -> group 100 -> project 1000
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);
    mock_service.allow("project", &[1000]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 11, "11 rows should be redacted");
    assert_eq!(result.authorized_count(), 1, "only 1 path should pass");

    // Verify the surviving row has correct edge data for BOTH hops
    let row = result.authorized_rows().next().expect("should have 1 row");

    // Verify node IDs
    assert_eq!(row.get_id(&u), Some(1), "user should be 1");
    assert_eq!(row.get_id(&g), Some(100), "group should be 100");
    assert_eq!(row.get_id(&p), Some(1000), "project should be 1000");

    // First edge: User 1 -> Group 100 (MEMBER_OF)
    assert_eq!(
        row.get("e0_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("MEMBER_OF"),
        "first edge type should be MEMBER_OF"
    );
    assert_eq!(
        row.get("e0_src").and_then(|v| v.as_int64().copied()),
        Some(1),
        "e0 source should be user 1"
    );
    assert_eq!(
        row.get("e0_src_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("User"),
        "e0 source type should be User"
    );
    assert_eq!(
        row.get("e0_dst").and_then(|v| v.as_int64().copied()),
        Some(100),
        "e0 target should be group 100"
    );
    assert_eq!(
        row.get("e0_dst_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Group"),
        "e0 target type should be Group"
    );

    // Second edge: Group 100 -> Project 1000 (CONTAINS)
    assert_eq!(
        row.get("e1_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("CONTAINS"),
        "second edge type should be CONTAINS"
    );
    assert_eq!(
        row.get("e1_src").and_then(|v| v.as_int64().copied()),
        Some(100),
        "e1 source should be group 100"
    );
    assert_eq!(
        row.get("e1_src_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Group"),
        "e1 source type should be Group"
    );
    assert_eq!(
        row.get("e1_dst").and_then(|v| v.as_int64().copied()),
        Some(1000),
        "e1 target should be project 1000"
    );
    assert_eq!(
        row.get("e1_dst_type")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Project"),
        "e1 target type should be Project"
    );
}

/// Tests that neighbors query filters by entity type, preventing ID collisions.
///
/// This validates the fix for the bug where neighbors query would return edges
/// for unrelated entities that happen to share the same numeric ID.
/// For example, User 1's neighbors should not include edges where source_id=1
/// but source_kind='Group'.
async fn neighbors_query_filters_by_entity_type(ctx: &TestContext) {
    setup_test_data(ctx).await;

    // Insert a "colliding" edge: source_id=1 but source_kind='Group'
    // This simulates a Group with ID=1 having an edge, which should NOT
    // appear when querying User 1's neighbors.
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/', 1, 'Group', 'CONTAINS', 9999, 'Project')",
        table_edges()
    ))
    .await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Query User 1's outgoing neighbors
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Verify the SQL contains source_kind filter to prevent ID collisions
    // Note: the entity type 'User' is passed as a parameter, not embedded in SQL
    assert!(
        query.base.sql.contains("source_kind"),
        "neighbors query must filter by source_kind to prevent ID collisions. SQL: {}",
        query.base.sql
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // User 1 has exactly 2 MEMBER_OF edges (to groups 100 and 102)
    // The "colliding" edge (Group 1 -> Project 9999) should NOT appear
    assert_eq!(
        result.len(),
        2,
        "User 1 should have exactly 2 neighbors (groups 100, 102), not 3. \
         The edge with source_id=1, source_kind='Group' must be filtered out."
    );

    // Verify all neighbors are Groups (not the colliding Project 9999)
    for row in result.iter() {
        let neighbor_type = row
            .get("_gkg_neighbor_type")
            .and_then(|v| v.as_string().map(|s| s.as_str()));
        assert_eq!(
            neighbor_type,
            Some("Group"),
            "all neighbors should be Groups, got {:?}",
            neighbor_type
        );
    }

    // Verify redaction works correctly on filtered neighbors
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.allow("group", &[100]);
    mock_service.deny("group", &[102]);

    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        1,
        "only the edge to group 100 should be authorized after redaction"
    );

    let authorized_neighbor_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|row| {
            row.get("_gkg_neighbor_id")
                .and_then(|v| v.as_int64().copied())
        })
        .collect();
    assert_eq!(
        authorized_neighbor_ids,
        HashSet::from([100]),
        "only group 100 should remain after redaction"
    );
}

/// Comprehensive test for enum filter normalization across int and string enum types.
///
/// Tests the query normalization phase which coerces filter values to match ontology types:
/// - Int-based enums: integer filter values are coerced to string labels (e.g., 1 → "opened")
/// - String-based enums: string values pass through unchanged (no coercion needed)
///
/// This ensures the normalization layer correctly distinguishes between enum storage types
/// and only applies int→string coercion where appropriate.
async fn enum_filter_normalization_int_vs_string_enums(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // ─────────────────────────────────────────────────────────────────────────
    // PART 1: Int-based enum (User.user_type) - filter by int, coerced to string
    // ─────────────────────────────────────────────────────────────────────────
    // User.user_type has no enum_type in ontology (defaults to int-based).
    // Ontology values: 0=human, 6=project_bot, 11=service_account
    // Filter by int 0 should be coerced to "human" and match.

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "columns": ["user_type"], "filters": {"user_type": 0}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // We have 3 human users (alice, bob, charlie)
    assert_eq!(
        result.len(),
        3,
        "should find 3 human users when filtering by int 0 (coerced to 'human')"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);
    run_redaction(&mut result, &mock_service);

    // Verify the user_type values are the string labels
    for row in result.authorized_rows() {
        let user_type = row
            .get("u_user_type")
            .and_then(|v| v.as_string().map(|s| s.as_str()));
        assert_eq!(
            user_type,
            Some("human"),
            "user_type should be 'human' string"
        );
    }

    // Filter by int 6 should be coerced to "project_bot"
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "columns": ["user_type"], "filters": {"user_type": 6}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(
        result.len(),
        1,
        "should find 1 project_bot user when filtering by int 6"
    );

    // ─────────────────────────────────────────────────────────────────────────
    // PART 2: Int-based enum (MergeRequest.state) - filter by int, coerced to string
    // ─────────────────────────────────────────────────────────────────────────
    // MergeRequest.state has no enum_type (defaults to int-based).
    // Ontology values: 1=opened, 2=closed, 3=merged, 4=locked
    // Filter by int 1 should be coerced to "opened" and match.

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "mr", "entity": "MergeRequest", "columns": ["state"], "filters": {"state": 1}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // We have 2 opened MRs (2000, 2001)
    assert_eq!(
        result.len(),
        2,
        "should find 2 opened MRs when filtering by int 1 (coerced to 'opened')"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("merge_request", ALL_MR_IDS);
    run_redaction(&mut result, &mock_service);

    for row in result.authorized_rows() {
        let state = row
            .get("mr_state")
            .and_then(|v| v.as_string().map(|s| s.as_str()));
        assert_eq!(state, Some("opened"), "MR state should be 'opened' string");
    }

    // Filter by int 3 should be coerced to "merged"
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "mr", "entity": "MergeRequest", "columns": ["state"], "filters": {"state": 3}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(
        result.len(),
        1,
        "should find 1 merged MR when filtering by int 3"
    );

    // IN operator with int values on int-based enum
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "mr", "entity": "MergeRequest", "filters": {"state": {"op": "in", "value": [1, 2]}}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);

    // 2 opened + 1 closed = 3
    assert_eq!(
        result.len(),
        3,
        "should find 3 MRs with IN filter on int-based enum [1, 2]"
    );

    // ─────────────────────────────────────────────────────────────────────────
    // PART 3: String-based enum (User.state) - filter by string, no coercion
    // ─────────────────────────────────────────────────────────────────────────
    // User.state has enum_type: string in ontology.
    // String filters pass through unchanged - no int→string coercion.

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "columns": ["state"], "filters": {"state": "active"}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // 4 active users (alice, bob, charlie, diana)
    assert_eq!(
        result.len(),
        4,
        "should find 4 active users with string enum filter"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", ALL_USER_IDS);
    run_redaction(&mut result, &mock_service);

    for row in result.authorized_rows() {
        let state = row
            .get("u_state")
            .and_then(|v| v.as_string().map(|s| s.as_str()));
        assert_eq!(state, Some("active"), "state should be 'active' string");
    }

    // Filter blocked user (string enum value)
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "filters": {"state": "blocked"}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(
        result.len(),
        1,
        "should find 1 blocked user with string enum filter"
    );

    // IN operator with string enum values
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "filters": {"state": {"op": "in", "value": ["active", "blocked"]}}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let result = QueryResult::from_batches(&batches, &query.base.result_context);

    assert_eq!(
        result.len(),
        5,
        "should find all 5 users with IN filter on string enum"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Cursor Pagination Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn cursor_pagination_basic(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // 5 users total. cursor: offset=0, page_size=2 → first 2 users
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "order_by": {"node": "u", "property": "id", "direction": "ASC"},
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 2}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    assert!(
        query.base.sql.contains("LIMIT 100"),
        "SQL LIMIT should come from limit field, not cursor: {}",
        query.base.sql
    );

    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // All 5 users returned from ClickHouse
    assert_eq!(result.len(), 5, "ClickHouse should return all 5 users");

    // Apply cursor: slice to [0..2]
    let has_more = result.apply_cursor(0, 2);
    assert!(has_more);
    assert_eq!(result.authorized_count(), 2);

    let u = result.ctx().get("u").unwrap().clone();
    let page1_ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(page1_ids, vec![1, 2], "first page should be user IDs 1, 2");

    // cursor: offset=2, page_size=2 → users 3, 4
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let has_more = result.apply_cursor(2, 2);
    assert!(has_more);
    assert_eq!(result.authorized_count(), 2);

    let u = result.ctx().get("u").unwrap().clone();
    let page2_ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(page2_ids, vec![3, 4], "second page should be user IDs 3, 4");

    // cursor: offset=4, page_size=2 → user 5, has_more=false
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let has_more = result.apply_cursor(4, 2);
    assert!(!has_more, "last page should not have more");
    assert_eq!(result.authorized_count(), 1);

    let u = result.ctx().get("u").unwrap().clone();
    let last_ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(last_ids, vec![5], "last page should be user ID 5");
}

async fn cursor_pagination_with_redaction(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // 5 users, but redaction will deny 2 of them
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "order_by": {"node": "u", "property": "id", "direction": "ASC"},
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 2}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    // Redact users 2 and 4 → 3 authorized (1, 3, 5)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 3, 5]);
    mock_service.deny("user", &[2, 4]);
    run_redaction(&mut result, &mock_service);

    assert_eq!(
        result.authorized_count(),
        3,
        "3 users should survive redaction"
    );

    // Apply cursor on the authorized set: offset=0, page_size=2 → users 1, 3
    let has_more = result.apply_cursor(0, 2);
    assert!(has_more);
    assert_eq!(result.authorized_count(), 2);

    let u = result.ctx().get("u").unwrap().clone();
    let page_ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(
        page_ids,
        vec![1, 3],
        "cursor should slice the authorized (post-redaction) set, not the raw set"
    );
}

async fn cursor_pagination_offset_beyond_data(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 1000,
        "cursor": {"offset": 100, "page_size": 10}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let has_more = result.apply_cursor(100, 10);
    assert!(!has_more);
    assert_eq!(
        result.authorized_count(),
        0,
        "offset beyond data should return 0 rows"
    );
}

async fn cursor_pagination_with_filters(ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // 4 active users (IDs 1-4), 1 blocked (ID 5)
    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "filters": {"state": "active"}},
        "order_by": {"node": "u", "property": "id", "direction": "ASC"},
        "limit": 100,
        "cursor": {"offset": 0, "page_size": 2}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);

    let has_more = result.apply_cursor(0, 2);
    assert!(has_more);
    assert_eq!(result.authorized_count(), 2);

    let u = result.ctx().get("u").unwrap().clone();
    let ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(ids, vec![1, 2], "first page of filtered results");

    // Second page
    let batches = ctx.query_parameterized(&query.base).await;
    let mut result = QueryResult::from_batches(&batches, &query.base.result_context);
    let has_more = result.apply_cursor(2, 2);
    assert!(!has_more);

    let u = result.ctx().get("u").unwrap().clone();
    let ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(ids, vec![3, 4], "second page of filtered results");
}

// ─────────────────────────────────────────────────────────────────────────────
// MergeRequest Redaction Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn search_merge_requests_with_redaction(ctx: &TestContext) {
    let (_, mut result) = compile_and_execute(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "mr", "entity": "MergeRequest", "id_range": {"start": 1, "end": 10000}},
            "limit": 10
        }"#,
    )
    .await;
    let mr = result.ctx().get("mr").unwrap().clone();

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id(&mr)).collect();
    assert_eq!(
        raw_ids,
        ALL_MR_IDS.iter().copied().collect::<HashSet<_>>(),
        "should find all 4 MRs"
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("merge_request", &[2000, 2001]);
    mock_service.deny("merge_request", &[2002, 2003]);

    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 2, "MRs 2002 and 2003 should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&mr))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([2000, 2001]));
}

// ─────────────────────────────────────────────────────────────────────────────
// Order Preservation Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn redaction_preserves_row_order(ctx: &TestContext) {
    let (_, mut result) = compile_and_execute(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            "order_by": {"node": "u", "property": "id", "direction": "ASC"},
            "limit": 10
        }"#,
    )
    .await;
    let u = result.ctx().get("u").unwrap().clone();

    let raw_ids: Vec<i64> = result.iter().filter_map(|r| r.get_id(&u)).collect();
    assert_eq!(raw_ids, vec![1, 2, 3, 4, 5]);

    // Remove alternating rows: surviving rows must maintain original order
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1, 3, 5]);
    mock_service.deny("user", &[2, 4]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(
        authorized_ids,
        vec![1, 3, 5],
        "relative order must be preserved after redaction"
    );
}

async fn redaction_preserves_row_order_desc(ctx: &TestContext) {
    let (_, mut result) = compile_and_execute(
        ctx,
        r#"{
            "query_type": "traversal",
            "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
            "order_by": {"node": "u", "property": "id", "direction": "DESC"},
            "limit": 10
        }"#,
    )
    .await;
    let u = result.ctx().get("u").unwrap().clone();

    let raw_ids: Vec<i64> = result.iter().filter_map(|r| r.get_id(&u)).collect();
    assert_eq!(raw_ids, vec![5, 4, 3, 2, 1]);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[5, 3, 1]);
    mock_service.deny("user", &[4, 2]);

    run_redaction(&mut result, &mock_service);

    let authorized_ids: Vec<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id(&u))
        .collect();
    assert_eq!(
        authorized_ids,
        vec![5, 3, 1],
        "descending order must be preserved after redaction"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Empty Path Finding Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_no_path_exists_returns_empty(ctx: &TestContext) {
    let (_, mut result) = compile_and_execute(
        ctx,
        r#"{
            "query_type": "path_finding",
            "nodes": [
                {"id": "start", "entity": "User", "node_ids": [99999]},
                {"id": "end", "entity": "Project", "node_ids": [99999]}
            ],
            "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
        }"#,
    )
    .await;

    assert_eq!(
        result.len(),
        0,
        "ClickHouse should return 0 rows when no path exists"
    );

    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &mock_service);

    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 0);
    assert!(result.resource_checks().is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-Entity ID Collision Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn cross_entity_id_collision_redaction(ctx: &TestContext) {
    setup_test_data(ctx).await;

    // Insert a Group with ID=1 (same numeric ID as User 1)
    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) \
         VALUES (1, 'Collision Group', 'public', '1/1/')",
        table_groups()
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) \
         VALUES ('1/1/', 1, 'User', 'MEMBER_OF', 1, 'Group')",
        table_edges()
    ))
    .await;

    let (_, mut result) = compile_and_execute(
        ctx,
        r#"{
            "query_type": "traversal",
            "nodes": [
                {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
                {"id": "g", "entity": "Group"}
            ],
            "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
            "limit": 30
        }"#,
    )
    .await;
    let u = result.ctx().get("u").unwrap().clone();
    let g = result.ctx().get("g").unwrap().clone();

    let collision_exists = result
        .iter()
        .any(|r| r.get_id(&u) == Some(1) && r.get_id(&g) == Some(1));
    assert!(collision_exists, "should have row where User 1 → Group 1");

    // Allow User 1 but deny Group 1 — the (1, 1) row must be redacted
    // even though the numeric ID 1 is allowed for Users.
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("group", &[1]);
    mock_service.allow("group", &[100, 102]);

    run_redaction(&mut result, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id(&u)?, r.get_id(&g)?)))
        .collect();

    assert!(
        !authorized_pairs.contains(&(1, 1)),
        "User 1 → Group 1 must be denied: resource_type discrimination is required"
    );
    assert!(
        authorized_pairs.contains(&(1, 100)),
        "User 1 → Group 100 should pass"
    );
    assert!(
        authorized_pairs.contains(&(1, 102)),
        "User 1 → Group 102 should pass"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Orchestrator Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn redaction_integration() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    setup_test_data(&ctx).await;

    // Read-only subtests share one database (seed once, query many).
    run_subtests_shared!(
        &ctx,
        // basics
        fail_closed_no_authorization_returns_nothing,
        fail_closed_partial_authorization_denies_unknown_ids,
        fail_closed_explicit_deny_filters_row,
        single_hop_user_group_verifies_both_nodes,
        two_hop_denying_intermediate_group_filters_all_paths_through_it,
        three_hop_user_group_project_verifies_all_paths,
        three_hop_denying_one_project_removes_only_those_paths,
        group_project_two_hop_verifies_exact_pairs,
        single_node_project_query_verifies_all_projects,
        all_nodes_have_required_type_columns,
        empty_query_result_stays_empty,
        all_authorized_preserves_all_data,
        all_columns_preserved_after_redaction,
        all_columns_preserved_on_three_hop_traversal,
        redacted_rows_filtered_from_authorized_iterator,
        // path finding
        path_finding_extracts_all_nodes_from_path,
        path_finding_no_authorization_returns_nothing,
        path_finding_denying_intermediate_node_filters_path,
        path_finding_all_nodes_authorized_preserves_paths,
        path_finding_denying_start_node_filters_all_paths,
        path_finding_denying_end_node_filters_those_paths,
        path_finding_multiple_paths_independent_authorization,
        path_finding_shared_intermediate_node_authorization,
        path_finding_deep_traversal_all_nodes_verified,
        path_finding_all_paths_denied_returns_empty,
        path_finding_edge_kinds_preserved_through_redaction,
        // search
        search_with_complex_filters_and_redaction,
        search_projects_with_visibility_and_path_filters,
        search_groups_with_traversal_path_starts_with,
        search_with_id_range_filter,
        search_with_specific_node_ids,
        search_no_results_with_impossible_filter,
        search_fail_closed_no_authorization,
        search_preserves_metadata_columns_after_redaction,
        // column selection
        column_selection_specific_columns_includes_mandatory_columns,
        column_selection_wildcard_returns_all_columns_plus_mandatory,
        column_selection_omitted_includes_mandatory_columns,
        column_selection_multi_hop_traversal_all_nodes_have_mandatory_columns,
        column_selection_redaction_works_with_specific_columns,
        column_selection_fail_closed_on_any_unauthorized_node,
        column_selection_data_values_preserved_through_redaction,
        column_selection_id_in_list_no_duplication,
        column_selection_traversal_join_semantics_preserved,
        column_selection_filters_work_with_columns,
        column_selection_fail_closed_no_authorization,
        // neighbors and edges
        neighbors_query_comprehensive,
        neighbors_query_center_node_denied_filters_all,
        neighbors_query_multiple_center_nodes_mixed_authorization,
        neighbors_query_incoming_with_redaction,
        // edge columns
        traversal_edge_columns_preserved_through_redaction,
        multi_hop_edge_columns_survive_redaction,
        enum_filter_normalization_int_vs_string_enums,
        // cursor pagination
        cursor_pagination_basic,
        cursor_pagination_with_redaction,
        cursor_pagination_offset_beyond_data,
        cursor_pagination_with_filters,
        // merge request redaction
        search_merge_requests_with_redaction,
        // order preservation
        redaction_preserves_row_order,
        redaction_preserves_row_order_desc,
        // empty path finding
        path_finding_no_path_exists_returns_empty,
    );

    // Mutating subtests need their own forked databases.
    run_subtests!(
        &ctx,
        column_selection_aggregation_only_group_by_node_has_mandatory_columns,
        column_selection_aggregation_with_wildcard_columns,
        neighbors_indirect_auth_definition_via_project,
        path_finding_indirect_auth_fail_closed_no_owner_in_path,
        neighbors_indirect_auth_mixed_projects,
        neighbors_query_filters_by_entity_type,
        cross_entity_id_collision_redaction,
    );
}

//! E2E integration tests for the redaction flow.
//!
//! Tests verify fail-closed behavior: any node that cannot be explicitly verified
//! as authorized results in the entire row being filtered.

mod common;

use std::collections::{HashMap, HashSet};

use common::TestContext;
use gkg_server::redaction::{
    QueryResult, RedactionExtractor, ResourceAuthorization, ResourceCheck,
};
use ontology::Ontology;
use query_engine::{SecurityContext, compile};
use serial_test::serial;

fn load_ontology() -> Ontology {
    Ontology::load_embedded().expect("embedded ontology should load")
}

fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

struct MockRedactionService {
    authorizations: HashMap<String, HashMap<i64, bool>>,
}

impl MockRedactionService {
    fn new() -> Self {
        Self {
            authorizations: HashMap::new(),
        }
    }

    fn allow(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
        for id in ids {
            map.insert(*id, true);
        }
    }

    fn deny(&mut self, resource_type: &str, ids: &[i64]) {
        let map = self
            .authorizations
            .entry(resource_type.to_string())
            .or_default();
        for id in ids {
            map.insert(*id, false);
        }
    }

    fn check(&self, checks: &[ResourceCheck]) -> Vec<ResourceAuthorization> {
        checks
            .iter()
            .map(|check| {
                let authorized = check
                    .ids
                    .iter()
                    .map(|id| {
                        let allowed = self
                            .authorizations
                            .get(&check.resource_type)
                            .and_then(|m| m.get(id))
                            .copied()
                            .unwrap_or(false);
                        (*id, allowed)
                    })
                    .collect();

                ResourceAuthorization {
                    resource_type: check.resource_type.clone(),
                    authorized,
                }
            })
            .collect()
    }
}

const TABLE_USERS: &str = "gl_users";
const TABLE_GROUPS: &str = "gl_groups";
const TABLE_PROJECTS: &str = "gl_projects";
const TABLE_EDGES: &str = "gl_edges";

const ALL_USER_IDS: &[i64] = &[1, 2, 3, 4, 5];
const ALL_GROUP_IDS: &[i64] = &[100, 101, 102];
const ALL_PROJECT_IDS: &[i64] = &[1000, 1001, 1002, 1003, 1004];

async fn setup_test_data(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {TABLE_USERS} (id, username, name, state) VALUES
         (1, 'alice', 'Alice Admin', 'active'),
         (2, 'bob', 'Bob Builder', 'active'),
         (3, 'charlie', 'Charlie Private', 'active'),
         (4, 'diana', 'Diana Developer', 'active'),
         (5, 'eve', 'Eve External', 'blocked')"
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {TABLE_GROUPS} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/'),
         (102, 'Internal Group', 'internal', '1/102/')"
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {TABLE_PROJECTS} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/'),
         (1002, 'Internal Project', 'internal', '1/100/1002/'),
         (1003, 'Secret Project', 'private', '1/101/1003/'),
         (1004, 'Shared Project', 'public', '1/102/1004/')"
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {TABLE_EDGES} (source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         (1, 'User', 'MEMBER_OF', 100, 'Group'),
         (1, 'User', 'MEMBER_OF', 102, 'Group'),
         (2, 'User', 'MEMBER_OF', 100, 'Group'),
         (3, 'User', 'MEMBER_OF', 101, 'Group'),
         (4, 'User', 'MEMBER_OF', 101, 'Group'),
         (4, 'User', 'MEMBER_OF', 102, 'Group'),
         (5, 'User', 'MEMBER_OF', 101, 'Group'),
         (100, 'Group', 'CONTAINS', 1000, 'Project'),
         (100, 'Group', 'CONTAINS', 1002, 'Project'),
         (101, 'Group', 'CONTAINS', 1001, 'Project'),
         (101, 'Group', 'CONTAINS', 1003, 'Project'),
         (102, 'Group', 'CONTAINS', 1004, 'Project')"
    ))
    .await;
}

fn run_redaction(
    result: &mut QueryResult,
    ontology: &Ontology,
    mock_service: &MockRedactionService,
) -> usize {
    let extractor = RedactionExtractor::new(ontology);
    let (_nodes, checks) = extractor.extract(result);
    let authorizations = mock_service.check(&checks);
    let entity_map = extractor.entity_to_resource_map();
    result.apply_authorizations(&authorizations, &entity_map)
}

#[tokio::test]
#[serial]
async fn fail_closed_no_authorization_returns_nothing() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("u")).collect();
    assert_eq!(
        raw_ids,
        ALL_USER_IDS.iter().copied().collect::<HashSet<_>>(),
        "before redaction, all 5 users should be present"
    );

    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 5, "all 5 rows must be redacted");
    assert_eq!(result.authorized_count(), 0, "no rows should be authorized");
}

#[tokio::test]
#[serial]
async fn fail_closed_partial_authorization_denies_unknown_ids() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 5);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(
        redacted, 3,
        "users 3, 4, 5 should be redacted (not in allow list)"
    );
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2]));
    assert!(!authorized_ids.contains(&3));
    assert!(!authorized_ids.contains(&4));
    assert!(!authorized_ids.contains(&5));
}

#[tokio::test]
#[serial]
async fn fail_closed_explicit_deny_filters_row() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2, 3, 4]);
    mock_service.deny("users", &[5]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 1, "only user 5 should be redacted");
    assert_eq!(result.authorized_count(), 4);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2, 3, 4]));
    assert!(!authorized_ids.contains(&5));
}

#[tokio::test]
#[serial]
async fn single_hop_user_group_verifies_both_nodes() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User"},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_pairs: HashSet<(i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id("u")?, r.get_id("g")?)))
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
    mock_service.allow("users", &[1, 2]);
    mock_service.allow("groups", &[100]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id("u")?, r.get_id("g")?)))
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

#[tokio::test]
#[serial]
async fn two_hop_denying_intermediate_group_filters_all_paths_through_it() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User"},
            {"id": "g", "entity": "Group"}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);
    mock_service.allow("groups", &[100]);
    mock_service.deny("groups", &[101, 102]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id("u")?, r.get_id("g")?)))
        .collect();

    assert_eq!(
        authorized_pairs,
        HashSet::from([(1, 100), (2, 100)]),
        "only paths through group 100 should remain"
    );

    for row in result.authorized_rows() {
        let group_id = row.get_id("g").unwrap();
        assert_eq!(group_id, 100, "no denied groups should appear");
    }
}

#[tokio::test]
#[serial]
async fn three_hop_user_group_project_verifies_all_paths() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
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
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_paths: HashSet<(i64, i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id("u")?, r.get_id("g")?, r.get_id("p")?)))
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
    mock_service.allow("users", &[1, 2]);
    mock_service.allow("groups", &[100]);
    mock_service.allow("projects", &[1000]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_paths: HashSet<(i64, i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id("u")?, r.get_id("g")?, r.get_id("p")?)))
        .collect();

    assert_eq!(
        authorized_paths,
        HashSet::from([(1, 100, 1000), (2, 100, 1000)]),
        "only fully authorized paths should remain"
    );
}

#[tokio::test]
#[serial]
async fn three_hop_denying_one_project_removes_only_those_paths() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
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
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);
    mock_service.allow("groups", ALL_GROUP_IDS);
    mock_service.allow("projects", &[1000, 1002, 1004]);
    mock_service.deny("projects", &[1001, 1003]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_paths: HashSet<(i64, i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id("u")?, r.get_id("g")?, r.get_id("p")?)))
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
        let project_id = row.get_id("p").unwrap();
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

#[tokio::test]
#[serial]
async fn group_project_two_hop_verifies_exact_pairs() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_pairs: HashSet<(i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id("g")?, r.get_id("p")?)))
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
    mock_service.allow("groups", &[100, 102]);
    mock_service.deny("groups", &[101]);
    mock_service.allow("projects", &[1000, 1002, 1004]);
    mock_service.deny("projects", &[1001, 1003]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id("g")?, r.get_id("p")?)))
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

#[tokio::test]
#[serial]
async fn single_node_project_query_verifies_all_projects() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "p", "entity": "Project"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("p")).collect();
    assert_eq!(
        raw_ids,
        ALL_PROJECT_IDS.iter().copied().collect::<HashSet<_>>()
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("projects", &[1000, 1004]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("p"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1000, 1004]));
    assert!(!authorized_ids.contains(&1001));
    assert!(!authorized_ids.contains(&1002));
    assert!(!authorized_ids.contains(&1003));
}

#[tokio::test]
#[serial]
async fn all_nodes_have_required_type_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
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
            query.sql.contains(&format!("AS {col}")),
            "SQL must include {col}"
        );
    }

    let batches = ctx.query_parameterized(&query).await;
    let result = QueryResult::from_batches(&batches, &query.result_context);

    for row in result.iter() {
        assert_eq!(row.get_type("u"), Some("User"));
        assert_eq!(row.get_type("g"), Some("Group"));
        assert_eq!(row.get_type("p"), Some("Project"));
        assert!(row.get_id("u").is_some());
        assert!(row.get_id("g").is_some());
        assert!(row.get_id("p").is_some());
    }
}

#[tokio::test]
#[serial]
async fn empty_query_result_stays_empty() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User", "filters": {"username": "nonexistent"}}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 0);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 0);
}

#[tokio::test]
#[serial]
async fn all_authorized_preserves_all_data() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("groups", ALL_GROUP_IDS);
    mock_service.allow("projects", ALL_PROJECT_IDS);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(
        redacted, 0,
        "nothing should be redacted when all authorized"
    );
    assert_eq!(result.authorized_count(), raw_count);
}

#[tokio::test]
#[serial]
async fn all_columns_preserved_after_redaction() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group"},
            {"id": "p", "entity": "Project"}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

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
    mock_service.allow("groups", &[100]);
    mock_service.deny("groups", &[101, 102]);
    mock_service.allow("projects", &[1000, 1002]);
    mock_service.deny("projects", &[1001, 1003, 1004]);

    run_redaction(&mut result, &ontology, &mock_service);

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
        .find(|r| r.get_id("p") == Some(1000))
        .unwrap();
    assert_eq!(row_1000.get_id("g"), Some(100));
    assert_eq!(row_1000.get_type("g"), Some("Group"));
    assert_eq!(row_1000.get_type("p"), Some("Project"));

    let row_1002 = authorized
        .iter()
        .find(|r| r.get_id("p") == Some(1002))
        .unwrap();
    assert_eq!(row_1002.get_id("g"), Some(100));
    assert_eq!(row_1002.get_type("g"), Some("Group"));
    assert_eq!(row_1002.get_type("p"), Some("Project"));
}

#[tokio::test]
#[serial]
async fn all_columns_preserved_on_three_hop_traversal() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
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
        "limit": 30
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]);
    mock_service.allow("projects", &[1000]);

    run_redaction(&mut result, &ontology, &mock_service);

    assert!(result.authorized_count() > 0);

    for row in result.authorized_rows() {
        assert_eq!(row.get_id("u"), Some(1));
        assert_eq!(row.get_id("g"), Some(100));
        assert_eq!(row.get_id("p"), Some(1000));

        assert_eq!(row.get_type("u"), Some("User"));
        assert_eq!(row.get_type("g"), Some("Group"));
        assert_eq!(row.get_type("p"), Some("Project"));
    }
}

#[tokio::test]
#[serial]
async fn redacted_rows_filtered_from_authorized_iterator() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [{"id": "u", "entity": "User"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let all_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("u")).collect();
    assert_eq!(
        all_ids,
        ALL_USER_IDS.iter().copied().collect::<HashSet<_>>()
    );

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2]));
    assert!(!authorized_ids.contains(&3));
    assert!(!authorized_ids.contains(&4));
    assert!(!authorized_ids.contains(&5));

    let unauthorized_ids: HashSet<i64> = result
        .rows()
        .iter()
        .filter(|r| !r.is_authorized())
        .filter_map(|r| r.get_id("u"))
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
    use query_engine::ResultContext;
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

    let mut result = QueryResult::from_batches(&[batch], &ctx);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 3]);
    mock_service.allow("projects", &[100, 200, 300]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

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

/// Verifies fail-closed behavior: rows with NULL entity type must be denied.
#[test]
fn fail_closed_null_type_denies_row() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::ResultContext;
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

    let mut result = QueryResult::from_batches(&[batch], &ctx);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 1, "row with NULL type must be redacted");
    assert!(result.rows()[0].is_authorized());
    assert!(
        !result.rows()[1].is_authorized(),
        "NULL type row must be denied (fail-closed)"
    );
}

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
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
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
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
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
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
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
        "query_type": "search",
        "node": {"id": "p", "entity": "Project"},
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
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "filters": {"username": "nonexistent"}},
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
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
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

// ─────────────────────────────────────────────────────────────────────────────
// Path Finding Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn path_finding_extracts_all_nodes_from_path() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let result = QueryResult::from_batches(&batches, &query.result_context);

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
    }
}

#[tokio::test]
#[serial]
async fn path_finding_no_authorization_returns_nothing() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should find paths before redaction");

    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, raw_count, "all paths should be redacted");
    assert_eq!(result.authorized_count(), 0);
}

#[tokio::test]
#[serial]
async fn path_finding_denying_intermediate_node_filters_path() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should find paths");

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]); // Only allow group 100, deny 102
    mock_service.deny("groups", &[102]);
    mock_service.allow("projects", &[1000, 1002, 1004]);

    run_redaction(&mut result, &ontology, &mock_service);

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
}

#[tokio::test]
#[serial]
async fn path_finding_all_nodes_authorized_preserves_paths() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);
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
async fn path_finding_denying_start_node_filters_all_paths() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert!(!result.is_empty());

    let mut mock_service = MockRedactionService::new();
    mock_service.deny("users", &[1]); // Deny the start node
    mock_service.allow("groups", ALL_GROUP_IDS);
    mock_service.allow("projects", ALL_PROJECT_IDS);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, result.len(), "all paths should be redacted");
    assert_eq!(result.authorized_count(), 0);
}

#[tokio::test]
#[serial]
async fn path_finding_denying_end_node_filters_those_paths() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", ALL_GROUP_IDS);
    mock_service.allow("projects", &[1000]);
    mock_service.deny("projects", &[1002]); // Deny one end node

    run_redaction(&mut result, &ontology, &mock_service);

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
#[tokio::test]
#[serial]
async fn path_finding_multiple_paths_independent_authorization() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count >= 2, "should find paths to both projects");

    // Authorize the path through group 100 to project 1000 only
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]);
    mock_service.allow("projects", &[1000]);
    mock_service.deny("projects", &[1002]); // Deny one destination

    run_redaction(&mut result, &ontology, &mock_service);

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
#[tokio::test]
#[serial]
async fn path_finding_shared_intermediate_node_authorization() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count >= 2, "should find paths from both users");

    // Authorize user 1's path but deny user 2
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.deny("users", &[2]);
    mock_service.allow("groups", &[100]);
    mock_service.allow("projects", &[1000]);

    run_redaction(&mut result, &ontology, &mock_service);

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
#[tokio::test]
#[serial]
async fn path_finding_deep_traversal_all_nodes_verified() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should find some paths");

    // Authorize everything except intermediate group 102
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]); // Only group 100
    mock_service.deny("groups", &[102]); // Deny group 102
    mock_service.allow("projects", ALL_PROJECT_IDS);

    run_redaction(&mut result, &ontology, &mock_service);

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
#[tokio::test]
#[serial]
async fn path_finding_all_paths_denied_returns_empty() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert!(!result.is_empty(), "should have paths before redaction");

    // Deny ALL intermediate nodes - paths cannot complete
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.deny("groups", ALL_GROUP_IDS); // Deny all groups
    mock_service.allow("projects", &[1000]);

    run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(
        result.authorized_count(),
        0,
        "all paths should be denied when intermediates are denied"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Search Query Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
#[serial]
async fn search_with_complex_filters_and_redaction() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for active users whose names start with a letter in the first half of
    // the alphabet, using multiple filter operators simultaneously
    let json = r#"{
        "query_type": "search",
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
        !query.sql.contains("JOIN"),
        "search queries should not produce JOINs, got: {}",
        query.sql
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Should find alice, bob, charlie, diana (all active and in the username list)
    // eve is blocked so filtered out by the state filter
    let raw_usernames: Vec<i64> = result.iter().filter_map(|r| r.get_id("u")).collect();
    assert_eq!(
        raw_usernames.len(),
        4,
        "should find 4 active users matching filters"
    );

    // Now apply redaction: only allow users 1 (alice) and 2 (bob)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2]);
    mock_service.deny("users", &[3, 4]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 2, "charlie and diana should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1, 2]));
}

#[tokio::test]
#[serial]
async fn search_projects_with_visibility_and_path_filters() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for projects that are either public or internal
    let json = r#"{
        "query_type": "search",
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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Should find: 1000 (public), 1002 (internal), 1004 (public)
    // Not: 1001, 1003 (private)
    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("p")).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([1000, 1002, 1004]),
        "should find only public and internal projects"
    );

    // Redaction: allow only project 1000
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("projects", &[1000]);
    mock_service.deny("projects", &[1002, 1004]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("p"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1000]));
}

#[tokio::test]
#[serial]
async fn search_groups_with_traversal_path_starts_with() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for groups under the root namespace using traversal_path prefix
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "g",
            "entity": "Group",
            "filters": {
                "traversal_path": {"op": "starts_with", "value": "1/"}
            }
        },
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // All our test groups have paths starting with "1/"
    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("g")).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([100, 101, 102]),
        "should find all groups under root"
    );

    // Partial authorization
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("groups", &[100, 102]);
    mock_service.deny("groups", &[101]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("g"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([100, 102]));
}

#[tokio::test]
#[serial]
async fn search_with_id_range_filter() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for users with IDs in a specific range
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "id_range": {"start": 2, "end": 4}
        },
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("u")).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([2, 3, 4]),
        "should find users 2, 3, 4 within ID range"
    );

    // Full authorization for this range
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[2, 3, 4]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);
    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 3);
}

#[tokio::test]
#[serial]
async fn search_with_specific_node_ids() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for specific projects by ID
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "p",
            "entity": "Project",
            "node_ids": [1000, 1003]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_ids: HashSet<i64> = result.iter().filter_map(|r| r.get_id("p")).collect();
    assert_eq!(
        raw_ids,
        HashSet::from([1000, 1003]),
        "should find only the specified projects"
    );

    // Allow one, deny the other
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("projects", &[1000]);
    mock_service.deny("projects", &[1003]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("p"))
        .collect();

    assert_eq!(authorized_ids, HashSet::from([1000]));
}

#[tokio::test]
#[serial]
async fn search_no_results_with_impossible_filter() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Search for a user that doesn't exist
    let json = r#"{
        "query_type": "search",
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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 0, "should find no users");

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);
    assert_eq!(redacted, 0);
    assert_eq!(result.authorized_count(), 0);
}

#[tokio::test]
#[serial]
async fn search_fail_closed_no_authorization() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "g",
            "entity": "Group"
        },
        "limit": 100
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert_eq!(raw_count, 3, "should find all 3 groups");

    // No authorizations at all - fail closed
    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 3, "all groups should be redacted");
    assert_eq!(
        result.authorized_count(),
        0,
        "fail-closed: nothing authorized"
    );
}

#[tokio::test]
#[serial]
async fn search_preserves_metadata_columns_after_redaction() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "search",
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
        query.sql.contains("_gkg_u_id"),
        "SQL should include _gkg_u_id"
    );
    assert!(
        query.sql.contains("_gkg_u_type"),
        "SQL should include _gkg_u_type"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Check columns exist before redaction
    for row in result.iter() {
        assert!(
            row.get_id("u").is_some(),
            "ID should exist before redaction"
        );
        assert_eq!(row.get_type("u"), Some("User"), "type should be User");
    }

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);

    run_redaction(&mut result, &ontology, &mock_service);

    // Check columns still exist after redaction
    for row in result.authorized_rows() {
        assert_eq!(row.get_id("u"), Some(1));
        assert_eq!(row.get_type("u"), Some("User"));
    }
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

// ─────────────────────────────────────────────────────────────────────────────
// Column Selection Integration Tests
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
#[tokio::test]
#[serial]
async fn column_selection_specific_columns_includes_mandatory_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Request only username and state, but mandatory columns must still appear
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username", "state"]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // The generated SQL MUST contain the mandatory redaction columns
    assert!(
        query.sql.contains("_gkg_u_id"),
        "SQL must include _gkg_u_id for redaction. Got: {}",
        query.sql
    );
    assert!(
        query.sql.contains("_gkg_u_type"),
        "SQL must include _gkg_u_type for redaction. Got: {}",
        query.sql
    );

    // Also verify the requested columns are present
    assert!(
        query.sql.contains("u_username"),
        "SQL must include requested column u_username"
    );
    assert!(
        query.sql.contains("u_state"),
        "SQL must include requested column u_state"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 5, "should have all 5 users before redaction");

    // Run redaction with partial authorization
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2, 3]);
    mock_service.deny("users", &[4, 5]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 2, "users 4 and 5 should be redacted");
    assert_eq!(result.authorized_count(), 3);

    // Verify authorized rows have correct IDs and types
    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([1, 2, 3]));

    for row in result.authorized_rows() {
        assert_eq!(row.get_type("u"), Some("User"));
    }
}

/// Verify wildcard `"*"` returns all entity columns plus mandatory columns,
/// and redaction works correctly with all columns selected.
/// Uses Group entity which has all ontology columns present in the test schema.
#[tokio::test]
#[serial]
async fn column_selection_wildcard_returns_all_columns_plus_mandatory() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Use Group entity - all its ontology columns exist in gl_groups
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "g",
            "entity": "Group",
            "columns": "*"
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // CRITICAL: Mandatory columns must be present for redaction
    assert!(
        query.sql.contains("_gkg_g_id"),
        "wildcard must include _gkg_g_id for redaction"
    );
    assert!(
        query.sql.contains("_gkg_g_type"),
        "wildcard must include _gkg_g_type for redaction"
    );

    // Group entity columns from ontology
    assert!(
        query.sql.contains("g_id"),
        "wildcard should include g_id column"
    );
    assert!(
        query.sql.contains("g_name"),
        "wildcard should include g_name column"
    );
    assert!(
        query.sql.contains("g_visibility_level"),
        "wildcard should include g_visibility_level column"
    );
    assert!(
        query.sql.contains("g_traversal_path"),
        "wildcard should include g_traversal_path column"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 3, "should have all 3 groups before redaction");

    // Run redaction - allow only group 100 (Public Group)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("groups", &[100]);
    mock_service.deny("groups", &[101, 102]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 2, "groups 101 and 102 should be redacted");
    assert_eq!(result.authorized_count(), 1);

    // Verify the authorized row is group 100
    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized.len(), 1);
    assert_eq!(authorized[0].get_id("g"), Some(100));
    assert_eq!(authorized[0].get_type("g"), Some("Group"));
}

/// Verify omitting `columns` entirely still includes mandatory columns
/// and redaction works correctly.
#[tokio::test]
#[serial]
async fn column_selection_omitted_includes_mandatory_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // No columns specified - should still work for redaction
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Mandatory columns MUST be present even when columns is omitted
    assert!(
        query.sql.contains("_gkg_u_id"),
        "mandatory _gkg_u_id must be present when columns omitted"
    );
    assert!(
        query.sql.contains("_gkg_u_type"),
        "mandatory _gkg_u_type must be present when columns omitted"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 5, "should have all 5 users");

    // Run redaction - allow users 1, 2; deny the rest
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2]);
    mock_service.deny("users", &[3, 4, 5]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 3, "users 3, 4, 5 should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([1, 2]));

    for row in result.authorized_rows() {
        assert_eq!(row.get_type("u"), Some("User"));
    }
}

/// Deep test: Verify multi-hop traversal with different column selections
/// per node still includes mandatory columns for ALL nodes, and redaction
/// works correctly across the entire path.
///
/// This is the most complex case: User -> Group -> Project with different
/// column selections on each node. Redaction must verify authorization
/// for every node in the path.
#[tokio::test]
#[serial]
async fn column_selection_multi_hop_traversal_all_nodes_have_mandatory_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Three-hop traversal with mixed column selections:
    // - User: specific columns
    // - Group: specific columns (not wildcard to avoid missing columns)
    // - Project: specific columns
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "g", "entity": "Group", "columns": ["name"]},
            {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"]}
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
        query.sql.contains("_gkg_u_id"),
        "User node must have _gkg_u_id. SQL: {}",
        query.sql
    );
    assert!(
        query.sql.contains("_gkg_u_type"),
        "User node must have _gkg_u_type"
    );
    assert!(
        query.sql.contains("_gkg_g_id"),
        "Group node must have _gkg_g_id"
    );
    assert!(
        query.sql.contains("_gkg_g_type"),
        "Group node must have _gkg_g_type"
    );
    assert!(
        query.sql.contains("_gkg_p_id"),
        "Project node must have _gkg_p_id"
    );
    assert!(
        query.sql.contains("_gkg_p_type"),
        "Project node must have _gkg_p_type"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should have traversal results");

    // Run redaction: allow specific path (user 1 -> group 100 -> project 1000)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.deny("users", &[2, 3, 4, 5]);
    mock_service.allow("groups", &[100]);
    mock_service.deny("groups", &[101, 102]);
    mock_service.allow("projects", &[1000]);
    mock_service.deny("projects", &[1001, 1002, 1003, 1004]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert!(redacted > 0, "some paths should be redacted");

    // Only one path should remain: user 1 -> group 100 -> project 1000
    assert_eq!(
        result.authorized_count(),
        1,
        "only one path should be authorized"
    );

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized[0].get_id("u"), Some(1));
    assert_eq!(authorized[0].get_id("g"), Some(100));
    assert_eq!(authorized[0].get_id("p"), Some(1000));
    assert_eq!(authorized[0].get_type("u"), Some("User"));
    assert_eq!(authorized[0].get_type("g"), Some("Group"));
    assert_eq!(authorized[0].get_type("p"), Some("Project"));
}

/// Deep test: Verify redaction works correctly when using specific column selection.
/// Authorization checks depend on mandatory columns - if they were missing,
/// redaction would fail or behave incorrectly.
#[tokio::test]
#[serial]
async fn column_selection_redaction_works_with_specific_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username", "state"]},
            {"id": "g", "entity": "Group", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert!(raw_count > 0, "should have raw results");

    // Authorize only user 1 and group 100
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]);
    mock_service.deny("users", &[2, 3, 4, 5]);
    mock_service.deny("groups", &[101, 102]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    // Should have filtered out unauthorized rows
    assert!(redacted > 0, "some rows should be redacted");
    assert!(
        result.authorized_count() < raw_count,
        "authorized count should be less than raw"
    );

    // Verify only authorized combinations remain
    for row in result.authorized_rows() {
        let user_id = row.get_id("u").expect("user ID must exist after redaction");
        let group_id = row
            .get_id("g")
            .expect("group ID must exist after redaction");

        assert_eq!(user_id, 1, "only user 1 should be authorized");
        assert_eq!(group_id, 100, "only group 100 should be authorized");

        // Verify types are correct (used for redaction lookup)
        assert_eq!(row.get_type("u"), Some("User"));
        assert_eq!(row.get_type("g"), Some("Group"));
    }
}

/// Deep test: Verify that denying ANY node in a path filters the entire row,
/// even when using column selection. This ensures fail-closed behavior.
#[tokio::test]
#[serial]
async fn column_selection_fail_closed_on_any_unauthorized_node() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Three-hop query with column selection
    let json = r#"{
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
        "limit": 50
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Authorize user and group, but DENY the project
    // This should filter ALL rows because fail-closed
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);
    mock_service.allow("groups", ALL_GROUP_IDS);
    mock_service.deny("projects", ALL_PROJECT_IDS); // Deny all projects

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

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
#[tokio::test]
#[serial]
async fn column_selection_data_values_preserved_through_redaction() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username", "name", "state"],
            "filters": {"username": {"op": "in", "value": ["alice", "bob"]}}
        },
        "order_by": {"node": "u", "property": "username", "direction": "ASC"},
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Before redaction, verify we have data
    assert_eq!(result.len(), 2, "should find alice and bob");

    // Allow both users
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2]);

    run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(result.authorized_count(), 2);

    // Collect authorized rows and verify data integrity
    let authorized: Vec<_> = result.authorized_rows().collect();

    // Find alice (user 1) and verify her data
    let alice = authorized
        .iter()
        .find(|r| r.get_id("u") == Some(1))
        .unwrap();
    assert_eq!(alice.get_id("u"), Some(1));
    assert_eq!(alice.get_type("u"), Some("User"));

    // Find bob (user 2) and verify his data
    let bob = authorized
        .iter()
        .find(|r| r.get_id("u") == Some(2))
        .unwrap();
    assert_eq!(bob.get_id("u"), Some(2));
    assert_eq!(bob.get_type("u"), Some("User"));
}

/// Deep test: Verify that requesting the same column as a mandatory column
/// (e.g., "id" in the columns list) doesn't cause duplicates or errors,
/// and redaction still works correctly.
#[tokio::test]
#[serial]
async fn column_selection_id_in_list_no_duplication() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Explicitly request "id" alongside other columns
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "p",
            "entity": "Project",
            "columns": ["id", "name", "visibility_level"]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Should have mandatory columns plus requested columns (no duplicates)
    assert!(
        query.sql.contains("_gkg_p_id"),
        "mandatory _gkg_p_id must exist"
    );
    assert!(
        query.sql.contains("_gkg_p_type"),
        "mandatory _gkg_p_type must exist"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 5, "should have all 5 projects");

    // Run redaction - allow only public projects (1000, 1004)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("projects", &[1000, 1004]);
    mock_service.deny("projects", &[1001, 1002, 1003]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 3, "3 projects should be redacted");
    assert_eq!(result.authorized_count(), 2);

    let authorized_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("p"))
        .collect();
    assert_eq!(authorized_ids, HashSet::from([1000, 1004]));

    for row in result.authorized_rows() {
        assert_eq!(row.get_type("p"), Some("Project"));
    }
}

/// Deep test: Verify aggregation queries properly handle column selection
/// and redaction works on the group_by node.
/// Aggregations only add mandatory columns for the group_by node, not the target.
#[tokio::test]
#[serial]
async fn column_selection_aggregation_only_group_by_node_has_mandatory_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    // Insert some additional data for aggregation
    ctx.execute(
        "INSERT INTO gl_merge_requests (id, iid, title, state, traversal_path) VALUES
         (10001, 1, 'MR 1', 'merged', '1/100/1000/'),
         (10002, 2, 'MR 2', 'merged', '1/100/1000/'),
         (10003, 3, 'MR 3', 'open', '1/100/1000/')",
    )
    .await;

    ctx.execute(&format!(
        "INSERT INTO {TABLE_EDGES} (source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         (1, 'User', 'AUTHORED', 10001, 'MergeRequest'),
         (1, 'User', 'AUTHORED', 10002, 'MergeRequest'),
         (2, 'User', 'AUTHORED', 10003, 'MergeRequest')"
    ))
    .await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "aggregation",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username"]},
            {"id": "mr", "entity": "MergeRequest"}
        ],
        "relationships": [{"type": "AUTHORED", "from": "u", "to": "mr"}],
        "aggregations": [{"function": "count", "target": "mr", "group_by": "u", "alias": "mr_count"}],
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // User (group_by node) should have mandatory columns
    assert!(
        query.sql.contains("_gkg_u_id"),
        "group_by node must have _gkg_u_id"
    );
    assert!(
        query.sql.contains("_gkg_u_type"),
        "group_by node must have _gkg_u_type"
    );

    // MergeRequest (target node, being aggregated) should NOT have mandatory columns
    // because it doesn't appear as individual rows
    assert!(
        !query.sql.contains("_gkg_mr_id"),
        "aggregated target node should not have _gkg_mr_id"
    );
    assert!(
        !query.sql.contains("_gkg_mr_type"),
        "aggregated target node should not have _gkg_mr_type"
    );

    // Should have the aggregation
    assert!(query.sql.contains("COUNT"), "should have COUNT aggregation");
    assert!(
        query.sql.contains("GROUP BY"),
        "should have GROUP BY clause"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Should have 2 rows (user 1 with 2 MRs, user 2 with 1 MR)
    assert_eq!(result.len(), 2, "should have 2 aggregation rows");

    // Run redaction - only allow user 1
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.deny("users", &[2]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 1, "user 2's row should be redacted");
    assert_eq!(result.authorized_count(), 1);

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(authorized[0].get_id("u"), Some(1));
    assert_eq!(authorized[0].get_type("u"), Some("User"));
}

/// Deep test: Verify that column selection with traversal maintains proper
/// JOIN semantics. Rows should still match correctly across relationships.
#[tokio::test]
#[serial]
async fn column_selection_traversal_join_semantics_preserved() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Two-hop traversal with specific columns
    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "g", "entity": "Group", "columns": ["name", "visibility_level"]},
            {"id": "p", "entity": "Project", "columns": ["name", "visibility_level"]}
        ],
        "relationships": [{"type": "CONTAINS", "from": "g", "to": "p"}],
        "limit": 20
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Verify raw data matches expected relationships
    let raw_pairs: HashSet<(i64, i64)> = result
        .iter()
        .filter_map(|r| Some((r.get_id("g")?, r.get_id("p")?)))
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
    mock_service.allow("groups", &[100]);
    mock_service.allow("projects", &[1000, 1002]);

    run_redaction(&mut result, &ontology, &mock_service);

    let authorized_pairs: HashSet<(i64, i64)> = result
        .authorized_rows()
        .filter_map(|r| Some((r.get_id("g")?, r.get_id("p")?)))
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
#[tokio::test]
#[serial]
async fn column_selection_filters_work_with_columns() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Filter by state, but only select username
    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username"],
            "filters": {"state": "active"}
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();

    // Should have mandatory columns and requested column
    assert!(query.sql.contains("_gkg_u_id"));
    assert!(query.sql.contains("_gkg_u_type"));
    assert!(query.sql.contains("u_username"));

    // Filter by state should be in WHERE clause
    assert!(
        query.sql.contains("state") || query.sql.contains("WHERE"),
        "query should filter by state"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Should find 4 active users (eve is blocked)
    assert_eq!(result.len(), 4, "should find 4 active users");

    // Redaction should work
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1, 2, 3, 4]);

    run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(result.authorized_count(), 4);
    for row in result.authorized_rows() {
        assert!(row.get_id("u").is_some());
        assert_eq!(row.get_type("u"), Some("User"));
    }
}

/// Deep test: Ensure that column selection with no authorization
/// still exhibits fail-closed behavior.
#[tokio::test]
#[serial]
async fn column_selection_fail_closed_no_authorization() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "search",
        "node": {
            "id": "u",
            "entity": "User",
            "columns": ["username", "name", "state"]
        },
        "limit": 10
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert_eq!(raw_count, 5, "should have all 5 users");

    // No authorizations - fail closed
    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 5, "all users should be redacted (fail-closed)");
    assert_eq!(result.authorized_count(), 0, "nothing should be authorized");
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors Query Tests
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
#[tokio::test]
#[serial]
async fn neighbors_query_comprehensive() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
        query.sql.contains("_gkg_u_id"),
        "neighbors query must include _gkg_u_id for center node. SQL: {}",
        query.sql
    );
    assert!(
        query.sql.contains("_gkg_u_type"),
        "neighbors query must include _gkg_u_type"
    );

    // Verify neighbor columns are present
    assert!(
        query.sql.contains("_gkg_neighbor_id"),
        "must include _gkg_neighbor_id"
    );
    assert!(
        query.sql.contains("_gkg_neighbor_type"),
        "must include _gkg_neighbor_type"
    );
    assert!(
        query.sql.contains("_gkg_relationship_type"),
        "must include _gkg_relationship_type"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // User 1 is member of groups 100 and 102
    assert_eq!(
        result.len(),
        2,
        "user 1 should have 2 outgoing neighbors (groups 100, 102)"
    );

    // Verify center node metadata
    for row in result.iter() {
        assert_eq!(row.get_id("u"), Some(1));
        assert_eq!(row.get_type("u"), Some("User"));
        assert!(
            row.neighbor_node().is_some(),
            "neighbor node should be extracted"
        );
    }

    // --- Test 2: Fail-closed when NO authorization provided ---
    let mock_service = MockRedactionService::new();
    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(
        result.authorized_count(),
        0,
        "fail-closed with no authorization"
    );
    assert_eq!(redacted, 2, "all rows should be redacted");

    // --- Test 3: Fail-closed when only center node authorized (neighbors not authorized) ---
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]); // Only authorize center node

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    // Neighbors (groups 100, 102) are NOT authorized, so rows should be redacted
    assert_eq!(
        result.authorized_count(),
        0,
        "neighbors must also be authorized (fail-closed)"
    );
    assert_eq!(redacted, 2);

    // --- Test 4: Both center node AND neighbors authorized ---
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100, 102]); // Authorize both neighbor groups

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 0, "nothing redacted when all nodes authorized");
    assert_eq!(result.authorized_count(), 2);

    // Verify neighbor data is accessible
    let neighbor_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.neighbor_node().map(|n| n.id))
        .collect();
    assert_eq!(neighbor_ids, HashSet::from([100, 102]));

    // --- Test 5: Partial neighbor authorization filters specific rows ---
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]); // Only authorize group 100
    mock_service.deny("groups", &[102]); // Deny group 102

    run_redaction(&mut result, &ontology, &mock_service);

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
#[tokio::test]
#[serial]
async fn neighbors_query_center_node_denied_filters_all() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(result.len(), 2, "should have 2 neighbors before redaction");

    // Authorize neighbors but DENY center node
    let mut mock_service = MockRedactionService::new();
    mock_service.deny("users", &[1]);
    mock_service.allow("groups", ALL_GROUP_IDS);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 2, "all rows redacted when center node denied");
    assert_eq!(result.authorized_count(), 0);
}

/// Tests neighbors query with multiple center nodes and mixed authorization.
#[tokio::test]
#[serial]
async fn neighbors_query_multiple_center_nodes_mixed_authorization() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

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
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    let raw_count = result.len();
    assert_eq!(
        raw_count, 3,
        "should have 3 total neighbors (2 for user 1, 1 for user 3)"
    );

    // Authorize user 1 and its neighbors, deny user 3
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.deny("users", &[3]);
    mock_service.allow("groups", &[100, 102]); // User 1's neighbors
    mock_service.deny("groups", &[101]); // User 3's neighbor

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 1, "user 3's neighbor row should be redacted");
    assert_eq!(result.authorized_count(), 2);

    // Verify only user 1's neighbors remain
    let authorized_center_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();
    assert_eq!(authorized_center_ids, HashSet::from([1]));
}

/// Tests incoming direction with neighbor authorization.
#[tokio::test]
#[serial]
async fn neighbors_query_incoming_with_redaction() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // Find users who are members of group 100 (incoming MEMBER_OF edges)
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "g", "entity": "Group", "node_ids": [100]},
        "neighbors": {"node": "g", "direction": "incoming", "rel_types": ["MEMBER_OF"]}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Group 100 has incoming MEMBER_OF from users 1 and 2
    assert_eq!(result.len(), 2, "group 100 should have 2 incoming members");

    // Authorize center (group 100) and one neighbor (user 1)
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("groups", &[100]);
    mock_service.allow("users", &[1]);
    mock_service.deny("users", &[2]);

    run_redaction(&mut result, &ontology, &mock_service);

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
// Edge Column Tests
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
#[tokio::test]
#[serial]
async fn traversal_edge_columns_preserved_through_redaction() {
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

    // Verify edge columns are in the SQL
    assert!(
        query.sql.contains("e0_type"),
        "SQL must contain e0_type. SQL: {}",
        query.sql
    );
    assert!(query.sql.contains("e0_src"), "SQL must contain e0_src");
    assert!(
        query.sql.contains("e0_src_type"),
        "SQL must contain e0_src_type"
    );
    assert!(query.sql.contains("e0_dst"), "SQL must contain e0_dst");
    assert!(
        query.sql.contains("e0_dst_type"),
        "SQL must contain e0_dst_type"
    );

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // We have 7 MEMBER_OF edges in test data
    assert_eq!(result.len(), 7, "should have 7 user-group memberships");

    // Verify edge columns are present and correct BEFORE redaction
    for row in result.iter() {
        let user_id = row.get_id("u").expect("user id should be present");
        let group_id = row.get_id("g").expect("group id should be present");

        assert_eq!(
            row.get("e0_type").and_then(|v| v.as_str()),
            Some("MEMBER_OF"),
            "edge type should be MEMBER_OF"
        );
        assert_eq!(
            row.get("e0_src").and_then(|v| v.as_i64()),
            Some(user_id),
            "edge source should match user id"
        );
        assert_eq!(
            row.get("e0_src_type").and_then(|v| v.as_str()),
            Some("User"),
            "edge source type should be User"
        );
        assert_eq!(
            row.get("e0_dst").and_then(|v| v.as_i64()),
            Some(group_id),
            "edge target should match group id"
        );
        assert_eq!(
            row.get("e0_dst_type").and_then(|v| v.as_str()),
            Some("Group"),
            "edge target type should be Group"
        );
    }

    // Now apply redaction - allow only user 1 and group 100
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    // User 1 is member of groups 100 and 102, but only 100 is allowed
    assert_eq!(redacted, 6, "6 rows should be redacted");
    assert_eq!(result.authorized_count(), 1, "only 1 row should pass");

    // Verify unauthorized data is NOT present in authorized results
    let authorized_user_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("u"))
        .collect();
    let authorized_group_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get_id("g"))
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
    assert_eq!(authorized_row.get_id("u"), Some(1));
    assert_eq!(authorized_row.get_id("g"), Some(100));
    assert_eq!(
        authorized_row.get("e0_type").and_then(|v| v.as_str()),
        Some("MEMBER_OF"),
        "edge type should be preserved after redaction"
    );
    assert_eq!(
        authorized_row.get("e0_src").and_then(|v| v.as_i64()),
        Some(1),
        "edge source should be user 1"
    );
    assert_eq!(
        authorized_row.get("e0_dst").and_then(|v| v.as_i64()),
        Some(100),
        "edge target should be group 100"
    );

    // Verify edge data for unauthorized entities is also not exposed
    let authorized_edge_sources: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get("e0_src").and_then(|v| v.as_i64()))
        .collect();
    let authorized_edge_targets: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.get("e0_dst").and_then(|v| v.as_i64()))
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
#[tokio::test]
#[serial]
async fn multi_hop_edge_columns_survive_redaction() {
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

    // Verify both edge column sets are in SQL
    assert!(query.sql.contains("e0_type"), "SQL must contain e0_type");
    assert!(query.sql.contains("e0_src"), "SQL must contain e0_src");
    assert!(query.sql.contains("e1_type"), "SQL must contain e1_type");
    assert!(query.sql.contains("e1_src"), "SQL must contain e1_src");

    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // Should have 12 paths total (see three_hop test for breakdown)
    assert_eq!(
        result.len(),
        12,
        "should have 12 user->group->project paths"
    );

    // Allow specific path: user 1 -> group 100 -> project 1000
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", &[1]);
    mock_service.allow("groups", &[100]);
    mock_service.allow("projects", &[1000]);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);

    assert_eq!(redacted, 11, "11 rows should be redacted");
    assert_eq!(result.authorized_count(), 1, "only 1 path should pass");

    // Verify the surviving row has correct edge data for BOTH hops
    let row = result.authorized_rows().next().expect("should have 1 row");

    // Verify node IDs
    assert_eq!(row.get_id("u"), Some(1), "user should be 1");
    assert_eq!(row.get_id("g"), Some(100), "group should be 100");
    assert_eq!(row.get_id("p"), Some(1000), "project should be 1000");

    // First edge: User 1 -> Group 100 (MEMBER_OF)
    assert_eq!(
        row.get("e0_type").and_then(|v| v.as_str()),
        Some("MEMBER_OF"),
        "first edge type should be MEMBER_OF"
    );
    assert_eq!(
        row.get("e0_src").and_then(|v| v.as_i64()),
        Some(1),
        "e0 source should be user 1"
    );
    assert_eq!(
        row.get("e0_src_type").and_then(|v| v.as_str()),
        Some("User"),
        "e0 source type should be User"
    );
    assert_eq!(
        row.get("e0_dst").and_then(|v| v.as_i64()),
        Some(100),
        "e0 target should be group 100"
    );
    assert_eq!(
        row.get("e0_dst_type").and_then(|v| v.as_str()),
        Some("Group"),
        "e0 target type should be Group"
    );

    // Second edge: Group 100 -> Project 1000 (CONTAINS)
    assert_eq!(
        row.get("e1_type").and_then(|v| v.as_str()),
        Some("CONTAINS"),
        "second edge type should be CONTAINS"
    );
    assert_eq!(
        row.get("e1_src").and_then(|v| v.as_i64()),
        Some(100),
        "e1 source should be group 100"
    );
    assert_eq!(
        row.get("e1_src_type").and_then(|v| v.as_str()),
        Some("Group"),
        "e1 source type should be Group"
    );
    assert_eq!(
        row.get("e1_dst").and_then(|v| v.as_i64()),
        Some(1000),
        "e1 target should be project 1000"
    );
    assert_eq!(
        row.get("e1_dst_type").and_then(|v| v.as_str()),
        Some("Project"),
        "e1 target type should be Project"
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
#[tokio::test]
#[serial]
async fn enum_filter_normalization_int_vs_string_enums() {
    let ctx = TestContext::new().await;
    setup_test_data(&ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    // User.state is a string-based enum (enum_type: string in ontology).
    // String enum filters should pass through without coercion.
    // Filter by string value directly - should work.
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "filters": {"state": "active"}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let mut result = QueryResult::from_batches(&batches, &query.result_context);

    // We have 4 active users in test data (alice, bob, charlie, diana)
    assert_eq!(
        result.len(),
        4,
        "should find 4 active users with string enum filter"
    );

    // Authorize all users for this test
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("users", ALL_USER_IDS);

    let redacted = run_redaction(&mut result, &ontology, &mock_service);
    assert_eq!(
        redacted, 0,
        "no rows should be redacted when all authorized"
    );
    assert_eq!(
        result.authorized_count(),
        4,
        "4 active users should be authorized"
    );

    // Verify the state values in results are strings (not coerced from ints)
    for row in result.authorized_rows() {
        let state = row.get("u_state").and_then(|v| v.as_str());
        assert_eq!(state, Some("active"), "state should be 'active' string");
    }

    // Test filtering blocked user (string enum value)
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "filters": {"state": "blocked"}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let result = QueryResult::from_batches(&batches, &query.result_context);

    // We have 1 blocked user (eve)
    assert_eq!(
        result.len(),
        1,
        "should find 1 blocked user with string enum filter"
    );

    // Test IN operator with string enum values
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "filters": {"state": {"op": "in", "value": ["active", "blocked"]}}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let result = QueryResult::from_batches(&batches, &query.result_context);

    // All 5 users should match (4 active + 1 blocked)
    assert_eq!(
        result.len(),
        5,
        "should find all 5 users with IN filter on string enum"
    );

    // Verify string enum doesn't attempt int coercion:
    // If we accidentally passed an int (like 0 for 'active'), it should NOT match
    // because string enums don't coerce - the raw int would be compared against string values.
    // This query should return 0 results since 0 != "active".
    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User", "filters": {"state": 0}}
    }"#;

    let query = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&query).await;
    let result = QueryResult::from_batches(&batches, &query.result_context);

    assert_eq!(
        result.len(),
        0,
        "int value on string enum should not match (no coercion)"
    );
}

//! E2E integration tests for the hydration pipeline.
//!
//! Tests the full compile → execute → hydrate → format flow, verifying that
//! hydrated properties appear on NodeRef for Dynamic plans (PathFinding,
//! Neighbors) and on flat columns for Static plans (Traversal).

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, load_ontology,
    run_redaction, test_security_context,
};
use gkg_server::query_pipeline::{HydrationStage, PipelineObserver, RedactionOutput, row_to_json};
use gkg_server::redaction::QueryResult;
use integration_testkit::run_subtests;
use query_engine::{HydrationPlan, SecurityContext, compile};

async fn setup_test_data(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_group (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_project (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_merge_request (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'merged', 'fix-b', 'main', '1/101/1001/')",
    )
    .await;

    ctx.execute(
        "INSERT INTO gl_edge (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 2, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
         ('1/101/', 101, 'Group', 'CONTAINS', 1001, 'Project')",
    )
    .await;
}

/// Compile, execute base query, skip redaction, run hydration.
async fn compile_execute_hydrate(
    ctx: &TestContext,
    json: &str,
    ontology: &Arc<ontology::Ontology>,
    security_ctx: &SecurityContext,
    hydration_stage: &HydrationStage,
) -> (QueryResult, query_engine::ResultContext, HydrationPlan) {
    let compiled = compile(json, ontology, security_ctx).unwrap();
    let plan = compiled.hydration.clone();

    let batches = ctx.query_parameterized(&compiled.base).await;
    let result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let redaction_output = RedactionOutput {
        query_result: result,
        redacted_count: 0,
    };

    let mut obs = PipelineObserver::start();
    let output = hydration_stage
        .execute(
            redaction_output,
            &compiled.hydration,
            security_ctx,
            &mut obs,
        )
        .await
        .expect("hydration should succeed");

    (output.query_result, output.result_context, plan)
}

/// Compile, execute, redact, then hydrate — the actual production flow.
async fn compile_execute_redact_hydrate(
    ctx: &TestContext,
    json: &str,
    ontology: &Arc<ontology::Ontology>,
    security_ctx: &SecurityContext,
    hydration_stage: &HydrationStage,
    mock_service: &MockRedactionService,
) -> (QueryResult, query_engine::ResultContext, usize) {
    let compiled = compile(json, ontology, security_ctx).unwrap();

    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let redacted_count = run_redaction(&mut result, mock_service);

    let redaction_output = RedactionOutput {
        query_result: result,
        redacted_count,
    };

    let mut obs = PipelineObserver::start();
    let output = hydration_stage
        .execute(
            redaction_output,
            &compiled.hydration,
            security_ctx,
            &mut obs,
        )
        .await
        .expect("hydration should succeed");

    (output.query_result, output.result_context, redacted_count)
}

fn make_hydration_stage(ctx: &TestContext) -> (Arc<ontology::Ontology>, HydrationStage) {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());
    let stage = HydrationStage::new(ontology.clone(), client);
    (ontology, stage)
}

// ─────────────────────────────────────────────────────────────────────────────
// PathFinding Hydration
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_dynamic_hydration(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let (result, _ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    assert!(
        matches!(plan, HydrationPlan::Dynamic),
        "PathFinding should produce Dynamic plan"
    );
    assert!(!result.is_empty(), "should find at least one path");

    for row in result.authorized_rows() {
        let path_nodes = row.path_nodes();
        assert!(path_nodes.len() >= 2, "path should have start and end");

        assert_eq!(path_nodes[0].id, 1);
        assert_eq!(path_nodes[0].entity_type, "User");
        assert!(
            !path_nodes[0].properties.is_empty(),
            "User node should have hydrated properties"
        );

        let last = path_nodes.last().unwrap();
        assert_eq!(last.id, 1000);
        assert_eq!(last.entity_type, "Project");
        assert!(
            !last.properties.is_empty(),
            "Project node should have hydrated properties"
        );

        if path_nodes.len() >= 3 {
            let mid = &path_nodes[1];
            assert_eq!(mid.entity_type, "Group");
            assert!(
                !mid.properties.is_empty(),
                "Group node should have hydrated properties"
            );
        }

        let edge_kinds = row.edge_kinds();
        assert_eq!(edge_kinds.len(), path_nodes.len() - 1);

        if path_nodes.len() == 3 {
            assert_eq!(edge_kinds[0], "MEMBER_OF");
            assert_eq!(edge_kinds[1], "CONTAINS");
        }
    }
}

async fn path_finding_hydrated_property_values(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let (result, _ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    let row = result.authorized_rows().next().expect("should have a path");
    let path_nodes = row.path_nodes();

    // User 1 = alice
    let user_props = &path_nodes[0].properties;
    assert_eq!(
        user_props.get("username").and_then(|v| v.as_str()),
        Some("alice"),
        "User 1 username should be 'alice'"
    );
    assert_eq!(
        user_props.get("name").and_then(|v| v.as_str()),
        Some("Alice Admin"),
        "User 1 name should be 'Alice Admin'"
    );

    // Project 1000 = Public Project
    let project = path_nodes.last().unwrap();
    let project_props = &project.properties;
    assert_eq!(
        project_props.get("name").and_then(|v| v.as_str()),
        Some("Public Project"),
        "Project 1000 name should be 'Public Project'"
    );

    // Group 100 (intermediate)
    if path_nodes.len() == 3 {
        let group_props = &path_nodes[1].properties;
        assert_eq!(
            group_props.get("name").and_then(|v| v.as_str()),
            Some("Public Group"),
            "Group 100 name should be 'Public Group'"
        );
    }
}

async fn path_finding_json_format(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let (result, ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    let row = result.authorized_rows().next().unwrap();
    let json_val = row_to_json(row, &ctx_ref);
    let obj = json_val.as_object().unwrap();

    let path = obj
        .get("path")
        .expect("PathFinding JSON should have 'path' key")
        .as_array()
        .expect("'path' should be an array");
    assert!(path.len() >= 2);

    let first = path[0].as_object().unwrap();
    assert_eq!(first.get("id").unwrap().as_i64().unwrap(), 1);
    assert_eq!(first.get("entity_type").unwrap().as_str().unwrap(), "User");
    assert!(
        first.len() > 2,
        "path node should have properties beyond id/entity_type, keys: {:?}",
        first.keys().collect::<Vec<_>>()
    );

    let edges = obj
        .get("edges")
        .expect("PathFinding JSON should have 'edges' key")
        .as_array()
        .expect("'edges' should be an array");
    assert_eq!(edges.len(), path.len() - 1, "one edge per hop");

    if edges.len() == 2 {
        assert_eq!(edges[0].as_str().unwrap(), "MEMBER_OF");
        assert_eq!(edges[1].as_str().unwrap(), "CONTAINS");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Neighbors Hydration
// ─────────────────────────────────────────────────────────────────────────────

async fn neighbors_dynamic_hydration(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, _ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    assert!(matches!(plan, HydrationPlan::Dynamic));
    assert!(!result.is_empty(), "should find neighbors");

    for row in result.authorized_rows() {
        let neighbor = row.neighbor_node().expect("should have neighbor");
        assert_eq!(neighbor.entity_type, "Group");
        assert!(
            !neighbor.properties.is_empty(),
            "neighbor should have hydrated properties"
        );
    }
}

async fn neighbors_hydrated_property_values(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, _ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    // User 1 is member of Group 100 ("Public Group")
    let neighbor_names: HashSet<&str> = result
        .authorized_rows()
        .filter_map(|r| {
            r.neighbor_node()
                .and_then(|n| n.properties.get("name")?.as_str())
        })
        .collect();

    assert!(
        neighbor_names.contains("Public Group"),
        "should find 'Public Group' in neighbor properties, got: {:?}",
        neighbor_names
    );
}

async fn neighbors_json_format(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    let row = result.authorized_rows().next().unwrap();
    let json_val = row_to_json(row, &ctx_ref);
    let obj = json_val.as_object().unwrap();

    assert!(
        !obj.contains_key("path"),
        "neighbors should not have 'path'"
    );

    let has_hydrated_prop = obj
        .keys()
        .any(|k| !k.starts_with("_gkg_") && k != "u_id" && k != "u_type");
    assert!(
        has_hydrated_prop,
        "neighbors JSON should have hydrated top-level properties, keys: {:?}",
        obj.keys().collect::<Vec<_>>()
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Hydration Plan Selection
// ─────────────────────────────────────────────────────────────────────────────

async fn search_produces_no_hydration_plan(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "search",
        "node": {"id": "u", "entity": "User"},
        "limit": 10
    }"#;

    let compiled = compile(json, &ontology, &security_ctx).unwrap();
    assert!(
        matches!(compiled.hydration, HydrationPlan::None),
        "Search should produce None (static hydration disabled), got: {:?}",
        compiled.hydration
    );
}

async fn traversal_produces_no_hydration_plan(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "columns": ["username", "name"]},
            {"id": "g", "entity": "Group", "columns": ["name", "visibility_level"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let compiled = compile(json, &ontology, &security_ctx).unwrap();
    assert!(
        matches!(compiled.hydration, HydrationPlan::None),
        "Traversal should produce None (static hydration disabled), got: {:?}",
        compiled.hydration
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Full Pipeline: Redact → Hydrate
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_hydration_after_partial_redaction(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    // Two paths exist: User 1 → Group 100 → Project 1000
    //                  User 2 → Group 101 → Project 1001
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1, 2]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1001]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    // Allow User 1's path, deny User 2
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[1]);
    mock_service.deny("user", &[2]);
    mock_service.allow("group", &[100, 101]);
    mock_service.allow("project", &[1000, 1001]);

    let (result, _ctx_ref, redacted_count) = compile_execute_redact_hydrate(
        ctx,
        json,
        &ontology,
        &security_ctx,
        &hydration_stage,
        &mock_service,
    )
    .await;

    assert!(redacted_count > 0, "some paths should have been redacted");
    assert!(
        result.authorized_count() > 0,
        "some paths should survive redaction"
    );

    // Surviving paths should still have hydrated properties
    for row in result.authorized_rows() {
        let path_nodes = row.path_nodes();
        assert!(path_nodes.len() >= 2);

        // Start node must be User 1 (User 2 was denied)
        assert_eq!(path_nodes[0].id, 1);
        assert_eq!(path_nodes[0].entity_type, "User");
        assert!(
            !path_nodes[0].properties.is_empty(),
            "surviving path start node should be hydrated"
        );

        // Verify actual property value on surviving path
        assert_eq!(
            path_nodes[0]
                .properties
                .get("username")
                .and_then(|v| v.as_str()),
            Some("alice"),
            "hydrated User 1 should have username 'alice'"
        );

        let last = path_nodes.last().unwrap();
        assert!(
            !last.properties.is_empty(),
            "surviving path end node should be hydrated"
        );
    }

    // User 2's paths must not appear
    let start_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().first().map(|n| n.id))
        .collect();
    assert!(
        !start_ids.contains(&2),
        "denied User 2's paths must not appear after redaction + hydration"
    );
}

async fn neighbors_hydration_after_partial_redaction(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    // User 2's outgoing neighbors: Group 101 ("Private Group")
    // User 3's outgoing neighbors: Group 101 ("Private Group")
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [2, 3]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    // Allow User 2, deny User 3; allow the neighbor group
    let mut mock_service = MockRedactionService::new();
    mock_service.allow("user", &[2]);
    mock_service.deny("user", &[3]);
    mock_service.allow("group", &[101]);

    let (result, _ctx_ref, redacted_count) = compile_execute_redact_hydrate(
        ctx,
        json,
        &ontology,
        &security_ctx,
        &hydration_stage,
        &mock_service,
    )
    .await;

    assert!(redacted_count > 0, "User 3's row should be redacted");
    assert_eq!(
        result.authorized_count(),
        1,
        "only User 2's row should survive"
    );

    let row = result.authorized_rows().next().unwrap();
    let neighbor = row.neighbor_node().expect("should have neighbor");

    assert_eq!(neighbor.entity_type, "Group");
    assert_eq!(neighbor.id, 101);

    // Hydrated properties should be present on the surviving neighbor
    assert!(
        !neighbor.properties.is_empty(),
        "surviving neighbor should be hydrated after redaction"
    );
    assert_eq!(
        neighbor.properties.get("name").and_then(|v| v.as_str()),
        Some("Private Group"),
        "surviving neighbor should have correct hydrated name"
    );
}

async fn path_finding_all_denied_then_hydrate(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let (ontology, hydration_stage) = make_hydration_stage(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    // Deny everything
    let mock_service = MockRedactionService::new();

    let (result, _ctx_ref, redacted_count) = compile_execute_redact_hydrate(
        ctx,
        json,
        &ontology,
        &security_ctx,
        &hydration_stage,
        &mock_service,
    )
    .await;

    assert!(redacted_count > 0, "should have had paths to redact");
    assert_eq!(
        result.authorized_count(),
        0,
        "no rows should survive after full denial"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn hydration_integration() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(
        &ctx,
        // path finding hydration
        path_finding_dynamic_hydration,
        path_finding_hydrated_property_values,
        path_finding_json_format,
        // neighbors hydration
        neighbors_dynamic_hydration,
        neighbors_hydrated_property_values,
        neighbors_json_format,
        // hydration plan selection
        search_produces_no_hydration_plan,
        traversal_produces_no_hydration_plan,
        // full pipeline: redact then hydrate
        path_finding_hydration_after_partial_redaction,
        neighbors_hydration_after_partial_redaction,
        path_finding_all_denied_then_hydrate,
    );
}

//! E2E integration tests for the hydration pipeline.
//!
//! Tests the full compile → execute → hydrate → format flow, verifying that
//! hydrated properties appear on NodeRef for Dynamic plans (PathFinding,
//! Neighbors) and on flat columns for Static plans (Traversal).
//! Redaction is skipped — all rows are treated as authorized.

mod common;

use std::sync::Arc;

use common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext};
use gkg_server::query_pipeline::{HydrationStage, PipelineObserver, RedactionOutput, row_to_json};
use gkg_server::redaction::QueryResult;
use integration_testkit::run_subtests;
use ontology::Ontology;
use query_engine::{HydrationPlan, SecurityContext, compile};

fn load_ontology() -> Ontology {
    Ontology::load_embedded().expect("embedded ontology should load")
}

fn test_security_context() -> SecurityContext {
    SecurityContext::new(1, vec!["1/".into()]).expect("valid security context")
}

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

/// Helper: compile, execute base query, skip redaction, run hydration.
async fn compile_execute_hydrate(
    ctx: &TestContext,
    json: &str,
    ontology: &Arc<Ontology>,
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

// ─────────────────────────────────────────────────────────────────────────────
// Subtests
// ─────────────────────────────────────────────────────────────────────────────

async fn hydration_full_pipeline(ctx: &TestContext) {
    setup_test_data(ctx).await;

    let ontology = Arc::new(load_ontology());
    let security_ctx = test_security_context();
    let client = Arc::new(ctx.create_client());
    let hydration_stage = HydrationStage::new(ontology.clone(), client);

    // ── PathFinding: Dynamic hydration ──────────────────────────────────
    // Path: User 1 → Group 100 → Project 1000
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let (result, ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &hydration_stage).await;

    assert!(
        matches!(plan, HydrationPlan::Dynamic),
        "PathFinding should produce Dynamic plan"
    );
    assert!(!result.is_empty(), "should find at least one path");

    // Every node in every path should have hydrated properties
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
    }

    // Formatter should emit a "path" array with properties
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

    // ── Neighbors: Dynamic hydration ────────────────────────────────────
    // User 1's outgoing neighbors → Group 100
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, ctx_ref, plan) =
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

    // Formatter should merge neighbor properties as top-level keys
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

    // ── Search: No hydration (base query already carries all columns) ──
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

    // ── Traversal: No hydration (static disabled, base query has columns) ──
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
// Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn hydration_integration() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    run_subtests!(&ctx, hydration_full_pipeline,);
}

//! E2E integration tests for the hydration pipeline.
//!
//! Tests the full compile → execute → hydrate → format flow, verifying that
//! hydrated properties appear on NodeRef for Dynamic plans (PathFinding,
//! Neighbors) and on flat columns for Static plans (Traversal).

use std::collections::HashSet;
use std::sync::Arc;

use crate::common::{
    GRAPH_SCHEMA_SQL, MockRedactionService, SIPHON_SCHEMA_SQL, TestContext, load_ontology,
    run_redaction, test_security_context,
};
use gkg_server::pipeline::HydrationStage;
use gkg_server::redaction::QueryResult;
use integration_testkit::{run_subtests_shared, t};
use query_engine::compiler::{HydrationPlan, SecurityContext, compile};
use query_engine::formatters::row_to_json;
use query_engine::pipeline::{NoOpObserver, PipelineStage, QueryPipelineContext, TypeMap};
use query_engine::shared::RedactionOutput;

async fn setup_test_data(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state, user_type) VALUES
         (1, 'alice', 'Alice Admin', 'active', 'human'),
         (2, 'bob', 'Bob Builder', 'active', 'human'),
         (3, 'charlie', 'Charlie Private', 'active', 'human')",
        t("gl_user")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (100, 'Public Group', 'public', '1/100/'),
         (101, 'Private Group', 'private', '1/101/')",
        t("gl_group")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, name, visibility_level, traversal_path) VALUES
         (1000, 'Public Project', 'public', '1/100/1000/'),
         (1001, 'Private Project', 'private', '1/101/1001/')",
        t("gl_project")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, iid, title, state, source_branch, target_branch, traversal_path) VALUES
         (2000, 1, 'Add feature A', 'opened', 'feature-a', 'main', '1/100/1000/'),
         (2001, 2, 'Fix bug B', 'merged', 'fix-b', 'main', '1/101/1001/')",
        t("gl_merge_request")
    ))
    .await;

    // Source code entities -- File and Definition have redaction id_column = project_id,
    // so their PK (_gkg_f_pk) differs from the auth ID (_gkg_f_id).
    ctx.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, path, name, extension, language) VALUES
         (5001, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'lib.rs', 'rs', 'Rust'),
         (5002, '1/100/1000/', 1000, 'main', 'src/main.rs', 'main.rs', 'rs', 'Rust')",
        t("gl_file")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (id, traversal_path, project_id, branch, file_path, fqn, name, definition_type, start_line, end_line, start_byte, end_byte, start_char, end_char) VALUES
         (6001, '1/100/1000/', 1000, 'main', 'src/lib.rs', 'lib::greet', 'greet', 'function', 1, 5, 0, 80, 0, 0),
         (6002, '1/100/1000/', 1000, 'main', 'src/main.rs', 'main', 'main', 'function', 1, 10, 0, 120, 0, 0)",
        t("gl_definition")
    ))
    .await;

    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/', 1, 'User', 'MEMBER_OF', 100, 'Group'),
         ('1/101/', 2, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/101/', 3, 'User', 'MEMBER_OF', 101, 'Group'),
         ('1/100/', 100, 'Group', 'CONTAINS', 1000, 'Project'),
         ('1/101/', 101, 'Group', 'CONTAINS', 1001, 'Project')",
        t("gl_edge")
    ))
    .await;

    let ontology = load_ontology();
    ctx.execute(&format!(
        "INSERT INTO {} (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind) VALUES
         ('1/100/1000/', 5001, 'File', 'DEFINES', 6001, 'Definition'),
         ('1/100/1000/', 5002, 'File', 'DEFINES', 6002, 'Definition')",
        ontology.edge_table_for_relationship("DEFINES")
    ))
    .await;

    ctx.optimize_all().await;
}

/// Compile, execute base query, skip redaction, run hydration.
async fn compile_execute_hydrate(
    ctx: &TestContext,
    json: &str,
    ontology: &Arc<ontology::Ontology>,
    security_ctx: &SecurityContext,
    client: &Arc<clickhouse_client::ArrowClickHouseClient>,
) -> (
    QueryResult,
    query_engine::compiler::ResultContext,
    HydrationPlan,
) {
    let compiled = compile(json, ontology, security_ctx).unwrap();
    let plan = compiled.hydration.clone();

    let batches = ctx.query_parameterized(&compiled.base).await;
    let result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let redaction_output = RedactionOutput {
        query_result: result,
        redacted_count: 0,
    };

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::clone(client));
    let mut pipeline_ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(Arc::new(compiled)),
        ontology: Arc::clone(ontology),
        security_context: Some(security_ctx.clone()),
        server_extensions,
        phases: TypeMap::default(),
    };
    pipeline_ctx.phases.insert(redaction_output);
    let mut obs = NoOpObserver;

    let output = HydrationStage
        .execute(&mut pipeline_ctx, &mut obs)
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
    client: &Arc<clickhouse_client::ArrowClickHouseClient>,
    mock_service: &MockRedactionService,
) -> (QueryResult, query_engine::compiler::ResultContext, usize) {
    let compiled = compile(json, ontology, security_ctx).unwrap();

    let batches = ctx.query_parameterized(&compiled.base).await;
    let mut result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let redacted_count = run_redaction(&mut result, mock_service);

    let redaction_output = RedactionOutput {
        query_result: result,
        redacted_count,
    };

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::clone(client));
    let mut pipeline_ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(Arc::new(compiled)),
        ontology: Arc::clone(ontology),
        security_context: Some(security_ctx.clone()),
        server_extensions,
        phases: TypeMap::default(),
    };
    pipeline_ctx.phases.insert(redaction_output);
    let mut obs = NoOpObserver;

    let output = HydrationStage
        .execute(&mut pipeline_ctx, &mut obs)
        .await
        .expect("hydration should succeed");

    (output.query_result, output.result_context, redacted_count)
}

fn make_test_resources(
    ctx: &TestContext,
) -> (
    Arc<ontology::Ontology>,
    Arc<clickhouse_client::ArrowClickHouseClient>,
) {
    let ontology = Arc::new(load_ontology());
    let client = Arc::new(ctx.create_client());
    (ontology, client)
}

// ─────────────────────────────────────────────────────────────────────────────
// PathFinding Hydration
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_dynamic_hydration(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
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
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    assert!(
        matches!(plan, HydrationPlan::Dynamic(_)),
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
    let (ontology, client) = make_test_resources(ctx);
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
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    let row = result.authorized_rows().next().expect("should have a path");
    let path_nodes = row.path_nodes();

    // User 1 = alice
    let user_props = &path_nodes[0].properties;
    assert_eq!(
        user_props
            .get("username")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("alice"),
        "User 1 username should be 'alice'"
    );
    assert_eq!(
        user_props
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Alice Admin"),
        "User 1 name should be 'Alice Admin'"
    );

    // Project 1000 = Public Project
    let project = path_nodes.last().unwrap();
    let project_props = &project.properties;
    assert_eq!(
        project_props
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Public Project"),
        "Project 1000 name should be 'Public Project'"
    );

    // Group 100 (intermediate)
    if path_nodes.len() == 3 {
        let group_props = &path_nodes[1].properties;
        assert_eq!(
            group_props
                .get("name")
                .and_then(|v| v.as_string().map(|s| s.as_str())),
            Some("Public Group"),
            "Group 100 name should be 'Public Group'"
        );
    }
}

async fn path_finding_json_format(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
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
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

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
    assert_eq!(first.get("id").unwrap().as_str().unwrap(), "1");
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
    let (ontology, client) = make_test_resources(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, _ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    assert!(matches!(plan, HydrationPlan::Dynamic(_)));
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
    let (ontology, client) = make_test_resources(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, _ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    // User 1 is member of Group 100 ("Public Group")
    let neighbor_names: HashSet<&str> = result
        .authorized_rows()
        .filter_map(|r| {
            r.neighbor_node()
                .and_then(|n| n.properties.get("name")?.as_string().map(|s| s.as_str()))
        })
        .collect();

    assert!(
        neighbor_names.contains("Public Group"),
        "should find 'Public Group' in neighbor properties, got: {:?}",
        neighbor_names
    );
}

async fn neighbors_json_format(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

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

async fn search_produces_no_hydration_plan(_ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "node": {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}},
        "limit": 10
    }"#;

    let compiled = compile(json, &ontology, &security_ctx).unwrap();
    assert!(
        matches!(compiled.hydration, HydrationPlan::None),
        "Search should produce None (static hydration disabled), got: {:?}",
        compiled.hydration
    );
}

async fn traversal_produces_static_hydration_plan(_ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username", "name"]},
            {"id": "g", "entity": "Group", "columns": ["name", "visibility_level"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 20
    }"#;

    let compiled = compile(json, &ontology, &security_ctx).unwrap();
    assert!(
        matches!(compiled.hydration, HydrationPlan::Static(ref t) if t.len() == 2),
        "Edge-centric traversal should produce Static hydration with 2 templates, got: {:?}",
        compiled.hydration
    );
}

async fn hydration_query_type_rejected_from_user_input(_ctx: &TestContext) {
    let ontology = load_ontology();
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "hydration",
        "node": {"id": "h", "entity": "User", "node_ids": [1]},
        "limit": 10
    }"#;

    let result = compile(json, &ontology, &security_ctx);
    assert!(
        result.is_err(),
        "hydration query type must be rejected when submitted via user-facing compile(): {result:?}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Full Pipeline: Redact → Hydrate
// ─────────────────────────────────────────────────────────────────────────────

async fn path_finding_hydration_after_partial_redaction(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
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

    let (result, _ctx_ref, redacted_count) =
        compile_execute_redact_hydrate(ctx, json, &ontology, &security_ctx, &client, &mock_service)
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
                .and_then(|v| v.as_string().map(|s| s.as_str())),
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
    let (ontology, client) = make_test_resources(ctx);
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

    let (result, _ctx_ref, redacted_count) =
        compile_execute_redact_hydrate(ctx, json, &ontology, &security_ctx, &client, &mock_service)
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
        neighbor
            .properties
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Private Group"),
        "surviving neighbor should have correct hydrated name"
    );
}

async fn path_finding_all_denied_then_hydrate(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
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

    let (result, _ctx_ref, redacted_count) =
        compile_execute_redact_hydrate(ctx, json, &ontology, &security_ctx, &client, &mock_service)
            .await;

    assert!(redacted_count > 0, "should have had paths to redact");
    assert_eq!(
        result.authorized_count(),
        0,
        "no rows should survive after full denial"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Consolidated Hydration Data Correctness
// ─────────────────────────────────────────────────────────────────────────────

/// The consolidated path User→Group→Project hydrates all three entity types
/// in a single UNION ALL query. Verify each node has the correct property values.
async fn consolidated_path_hydrates_all_entity_types(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
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
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    let row = result.authorized_rows().next().expect("should have a path");
    let nodes = row.path_nodes();
    assert_eq!(nodes.len(), 3, "path should be User→Group→Project");

    let entity_types: Vec<&str> = nodes.iter().map(|n| n.entity_type.as_str()).collect();
    assert_eq!(entity_types, vec!["User", "Group", "Project"]);

    assert_eq!(
        nodes[0]
            .properties
            .get("username")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("alice")
    );
    assert_eq!(
        nodes[1]
            .properties
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Public Group")
    );
    assert_eq!(
        nodes[2]
            .properties
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("Public Project")
    );
}

/// Null values from ClickHouse (e.g. full_path=NULL) should be filtered out,
/// not appear as empty strings or crash the parser.
async fn consolidated_hydration_filters_null_properties(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "u", "entity": "User", "node_ids": [1]},
        "neighbors": {"node": "u", "direction": "outgoing"}
    }"#;

    let (result, _ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    for row in result.authorized_rows() {
        let neighbor = row.neighbor_node().expect("should have neighbor");
        for (key, value) in &neighbor.properties {
            assert!(
                !value
                    .as_string()
                    .is_some_and(|s| s == "null" || s.is_empty()),
                "property '{key}' should not be null or empty string, got: {value:?}"
            );
        }
    }
}

/// When multiple IDs of the same entity type need hydration, all should be returned.
async fn consolidated_hydration_multiple_ids_same_type(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
    let security_ctx = test_security_context();

    // Both users 1 and 2 are members of groups, producing a path through both
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1, 2]},
            {"id": "end", "entity": "Project", "node_ids": [1000, 1001]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let (result, _ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    assert!(
        result.authorized_count() >= 2,
        "should find paths for both users"
    );

    let user_ids: HashSet<i64> = result
        .authorized_rows()
        .filter_map(|r| r.path_nodes().first().map(|n| n.id))
        .collect();
    assert!(user_ids.contains(&1), "User 1 path should exist");
    assert!(user_ids.contains(&2), "User 2 path should exist");

    for row in result.authorized_rows() {
        for node in row.path_nodes() {
            assert!(
                !node.properties.is_empty(),
                "node {} ({}) should have hydrated properties",
                node.id,
                node.entity_type
            );
        }
    }
}

/// Verify that the consolidated query produces exactly one ClickHouse query
/// for hydration, regardless of how many entity types are discovered.
async fn consolidated_hydration_single_query_execution(ctx: &TestContext) {
    let (ontology, client) = make_test_resources(ctx);
    let security_ctx = test_security_context();

    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "User", "node_ids": [1]},
            {"id": "end", "entity": "Project", "node_ids": [1000]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 3}
    }"#;

    let compiled = compile(json, &ontology, &security_ctx).unwrap();
    let batches = ctx.query_parameterized(&compiled.base).await;
    let result = QueryResult::from_batches(&batches, &compiled.base.result_context);

    let redaction_output = RedactionOutput {
        query_result: result,
        redacted_count: 0,
    };

    let mut server_extensions = TypeMap::default();
    server_extensions.insert(Arc::clone(&client));
    let mut pipeline_ctx = QueryPipelineContext {
        query_json: String::new(),
        compiled: Some(Arc::new(compiled)),
        ontology: Arc::clone(&ontology),
        security_context: Some(security_ctx.clone()),
        server_extensions,
        phases: TypeMap::default(),
    };
    pipeline_ctx.phases.insert(redaction_output);

    let mut obs = NoOpObserver;
    let output = HydrationStage
        .execute(&mut pipeline_ctx, &mut obs)
        .await
        .expect("hydration should succeed");

    assert_eq!(
        output.hydration_queries.len(),
        1,
        "consolidated hydration should produce exactly 1 debug query entry, got {}",
        output.hydration_queries.len()
    );
    assert!(
        output.hydration_queries[0].sql.contains("UNION ALL")
            || output.hydration_queries[0].rendered.contains("UNION ALL"),
        "the single hydration query should be a UNION ALL"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// ─── Traversal static hydration with indirect-auth entities ─────────────────

/// Regression test: entities with `redaction.id_column != "id"` (e.g. File,
/// Definition where auth uses `project_id`) must hydrate using the entity's
/// own primary key (`_gkg_f_pk`), not the authorization ID (`_gkg_f_id`).
///
/// Without the fix, `collect_static_ids` reads `_gkg_f_id` (= project_id 1000)
/// instead of `_gkg_f_pk` (= file id 5001/5002), causing the hydration query
/// to look up `gl_file WHERE id = 1000` which returns nothing (or the wrong row).
async fn traversal_static_hydration_indirect_auth_entities(ctx: &TestContext) {
    let ontology = Arc::new(load_ontology());
    let security_ctx = test_security_context();
    let client = Arc::new(ctx.create_client());

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "f", "entity": "File", "id_range": {"start": 1, "end": 10000}, "columns": ["name", "path", "branch"]},
            {"id": "d", "entity": "Definition", "columns": ["name"]}
        ],
        "relationships": [{"type": "DEFINES", "from": "f", "to": "d"}],
        "limit": 10
    }"#;

    let (result, _ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    assert!(
        matches!(plan, HydrationPlan::Static(ref t) if t.len() == 2),
        "should produce Static hydration with 2 templates (File + Definition)"
    );

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert_eq!(
        authorized.len(),
        2,
        "should have 2 authorized rows (File->DEFINES->Definition edges for both files)"
    );

    // Verify EVERY row has hydrated properties — not just any. Catches partial
    // failures where some rows hydrate and others don't.
    let mut seen_file_names: Vec<String> = Vec::new();
    let mut seen_def_names: Vec<String> = Vec::new();
    for row in &authorized {
        let f_name = row.get_column_string("f_name").unwrap_or_else(|| {
            panic!(
                "File name missing on row. Hydration is likely using \
                 redaction ID (project_id) instead of PK (file id)."
            )
        });
        seen_file_names.push(f_name);

        let d_name = row.get_column_string("d_name").unwrap_or_else(|| {
            panic!(
                "Definition name missing on row. Hydration is likely using \
                 redaction ID (project_id) instead of PK (definition id)."
            )
        });
        seen_def_names.push(d_name);
    }

    seen_file_names.sort();
    seen_def_names.sort();
    assert_eq!(
        seen_file_names,
        vec!["lib.rs", "main.rs"],
        "both seeded files (5001, 5002) should be hydrated"
    );
    assert_eq!(
        seen_def_names,
        vec!["greet", "main"],
        "both seeded definitions (6001, 6002) should be hydrated"
    );
}

// ─── Dynamic hydration with indirect-auth entities ──────────────────────────

/// Verify that dynamic hydration (Neighbors) correctly resolves properties for
/// indirect-auth entities (File, Definition) where the entity PK differs from
/// the authorization ID. Dynamic hydration uses NodeRef.id which comes from
/// edge source_id/target_id (the actual entity PK), so this should work
/// without the static-hydration PK fix — but we test it to be sure.
async fn neighbors_dynamic_hydration_indirect_auth_entities(ctx: &TestContext) {
    let ontology = Arc::new(load_ontology());
    let security_ctx = test_security_context();
    let client = Arc::new(ctx.create_client());

    // File 5001 has outgoing DEFINES edges to Definition 6001
    let json = r#"{
        "query_type": "neighbors",
        "node": {"id": "f", "entity": "File", "node_ids": [5001]},
        "neighbors": {"node": "f", "direction": "outgoing"}
    }"#;

    let (result, _ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    assert!(
        matches!(plan, HydrationPlan::Dynamic(_)),
        "neighbors should produce Dynamic hydration plan"
    );

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert!(
        !authorized.is_empty(),
        "File 5001 should have at least one outgoing neighbor"
    );

    // The neighbor should be Definition 6001 with hydrated properties
    let has_definition_neighbor = authorized.iter().any(|row| {
        row.dynamic_nodes()
            .iter()
            .any(|n| n.entity_type == "Definition" && n.id == 6001 && !n.properties.is_empty())
    });
    assert!(
        has_definition_neighbor,
        "Definition 6001 should appear as a neighbor with hydrated properties"
    );

    // Verify the actual property value
    for row in &authorized {
        for node in row.dynamic_nodes() {
            if node.entity_type == "Definition" && node.id == 6001 {
                assert_eq!(
                    node.properties
                        .get("name")
                        .and_then(|v| v.as_string().map(|s| s.as_str())),
                    Some("greet"),
                    "Definition 6001 name should be 'greet'"
                );
            }
        }
    }
}

/// Verify that dynamic hydration (PathFinding) correctly resolves properties for
/// indirect-auth entities. The path File->Definition traverses entities where
/// PK != auth ID. NodeRef.id should carry the entity PK (from edge source_id/
/// target_id), so hydration queries `gl_file WHERE id = <file_id>`.
async fn path_finding_dynamic_hydration_indirect_auth_entities(ctx: &TestContext) {
    let ontology = Arc::new(load_ontology());
    let security_ctx = test_security_context();
    let client = Arc::new(ctx.create_client());

    // Path from File 5001 to Definition 6001 (single hop via DEFINES)
    let json = r#"{
        "query_type": "path_finding",
        "nodes": [
            {"id": "start", "entity": "File", "node_ids": [5001]},
            {"id": "end", "entity": "Definition", "node_ids": [6001]}
        ],
        "path": {"type": "shortest", "from": "start", "to": "end", "max_depth": 2}
    }"#;

    let (result, _ctx_ref, plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    assert!(
        matches!(plan, HydrationPlan::Dynamic(_)),
        "path_finding should produce Dynamic hydration plan"
    );

    let row = result
        .authorized_rows()
        .next()
        .expect("should find a path from File 5001 to Definition 6001");
    let path_nodes = row.path_nodes();
    assert_eq!(path_nodes.len(), 2, "path should be File -> Definition");

    // File 5001 = lib.rs
    let file_node = &path_nodes[0];
    assert_eq!(file_node.entity_type, "File");
    assert_eq!(file_node.id, 5001);
    assert!(
        !file_node.properties.is_empty(),
        "File node should have hydrated properties (PK 5001, not project_id 1000)"
    );
    assert_eq!(
        file_node
            .properties
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("lib.rs"),
        "File 5001 name should be 'lib.rs'"
    );

    // Definition 6001 = greet
    let def_node = &path_nodes[1];
    assert_eq!(def_node.entity_type, "Definition");
    assert_eq!(def_node.id, 6001);
    assert!(
        !def_node.properties.is_empty(),
        "Definition node should have hydrated properties (PK 6001, not project_id 1000)"
    );
    assert_eq!(
        def_node
            .properties
            .get("name")
            .and_then(|v| v.as_string().map(|s| s.as_str())),
        Some("greet"),
        "Definition 6001 name should be 'greet'"
    );
}

/// Verify that entities where PK == auth ID (User, Group) still hydrate correctly
/// after the fix (no regression from the PK-fallback logic).
async fn traversal_static_hydration_default_auth_entities(ctx: &TestContext) {
    let ontology = Arc::new(load_ontology());
    let security_ctx = test_security_context();
    let client = Arc::new(ctx.create_client());

    let json = r#"{
        "query_type": "traversal",
        "nodes": [
            {"id": "u", "entity": "User", "id_range": {"start": 1, "end": 10000}, "columns": ["username"]},
            {"id": "g", "entity": "Group", "columns": ["name"]}
        ],
        "relationships": [{"type": "MEMBER_OF", "from": "u", "to": "g"}],
        "limit": 10
    }"#;

    let (result, _ctx_ref, _plan) =
        compile_execute_hydrate(ctx, json, &ontology, &security_ctx, &client).await;

    let authorized: Vec<_> = result.authorized_rows().collect();
    assert!(!authorized.is_empty(), "should have authorized rows");

    let user_props_found = authorized.iter().any(|row| {
        row.get_column_string("u_username")
            .is_some_and(|v| v == "alice")
    });
    assert!(
        user_props_found,
        "User username should be hydrated (fallback to _gkg_u_id when _gkg_u_pk absent)"
    );

    let group_props_found = authorized.iter().any(|row| {
        row.get_column_string("g_name")
            .is_some_and(|v| v == "Public Group")
    });
    assert!(group_props_found, "Group name should be hydrated");
}

// Orchestrator
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn hydration_integration() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    setup_test_data(&ctx).await;

    run_subtests_shared!(
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
        traversal_produces_static_hydration_plan,
        hydration_query_type_rejected_from_user_input,
        // full pipeline: redact then hydrate
        path_finding_hydration_after_partial_redaction,
        neighbors_hydration_after_partial_redaction,
        path_finding_all_denied_then_hydrate,
        // consolidated hydration data correctness
        consolidated_path_hydrates_all_entity_types,
        consolidated_hydration_filters_null_properties,
        consolidated_hydration_multiple_ids_same_type,
        consolidated_hydration_single_query_execution,
        // static + dynamic hydration with indirect-auth entities
        traversal_static_hydration_indirect_auth_entities,
        traversal_static_hydration_default_auth_entities,
        neighbors_dynamic_hydration_indirect_auth_entities,
        path_finding_dynamic_hydration_indirect_auth_entities,
    );
}

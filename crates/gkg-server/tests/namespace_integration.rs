//! Integration tests for the namespace handler (groups and projects).
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

mod common;

use arrow::array::{StringArray, UInt64Array};
use chrono::{DateTime, Utc};
use etl_engine::module::Module;
use etl_engine::testkit::TestEnvelopeFactory;
use gkg_server::indexer::modules::SdlcModule;
use serial_test::serial;

use common::{TestContext, create_namespace_payload};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_and_transforms_groups() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
            (101, 'team-a', 'team-a', 10, 100, 2, '2023-06-01', '2024-01-10', '2024-01-20 12:00:00'),
            (102, 'team-b', 'team-b', 20, 100, NULL, '2023-09-01', '2024-01-05', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES
            (100, 'Organization 1'),
            (101, 'Team A under org1'),
            (102, NULL)",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES
            (100, '1/100/'),
            (101, '1/100/101/'),
            (102, '1/100/102/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config, TestContext::ontology_path())
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_groups ORDER BY id").await;
    assert!(!result.is_empty(), "groups result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let visibility_column = batch
        .column_by_name("visibility_level")
        .expect("visibility_level column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("visibility_level should be StringArray");

    assert_eq!(visibility_column.value(0), "private");
    assert_eq!(visibility_column.value(1), "internal");
    assert_eq!(visibility_column.value(2), "public");
}

#[tokio::test]
#[serial]
async fn namespace_handler_creates_group_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
            (101, 'team-a', 'team-a', 10, 100, 2, '2023-06-01', '2024-01-10', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Org'), (101, 'Team')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/'), (101, '1/100/101/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config, TestContext::ontology_path())
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify owner edges
    let owner_edges = context
        .query("SELECT source_id, target_id FROM kg_edges WHERE relationship_kind = 'owner' ORDER BY target_id")
        .await;

    assert!(!owner_edges.is_empty(), "owner edges should exist");
    let batch = &owner_edges[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 owner edges");

    // Verify parent-child edges
    let parent_edges = context
        .query("SELECT source_id, target_id FROM kg_edges WHERE relationship_kind = 'contains' AND source_kind = 'Group' AND target_kind = 'Group'")
        .await;

    assert!(!parent_edges.is_empty(), "parent edges should exist");
    let batch = &parent_edges[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "should have 1 parent-child edge (100 contains 101)"
    );
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_projects() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Organization 1')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_projects (id, name, description, visibility_level, path, namespace_id, creator_id, created_at, updated_at, archived, star_count, last_activity_at, _siphon_replicated_at)
            VALUES
            (1000, 'project-alpha', 'Alpha project', 0, 'project-alpha', 100, 1, '2023-01-01', '2024-01-15', false, 42, '2024-01-15', '2024-01-20 12:00:00'),
            (1001, 'project-beta', 'Beta project', 20, 'project-beta', 100, 2, '2023-06-01', '2024-01-10', true, 10, '2024-01-10', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/'), (1001, '1/100/1001/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config, TestContext::ontology_path())
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT * FROM gl_projects ORDER BY id").await;
    assert!(!result.is_empty(), "projects result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let visibility_column = batch
        .column_by_name("visibility_level")
        .expect("visibility_level column should exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("visibility_level should be StringArray");

    assert_eq!(visibility_column.value(0), "private");
    assert_eq!(visibility_column.value(1), "public");

    // Verify project edges (Project is the source, pointing to User/Group as target)
    let creator_edges = context
        .query("SELECT source_id, target_id FROM kg_edges WHERE relationship_kind = 'creator' AND source_kind = 'Project' AND target_kind = 'User' ORDER BY source_id")
        .await;

    assert!(!creator_edges.is_empty(), "creator edges should exist");
    assert_eq!(creator_edges[0].num_rows(), 2);

    let contains_edges = context
        .query("SELECT source_id, target_id FROM kg_edges WHERE relationship_kind = 'contains' AND source_kind = 'Project' AND target_kind = 'Group'")
        .await;

    assert!(!contains_edges.is_empty(), "contains edges should exist");
    assert_eq!(contains_edges[0].num_rows(), 2);
}

#[tokio::test]
#[serial]
async fn namespace_handler_uses_watermark_for_incremental_processing() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO namespace_indexing_watermark (namespace, watermark)
            VALUES (100, '2024-01-19 00:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-18 12:00:00'),
            (101, 'new-team', 'new-team', 10, 100, NULL, '2024-01-19', '2024-01-20', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Old org'), (101, 'New team')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/'), (101, '1/100/101/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config, TestContext::ontology_path())
        .await
        .expect("failed to create SDLC module");

    let handlers = sdlc_module.handlers();
    let namespace_handler = handlers
        .iter()
        .find(|h| h.name() == "namespace-handler")
        .expect("namespace-handler not found");

    let watermark = DateTime::parse_from_rfc3339("2024-01-21T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context.query("SELECT count() as cnt FROM gl_groups").await;
    let count_array = result[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("expected UInt64Array");

    assert_eq!(
        count_array.value(0),
        1,
        "should only process new-team, not org1"
    );

    let names = context.query("SELECT name FROM gl_groups").await;
    let name_array = names[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name should be StringArray");

    assert_eq!(name_array.value(0), "new-team");
}

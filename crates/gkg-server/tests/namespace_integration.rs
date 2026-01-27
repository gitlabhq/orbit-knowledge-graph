//! Integration tests for the namespace handler (groups and projects).
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

mod common;

use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array, UInt64Array, UInt8Array};
use chrono::{DateTime, Utc};
use etl_engine::module::Module;
use etl_engine::testkit::TestEnvelopeFactory;
use gkg_server::indexer::modules::SdlcModule;
use serial_test::serial;

use common::{TestContext, binary_as_str, create_handler_context, create_namespace_payload};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_and_transforms_groups() {
    let context = TestContext::new().await;

    // Insert source data: a parent group and a child group
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

    let sdlc_module = SdlcModule::new(&context.config)
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
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify groups were created
    let result = context.query("SELECT * FROM groups ORDER BY id").await;
    assert!(!result.is_empty(), "groups result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    // Check visibility_level transformation
    let visibility_column = batch
        .column_by_name("visibility_level")
        .expect("visibility_level column should exist")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("visibility_level should be BinaryArray");

    assert_eq!(binary_as_str(visibility_column, 0), "private");
    assert_eq!(binary_as_str(visibility_column, 1), "internal");
    assert_eq!(binary_as_str(visibility_column, 2), "public");
}

#[tokio::test]
#[serial]
async fn namespace_handler_creates_group_owner_edges() {
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

    let sdlc_module = SdlcModule::new(&context.config)
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
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify owner edges were created
    let result = context
        .query("SELECT * FROM edges WHERE relationship_kind = 'owner' ORDER BY target_id")
        .await;

    assert!(!result.is_empty(), "owner edges result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 owner edges");

    let source_id = batch
        .column_by_name("source_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let target_id = batch
        .column_by_name("target_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // User 1 owns group 100, User 2 owns group 101
    assert_eq!(source_id.value(0), 1);
    assert_eq!(target_id.value(0), 100);
    assert_eq!(source_id.value(1), 2);
    assert_eq!(target_id.value(1), 101);
}

#[tokio::test]
#[serial]
async fn namespace_handler_creates_parent_child_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES
            (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00'),
            (101, 'team-a', 'team-a', 10, 100, NULL, '2023-06-01', '2024-01-10', '2024-01-20 12:00:00'),
            (102, 'sub-team', 'sub-team', 10, 101, NULL, '2023-09-01', '2024-01-05', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_namespace_details (namespace_id, description)
            VALUES (100, 'Org'), (101, 'Team'), (102, 'Sub')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path)
            VALUES (100, '1/100/'), (101, '1/100/101/'), (102, '1/100/101/102/')",
        )
        .await;

    let sdlc_module = SdlcModule::new(&context.config)
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
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify parent-child edges were created
    let result = context
        .query("SELECT * FROM edges WHERE relationship_kind = 'contains' AND source_kind = 'Group' AND target_kind = 'Group' ORDER BY target_id")
        .await;

    assert!(
        !result.is_empty(),
        "parent edges result should not be empty"
    );

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 parent-child edges");

    let source_id = batch
        .column_by_name("source_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let target_id = batch
        .column_by_name("target_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Group 100 contains group 101, Group 101 contains group 102
    assert_eq!(source_id.value(0), 100);
    assert_eq!(target_id.value(0), 101);
    assert_eq!(source_id.value(1), 101);
    assert_eq!(target_id.value(1), 102);
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_projects() {
    let context = TestContext::new().await;

    // Insert a group first (projects belong to groups)
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
            VALUES (100, '1/100')",
        )
        .await;

    // Insert projects
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

    let sdlc_module = SdlcModule::new(&context.config)
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
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify projects were created
    let result = context.query("SELECT * FROM projects ORDER BY id").await;
    assert!(!result.is_empty(), "projects result should not be empty");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    // Check visibility_level transformation
    let visibility_column = batch
        .column_by_name("visibility_level")
        .expect("visibility_level column should exist")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("visibility_level should be BinaryArray");

    assert_eq!(binary_as_str(visibility_column, 0), "private");
    assert_eq!(binary_as_str(visibility_column, 1), "public");

    // Check archived column
    let archived_column = batch
        .column_by_name("archived")
        .expect("archived column should exist")
        .as_any()
        .downcast_ref::<UInt8Array>()
        .expect("archived should be UInt8Array");

    assert_eq!(archived_column.value(0), 0);
    assert_eq!(archived_column.value(1), 1);

    // Check star_count
    let star_count_column = batch
        .column_by_name("star_count")
        .expect("star_count column should exist")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("star_count should be Int64Array");

    assert_eq!(star_count_column.value(0), 42);
    assert_eq!(star_count_column.value(1), 10);
}

#[tokio::test]
#[serial]
async fn namespace_handler_creates_project_edges() {
    let context = TestContext::new().await;

    // Insert a group
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

    // Insert projects with creators (exactly matching the passing test)
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

    let sdlc_module = SdlcModule::new(&context.config)
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
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Verify creator edges (User -> Project)
    let creator_result = context
        .query("SELECT * FROM edges WHERE relationship_kind = 'creator' AND target_kind = 'Project' ORDER BY target_id")
        .await;

    assert!(
        !creator_result.is_empty(),
        "creator edges result should not be empty"
    );

    let batch = &creator_result[0];
    assert_eq!(batch.num_rows(), 2);

    let source_id = batch
        .column_by_name("source_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // User 1 created project 1000, User 2 created project 1001
    assert_eq!(source_id.value(0), 1);
    assert_eq!(source_id.value(1), 2);

    // Verify contains edges (Group contains Project)
    let contains_result = context
        .query("SELECT * FROM edges WHERE relationship_kind = 'contains' AND target_kind = 'Project' ORDER BY target_id")
        .await;

    assert!(
        !contains_result.is_empty(),
        "contains edges result should not be empty"
    );

    let batch = &contains_result[0];
    assert_eq!(batch.num_rows(), 2);

    let source_id = batch
        .column_by_name("source_id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    // Group 100 contains both projects
    assert_eq!(source_id.value(0), 100);
    assert_eq!(source_id.value(1), 100);
}

#[tokio::test]
#[serial]
async fn namespace_handler_uses_watermark_for_incremental_processing() {
    let context = TestContext::new().await;

    // Set a watermark
    context
        .execute(
            "INSERT INTO namespace_indexing_watermark (namespace, watermark)
            VALUES (100, '2024-01-19 00:00:00')",
        )
        .await;

    // Insert groups: one old (before watermark), one new (after watermark)
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

    let sdlc_module = SdlcModule::new(&context.config)
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
    let destination = Arc::new(context.create_destination());
    let handler_context = create_handler_context(destination);

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    // Should only have processed the new group (after watermark)
    let result = context.query("SELECT count() as cnt FROM groups").await;
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

    let names = context.query("SELECT name FROM groups").await;
    let name_array = names[0]
        .column(0)
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("name should be BinaryArray");

    assert_eq!(binary_as_str(name_array, 0), "new-team");
}

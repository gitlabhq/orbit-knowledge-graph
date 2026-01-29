//! Integration tests for group processing in the namespace handler.

use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, create_namespace_payload, default_test_watermark, get_namespace_handler,
    get_string_column,
};

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

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

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

    let visibility_column = get_string_column(batch, "visibility_level");

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

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let owner_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'owner' ORDER BY target_id")
        .await;

    assert!(!owner_edges.is_empty(), "owner edges should exist");
    let batch = &owner_edges[0];
    assert_eq!(batch.num_rows(), 2, "should have 2 owner edges");

    let parent_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'contains' AND source_kind = 'Group' AND target_kind = 'Group'")
        .await;

    assert!(!parent_edges.is_empty(), "parent edges should exist");
    let batch = &parent_edges[0];
    assert_eq!(
        batch.num_rows(),
        1,
        "should have 1 parent-child edge (100 contains 101)"
    );
}

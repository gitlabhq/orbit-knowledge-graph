//! Integration tests for label processing in the namespace handler.

use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, assert_edge_count, create_namespace_payload, default_test_watermark,
    get_namespace_handler, get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_labels_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_namespaces (id, name, path, visibility_level, parent_id, owner_id, created_at, updated_at, _siphon_replicated_at)
            VALUES (100, 'org1', 'org1', 0, NULL, 1, '2023-01-01', '2024-01-15', '2024-01-20 12:00:00')",
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
            "INSERT INTO siphon_projects (id, name, namespace_id, _siphon_replicated_at)
            VALUES (1000, 'project-alpha', 100, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO project_namespace_traversal_paths (id, traversal_path)
            VALUES (1000, '1/100/1000/')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_labels
                (id, title, color, description, project_id, group_id, traversal_path, _siphon_replicated_at)
            VALUES
                (1, 'bug', '#ff0000', 'Bug reports', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
                (2, 'feature', '#00ff00', 'New features', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
                (3, 'priority', '#0000ff', 'Priority items', NULL, 100, '1/100/', '2024-01-20 12:00:00')",
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

    let result = context
        .query("SELECT id, title, color, description FROM gl_labels ORDER BY id")
        .await;
    assert!(!result.is_empty(), "labels should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let titles = get_string_column(batch, "title");
    assert_eq!(titles.value(0), "bug");
    assert_eq!(titles.value(1), "feature");
    assert_eq!(titles.value(2), "priority");

    let colors = get_string_column(batch, "color");
    assert_eq!(colors.value(0), "#ff0000");
    assert_eq!(colors.value(1), "#00ff00");
    assert_eq!(colors.value(2), "#0000ff");

    assert_edge_count(&context, "IN_PROJECT", "Label", "Project", 2).await;
    assert_edge_count(&context, "IN_GROUP", "Label", "Group", 1).await;
}

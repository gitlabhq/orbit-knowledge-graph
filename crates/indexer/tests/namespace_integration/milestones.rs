//! Integration tests for milestone processing in the namespace handler.

use indexer::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, assert_edges_have_traversal_path, create_namespace_payload,
    default_test_watermark, get_namespace_handler, get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_milestones_with_edges() {
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
            "INSERT INTO siphon_milestones
                (id, iid, title, description, state, due_date, start_date, project_id, group_id,
                 traversal_path, _siphon_replicated_at)
            VALUES
                (1, 1, 'v1.0', 'First release', 'active', '2024-03-01', '2024-01-01', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
                (2, 2, 'v2.0', 'Second release', 'closed', '2024-06-01', '2024-03-01', 1000, NULL, '1/100/', '2024-01-20 12:00:00'),
                (3, 1, 'Q1 Goals', 'Group milestone', 'active', '2024-03-31', '2024-01-01', NULL, 100, '1/100/', '2024-01-20 12:00:00')",
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
        .query("SELECT id, title, state, due_date FROM gl_milestone ORDER BY id")
        .await;
    assert!(!result.is_empty(), "milestones should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let titles = get_string_column(batch, "title");
    assert_eq!(titles.value(0), "v1.0");
    assert_eq!(titles.value(1), "v2.0");
    assert_eq!(titles.value(2), "Q1 Goals");

    let states = get_string_column(batch, "state");
    assert_eq!(states.value(0), "active");
    assert_eq!(states.value(1), "closed");

    assert_edges_have_traversal_path(&context, "IN_PROJECT", "Milestone", "Project", "1/100/", 2)
        .await;

    assert_edges_have_traversal_path(&context, "IN_GROUP", "Milestone", "Group", "1/100/", 1).await;
}

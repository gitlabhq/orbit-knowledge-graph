//! Integration tests for project processing in the namespace handler.

use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, create_namespace_payload, default_test_watermark, get_namespace_handler,
    get_string_column,
};

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

    let namespace_handler = get_namespace_handler(&context).await;
    let watermark = default_test_watermark();

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

    let visibility_column = get_string_column(batch, "visibility_level");

    assert_eq!(visibility_column.value(0), "private");
    assert_eq!(visibility_column.value(1), "public");

    let creator_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'CREATOR' AND source_kind = 'User' AND target_kind = 'Project' ORDER BY target_id")
        .await;

    assert!(!creator_edges.is_empty(), "creator edges should exist");
    assert_eq!(creator_edges[0].num_rows(), 2);

    let contains_edges = context
        .query("SELECT source_id, target_id FROM gl_edges WHERE relationship_kind = 'CONTAINS' AND source_kind = 'Group' AND target_kind = 'Project'")
        .await;

    assert!(!contains_edges.is_empty(), "contains edges should exist");
    assert_eq!(contains_edges[0].num_rows(), 2);
}

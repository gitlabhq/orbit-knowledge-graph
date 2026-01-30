//! Integration tests for work item processing in the namespace handler.

use etl_engine::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, assert_edge_count, create_namespace_payload, default_test_watermark,
    get_namespace_handler, get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_work_items_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
            VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_issues (id, title, description, author_id, project_id, state_id, work_item_type_id, _siphon_replicated_at)
            VALUES
                (1, 'Fix login bug', 'Users cannot log in', 1, 1000, 1, 0, '2024-01-20 12:00:00'),
                (2, 'Add feature Y', 'New feature request', 2, 1000, 2, 4, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, iid, title, author_id, state_id, work_item_type_id, confidential,
                 milestone_id, namespace_id, assignee_ids, label_ids,
                 traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 1, 'Fix login bug', 1, 1, 0, false, 10, 100, '2/3', '5/6/7', '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 2, 'Add feature Y', 2, 2, 4, true, NULL, 100, '', '8', '1/100/', '2024-01-20 12:00:00', 0, 0)",
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
        .query(
            "SELECT id, title, state, work_item_type, confidential FROM gl_work_items ORDER BY id",
        )
        .await;
    assert!(!result.is_empty(), "work items should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let titles = get_string_column(batch, "title");
    assert_eq!(titles.value(0), "Fix login bug");
    assert_eq!(titles.value(1), "Add feature Y");

    let states = get_string_column(batch, "state");
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "closed");

    let work_item_types = get_string_column(batch, "work_item_type");
    assert_eq!(work_item_types.value(0), "issue");
    assert_eq!(work_item_types.value(1), "task");
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_work_item_single_value_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
            VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO namespace_traversal_paths (id, traversal_path, version)
            VALUES (100, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_issues (id, title, description, _siphon_replicated_at)
            VALUES (1, 'Test issue', 'Test description', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 milestone_id, namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES (1, 'Test issue', 1, 1, 0, false, 10, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
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

    let authored_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'authored' AND target_kind = 'WorkItem'",
        )
        .await;
    assert_eq!(
        authored_edges[0].num_rows(),
        1,
        "work item should have author edge"
    );

    let in_milestone_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'in_milestone' AND source_kind = 'WorkItem'",
        )
        .await;
    assert_eq!(
        in_milestone_edges[0].num_rows(),
        1,
        "work item should have milestone edge"
    );

    let in_group_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'in_group' AND source_kind = 'WorkItem'",
        )
        .await;
    assert_eq!(
        in_group_edges[0].num_rows(),
        1,
        "work item should have group edge"
    );
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_work_item_multi_target_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_issues (id, title, description, _siphon_replicated_at)
            VALUES (1, 'Test issue', 'Test description', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 namespace_id, assignee_ids, label_ids, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES (1, 'Test issue', 1, 1, 0, false, 100, '10/20/30', '5/6', '1/100/', '2024-01-20 12:00:00', 0, 0)",
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

    let assigned_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'assigned' AND target_kind = 'WorkItem'
             ORDER BY source_id",
        )
        .await;
    assert_eq!(
        assigned_edges[0].num_rows(),
        3,
        "work item should have 3 assignee edges (10, 20, 30)"
    );

    let has_label_edges = context
        .query(
            "SELECT source_id, target_id FROM gl_edges
             WHERE relationship_kind = 'has_label' AND source_kind = 'WorkItem'
             ORDER BY target_id",
        )
        .await;
    assert_eq!(
        has_label_edges[0].num_rows(),
        2,
        "work item should have 2 label edges (5, 6)"
    );
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_work_item_parent_links() {
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
            "INSERT INTO siphon_issues (id, title, project_id, author_id, state_id, work_item_type_id, _siphon_replicated_at)
            VALUES
                (1, 'Epic: Q1 Goals', 1000, 1, 1, 7, '2024-01-20 12:00:00'),
                (2, 'Task: Design review', 1000, 1, 1, 4, '2024-01-20 12:00:00'),
                (3, 'Task: Implementation', 1000, 1, 1, 4, '2024-01-20 12:00:00'),
                (4, 'Sub-task: Frontend', 1000, 1, 1, 4, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 'Epic: Q1 Goals', 1, 1, 7, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 'Task: Design review', 1, 1, 4, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (3, 'Task: Implementation', 1, 1, 4, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (4, 'Sub-task: Frontend', 1, 1, 4, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_work_item_parent_links
                (id, work_item_id, work_item_parent_id, namespace_id, traversal_path,
                 created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 2, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 3, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (3, 4, 3, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
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

    assert_edge_count(&context, "CONTAINS", "WorkItem", "WorkItem", 3).await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_issue_links() {
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
            "INSERT INTO siphon_issues (id, title, project_id, author_id, state_id, work_item_type_id, _siphon_replicated_at)
            VALUES
                (1, 'Issue A', 1000, 1, 1, 0, '2024-01-20 12:00:00'),
                (2, 'Issue B', 1000, 1, 1, 0, '2024-01-20 12:00:00'),
                (3, 'Issue C', 1000, 1, 1, 0, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 'Issue A', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 'Issue B', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (3, 'Issue C', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_issue_links
                (id, source_id, target_id, link_type, namespace_id, traversal_path,
                 created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 1, 2, 0, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 2, 3, 1, 100, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
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

    assert_edge_count(&context, "RELATED_TO", "WorkItem", "WorkItem", 2).await;
}

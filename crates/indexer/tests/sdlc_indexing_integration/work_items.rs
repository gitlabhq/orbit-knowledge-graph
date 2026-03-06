//! Integration subtests for work item processing.

use indexer::testkit::TestEnvelopeFactory;

use crate::common::{
    IndexerTestExt, TestContext, create_namespace_payload, default_test_watermark,
    get_string_column,
};

pub async fn processes_work_items_with_edges(context: &TestContext) {
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
                (1, 'Fix login bug', 'Users cannot log in', 1, 1000, 1, 1, '2024-01-20 12:00:00'),
                (2, 'Add feature Y', 'New feature request', 2, 1000, 2, 5, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, iid, title, author_id, state_id, work_item_type_id, confidential,
                 milestone_id, namespace_id, assignee_ids, label_ids,
                 traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 1, 'Fix login bug', 1, 1, 1, false, 10, 100, '2/3', '5/6/7', '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 2, 'Add feature Y', 2, 2, 5, true, NULL, 100, '', '8', '1/100/', '2024-01-20 12:00:00', 0, 0)",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    let result = context
        .query(
            "SELECT id, title, state, work_item_type, confidential FROM gl_work_item FINAL ORDER BY id",
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

pub async fn processes_work_item_single_value_edges(context: &TestContext) {
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
            VALUES (1, 'Test issue', 1, 1, 1, false, 10, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    context
        .assert_edges_have_traversal_path("AUTHORED", "User", "WorkItem", "1/100/", 1)
        .await;
    context
        .assert_edges_have_traversal_path("IN_MILESTONE", "WorkItem", "Milestone", "1/100/", 1)
        .await;
    context
        .assert_edges_have_traversal_path("IN_GROUP", "WorkItem", "Group", "1/100/", 1)
        .await;
}

pub async fn processes_work_item_multi_target_edges(context: &TestContext) {
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
            VALUES (1, 'Test issue', 1, 1, 1, false, 100, '10/20/30', '5/6', '1/100/', '2024-01-20 12:00:00', 0, 0)",
        )
        .await;

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    context
        .assert_edges_have_traversal_path("ASSIGNED", "User", "WorkItem", "1/100/", 3)
        .await;
    context
        .assert_edges_have_traversal_path("HAS_LABEL", "WorkItem", "Label", "1/100/", 2)
        .await;
}

pub async fn processes_work_item_parent_links(context: &TestContext) {
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
                (1, 'Epic: Q1 Goals', 1000, 1, 1, 8, '2024-01-20 12:00:00'),
                (2, 'Task: Design review', 1000, 1, 1, 5, '2024-01-20 12:00:00'),
                (3, 'Task: Implementation', 1000, 1, 1, 5, '2024-01-20 12:00:00'),
                (4, 'Sub-task: Frontend', 1000, 1, 1, 5, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 'Epic: Q1 Goals', 1, 1, 8, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 'Task: Design review', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (3, 'Task: Implementation', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (4, 'Sub-task: Frontend', 1, 1, 5, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
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

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    context
        .assert_edges_have_traversal_path("CONTAINS", "WorkItem", "WorkItem", "1/100/", 3)
        .await;
}

pub async fn processes_issue_links(context: &TestContext) {
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
                (1, 'Issue A', 1000, 1, 1, 1, '2024-01-20 12:00:00'),
                (2, 'Issue B', 1000, 1, 1, 1, '2024-01-20 12:00:00'),
                (3, 'Issue C', 1000, 1, 1, 1, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 'Issue A', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 'Issue B', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (3, 'Issue C', 1, 1, 1, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
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

    let namespace_handler = context.get_namespace_handler().await;
    let watermark = default_test_watermark();

    let envelope = TestEnvelopeFactory::simple(&create_namespace_payload(1, 100, watermark));
    let handler_context = context.create_handler_context();

    namespace_handler
        .handle(handler_context, envelope)
        .await
        .expect("handler should succeed");

    context
        .assert_edges_have_traversal_path("RELATED_TO", "WorkItem", "WorkItem", "1/100/", 2)
        .await;
}

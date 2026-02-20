//! Integration tests for merge request processing in the namespace handler.

use indexer::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, assert_edges_have_traversal_path, create_namespace_payload,
    default_test_watermark, get_namespace_handler, get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_requests_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
            VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, author_id, assignee_ids, merge_user_id, milestone_id,
                 traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'Implements feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, 1, '2/3', NULL, 10, '1/100/', '2024-01-20 12:00:00'),
                (2, 102, 'Fix bug Y', 'Fixes critical bug', 'fix-y', 'main', 3, 'merged',
                 false, false, 1000, 2, '', 1, NULL, '1/100/', '2024-01-20 12:00:00')",
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
        .query("SELECT id, title, state, merge_status, draft, squash FROM gl_merge_request ORDER BY id")
        .await;
    assert!(!result.is_empty(), "merge requests should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let titles = get_string_column(batch, "title");
    assert_eq!(titles.value(0), "Add feature X");
    assert_eq!(titles.value(1), "Fix bug Y");

    let states = get_string_column(batch, "state");
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "merged");

    assert_edges_have_traversal_path(
        &context,
        "IN_PROJECT",
        "MergeRequest",
        "Project",
        "1/100/",
        2,
    )
    .await;
    assert_edges_have_traversal_path(&context, "AUTHORED", "User", "MergeRequest", "1/100/", 2)
        .await;
    assert_edges_have_traversal_path(&context, "ASSIGNED", "User", "MergeRequest", "1/100/", 2)
        .await;
    assert_edges_have_traversal_path(&context, "MERGED_BY", "User", "MergeRequest", "1/100/", 1)
        .await;
    assert_edges_have_traversal_path(
        &context,
        "IN_MILESTONE",
        "MergeRequest",
        "Milestone",
        "1/100/",
        1,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_requests_closing_issues() {
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
                (1, 'Bug: Login fails', 1000, 1, 1, 0, '2024-01-20 12:00:00'),
                (2, 'Bug: Signup broken', 1000, 1, 1, 0, '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_work_items
                (id, title, author_id, state_id, work_item_type_id, confidential,
                 namespace_id, traversal_path, version, custom_status_id, system_defined_status_id)
            VALUES
                (1, 'Bug: Login fails', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0),
                (2, 'Bug: Signup broken', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00', 0, 0)",
        )
        .await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, author_id, traversal_path, version)
            VALUES
                (10, 101, 'Fix login bug', 'Fixes login issue', 'fix-login', 'main', 3, 'merged',
                 false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
                (20, 102, 'Fix signup bug', 'Fixes signup issue', 'fix-signup', 'main', 3, 'merged',
                 false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_requests_closing_issues
                (id, merge_request_id, issue_id, project_id, traversal_path,
                 created_at, updated_at, _siphon_replicated_at)
            VALUES
                (1, 10, 1, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
                (2, 20, 2, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
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

    assert_edges_have_traversal_path(&context, "CLOSES", "MergeRequest", "WorkItem", "1/100/", 2)
        .await;
}

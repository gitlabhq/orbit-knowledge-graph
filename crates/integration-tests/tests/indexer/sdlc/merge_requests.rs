use arrow::array::StringArray;
use gkg_utils::arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, create_namespace,
    handler_context, namespace_envelope, namespace_handler,
};

pub async fn processes_merge_requests_with_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO siphon_milestones (id, title, project_id, state, traversal_path, _siphon_replicated_at)
        VALUES (10, 'v1.0', 1000, 'active', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, assignees, merge_user_id, milestone_id,
             reviewers, approvals, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 101, 'Add feature X', 'Implements feature X', 'feature-x', 'main', 1, 'can_be_merged',
             false, true, 1000, 1, [(2, '2024-01-20 12:00:00'), (3, '2024-01-20 12:00:00')], NULL, 10,
             [(4, 0, '2024-01-20 12:00:00'), (5, 0, '2024-01-20 12:00:00')],
             [(5, '2024-01-20 12:00:00')], '1/100/', '2024-01-20 12:00:00'),
            (2, 102, 'Fix bug Y', 'Fixes critical bug', 'fix-y', 'main', 3, 'merged',
             false, false, 1000, 2, [], 1, NULL,
             [(6, 0, '2024-01-20 12:00:00')],
             [(3, '2024-01-20 12:00:00'), (4, '2024-01-20 12:00:00')], '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_merge_request", 2).await;

    let result = ctx
        .query("SELECT title, state FROM gl_merge_request FINAL ORDER BY id")
        .await;
    let batch = &result[0];
    let titles =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "title").expect("title column");
    assert_eq!(titles.value(0), "Add feature X");
    assert_eq!(titles.value(1), "Fix bug Y");

    let states =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "state").expect("state column");
    assert_eq!(states.value(0), "opened");
    assert_eq!(states.value(1), "merged");

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "MergeRequest", "Project", "1/100/", 2)
        .await;
    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "MergeRequest", "1/100/", 2).await;
    assert_edges_have_traversal_path(ctx, "ASSIGNED", "User", "MergeRequest", "1/100/", 2).await;
    assert_edges_have_traversal_path(ctx, "MERGED_BY", "User", "MergeRequest", "1/100/", 1).await;
    assert_edges_have_traversal_path(
        ctx,
        "IN_MILESTONE",
        "MergeRequest",
        "Milestone",
        "1/100/",
        1,
    )
    .await;
    assert_edges_have_traversal_path(ctx, "REVIEWER", "User", "MergeRequest", "1/100/", 3).await;
    assert_edges_have_traversal_path(ctx, "APPROVED_BY", "User", "MergeRequest", "1/100/", 3).await;
}

pub async fn processes_merge_requests_closing_issues(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO work_items
            (id, iid, title, author_id, state_id, work_item_type_id, confidential,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 'Bug: Login fails', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 'Bug: Signup broken', 1, 1, 0, false, 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 101, 'Fix login bug', 'Fixes login issue', 'fix-login', 'main', 3, 'merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
            (20, 102, 'Fix signup bug', 'Fixes signup issue', 'fix-signup', 'main', 3, 'merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_requests_closing_issues
            (id, merge_request_id, issue_id, project_id, traversal_path,
             created_at, updated_at, _siphon_replicated_at)
        VALUES
            (1, 10, 1, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00'),
            (2, 20, 2, 1000, '1/100/', '2024-01-15 10:00:00', '2024-01-15 10:00:00', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "CLOSES", "MergeRequest", "WorkItem", "1/100/", 2).await;
}

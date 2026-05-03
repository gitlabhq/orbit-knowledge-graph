use arrow::array::{Array, Int64Array, StringArray};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edge_tags_by_target, assert_edges_have_traversal_path, assert_node_count,
    create_namespace, handler_context, namespace_envelope, namespace_handler,
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
             draft, squash, target_project_id, source_project_id, head_pipeline_id,
             latest_merge_request_diff_id, merged_commit_sha, squash_commit_sha,
             updated_by_id, last_edited_by_id,
             author_id, merge_user_id, milestone_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 101, 'Add feature X', 'Implements feature X', 'feature-x', 'main', 1, 'can_be_merged',
             false, true, 1000, 1001, 5001,
             7001, NULL, NULL,
             2, 3,
             1, NULL, 10,
             '1/100/', '2024-01-20 12:00:00'),
            (2, 102, 'Fix bug Y', 'Fixes critical bug', 'fix-y', 'main', 3, 'merged',
             false, false, 1000, 1000, 5002,
             7002, 'abc123def456', 'squash789',
             NULL, NULL,
             2, 1, NULL,
             '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_metrics
            (id, merge_request_id, merged_at, commits_count, added_lines, removed_lines,
             target_project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, NULL,                  3, 120, 40, 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, '2024-01-18 09:30:00', 2,  15,  5, 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_reviewers
            (id, user_id, merge_request_id, created_at, state, project_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 4, 1, '2024-01-20 12:00:00', 0, 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, 5, 1, '2024-01-20 12:00:00', 0, 1000, '1/100/', '2024-01-20 12:00:00'),
            (3, 6, 2, '2024-01-20 12:00:00', 0, 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_approvals
            (id, merge_request_id, user_id, created_at, updated_at, project_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 1, 5, '2024-01-20 12:00:00', '2024-01-20 12:00:00', 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, 2, 3, '2024-01-20 12:00:00', '2024-01-20 12:00:00', 1000, '1/100/', '2024-01-20 12:00:00'),
            (3, 2, 4, '2024-01-20 12:00:00', '2024-01-20 12:00:00', 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_merge_request", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT title, state, project_id, \
             merged_commit_sha, squash_commit_sha, commits_count, added_lines \
             FROM {} FINAL ORDER BY id",
            t("gl_merge_request")
        ))
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

    let project_ids =
        ArrowUtils::get_column_by_name::<Int64Array>(batch, "project_id").expect("project_id");
    assert_eq!(project_ids.value(0), 1000);
    assert_eq!(project_ids.value(1), 1000);

    let merged_sha =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "merged_commit_sha").unwrap();
    assert!(merged_sha.is_null(0));
    assert_eq!(merged_sha.value(1), "abc123def456");

    let squash_sha =
        ArrowUtils::get_column_by_name::<StringArray>(batch, "squash_commit_sha").unwrap();
    assert!(squash_sha.is_null(0));
    assert_eq!(squash_sha.value(1), "squash789");

    let commits = ArrowUtils::get_column_by_name::<Int64Array>(batch, "commits_count")
        .expect("commits_count column");
    assert_eq!(commits.value(0), 3);
    assert_eq!(commits.value(1), 2);

    let added = ArrowUtils::get_column_by_name::<Int64Array>(batch, "added_lines")
        .expect("added_lines column");
    assert_eq!(added.value(0), 120);
    assert_eq!(added.value(1), 15);

    assert_edges_have_traversal_path(ctx, "IN_PROJECT", "MergeRequest", "Project", "1/100/", 2)
        .await;
    // target_project_id is 1000 for both; source_project_id is 1001 for MR 1 (fork) and
    // 1000 for MR 2 (same project) -> two SOURCE_PROJECT edges.
    assert_edges_have_traversal_path(
        ctx,
        "SOURCE_PROJECT",
        "MergeRequest",
        "Project",
        "1/100/",
        2,
    )
    .await;
    assert_edges_have_traversal_path(
        ctx,
        "HAS_HEAD_PIPELINE",
        "MergeRequest",
        "Pipeline",
        "1/100/",
        2,
    )
    .await;
    assert_edges_have_traversal_path(
        ctx,
        "HAS_LATEST_DIFF",
        "MergeRequest",
        "MergeRequestDiff",
        "1/100/",
        2,
    )
    .await;
    // Only MR 1 has updated_by_id / last_edited_by_id set.
    assert_edges_have_traversal_path(ctx, "UPDATED_BY", "User", "MergeRequest", "1/100/", 1).await;
    assert_edges_have_traversal_path(ctx, "LAST_EDITED_BY", "User", "MergeRequest", "1/100/", 1)
        .await;
    assert_edges_have_traversal_path(ctx, "AUTHORED", "User", "MergeRequest", "1/100/", 2).await;
    assert_edges_have_traversal_path(ctx, "MERGED", "User", "MergeRequest", "1/100/", 1).await;
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
    assert_edges_have_traversal_path(ctx, "APPROVED", "User", "MergeRequest", "1/100/", 3).await;
}

pub async fn metric_columns_read_from_siphon_not_stale_denorm(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id,
             metric_diff_size, metric_commits_count, metric_added_lines, metric_removed_lines,
             traversal_path, _siphon_replicated_at)
        VALUES
            (42, 1, 'Stale metrics', 'metrics row updated without parent re-emit',
             'branch', 'main', 1, 'can_be_merged', false, false, 1000, 1,
             NULL, NULL, NULL, NULL,
             '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_metrics
            (id, merge_request_id, diff_size, commits_count, added_lines, removed_lines,
             target_project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 42, 4242, 7, 314, 88, 1000, '1/100/', '2024-01-20 12:30:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT diff_size, commits_count, added_lines, removed_lines \
             FROM {} FINAL WHERE id = 42",
            t("gl_merge_request")
        ))
        .await;
    let batch = &result[0];

    let diff_size =
        ArrowUtils::get_column_by_name::<Int64Array>(batch, "diff_size").expect("diff_size column");
    assert_eq!(
        diff_size.value(0),
        4242,
        "destination diff_size must come from siphon_merge_request_metrics, not the stale NULL on merge_requests",
    );

    let commits = ArrowUtils::get_column_by_name::<Int64Array>(batch, "commits_count")
        .expect("commits_count column");
    assert_eq!(commits.value(0), 7);

    let added = ArrowUtils::get_column_by_name::<Int64Array>(batch, "added_lines")
        .expect("added_lines column");
    assert_eq!(added.value(0), 314);

    let removed = ArrowUtils::get_column_by_name::<Int64Array>(batch, "removed_lines")
        .expect("removed_lines column");
    assert_eq!(removed.value(0), 88);
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

pub async fn processes_standalone_reviewer_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 101, 'Add auth', 'Auth feature', 'feat-auth', 'main', 1, 'can_be_merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
            (20, 102, 'Fix crash', 'Crash fix', 'fix-crash', 'main', 3, 'merged',
             false, false, 1000, 2, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_reviewers
            (id, user_id, merge_request_id, created_at, state, project_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 10, 10, '2024-01-15 10:00:00', 0, 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, 11, 10, '2024-01-15 11:00:00', 0, 1000, '1/100/', '2024-01-20 12:00:00'),
            (3, 10, 20, '2024-01-16 09:00:00', 0, 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "REVIEWER", "User", "MergeRequest", "1/100/", 3).await;
    assert_edge_tags_by_target(
        ctx,
        "REVIEWER",
        "User",
        "MergeRequest",
        "target_tags",
        &[
            (
                10,
                &[
                    "discussion_locked:null",
                    "draft:false",
                    "merge_status:can_be_merged",
                    "squash:false",
                    "state:opened",
                ],
            ),
            (
                20,
                &[
                    "discussion_locked:null",
                    "draft:false",
                    "merge_status:merged",
                    "squash:false",
                    "state:merged",
                ],
            ),
        ],
    )
    .await;
}

pub async fn processes_standalone_approved_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 101, 'Add auth', 'Auth feature', 'feat-auth', 'main', 3, 'merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
            (20, 102, 'Fix crash', 'Crash fix', 'fix-crash', 'main', 3, 'merged',
             false, false, 1000, 2, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_approvals
            (id, merge_request_id, user_id, created_at, updated_at, project_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 10, 10, '2024-01-15 10:00:00', '2024-01-15 10:00:00', 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, 10, 11, '2024-01-15 11:00:00', '2024-01-15 11:00:00', 1000, '1/100/', '2024-01-20 12:00:00'),
            (3, 20, 12, '2024-01-16 09:00:00', '2024-01-16 09:00:00', 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "APPROVED", "User", "MergeRequest", "1/100/", 3).await;
    assert_edge_tags_by_target(
        ctx,
        "APPROVED",
        "User",
        "MergeRequest",
        "target_tags",
        &[
            (
                10,
                &[
                    "discussion_locked:null",
                    "draft:false",
                    "merge_status:merged",
                    "squash:false",
                    "state:merged",
                ],
            ),
            (
                20,
                &[
                    "discussion_locked:null",
                    "draft:false",
                    "merge_status:merged",
                    "squash:false",
                    "state:merged",
                ],
            ),
        ],
    )
    .await;
}

pub async fn processes_standalone_assigned_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 101, 'Add auth', 'Auth feature', 'feat-auth', 'main', 1, 'can_be_merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00'),
            (20, 102, 'Fix crash', 'Crash fix', 'fix-crash', 'main', 3, 'merged',
             false, false, 1000, 2, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_assignees
            (id, user_id, merge_request_id, created_at, project_id,
             traversal_path, _siphon_replicated_at)
        VALUES
            (1, 10, 10, '2024-01-15 10:00:00', 1000, '1/100/', '2024-01-20 12:00:00'),
            (2, 10, 20, '2024-01-16 09:00:00', 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "ASSIGNED", "User", "MergeRequest", "1/100/", 2).await;
    assert_edge_tags_by_target(
        ctx,
        "ASSIGNED",
        "User",
        "MergeRequest",
        "target_tags",
        &[
            (
                10,
                &[
                    "discussion_locked:null",
                    "draft:false",
                    "merge_status:can_be_merged",
                    "squash:false",
                    "state:opened",
                ],
            ),
            (
                20,
                &[
                    "discussion_locked:null",
                    "draft:false",
                    "merge_status:merged",
                    "squash:false",
                    "state:merged",
                ],
            ),
        ],
    )
    .await;
}

pub async fn processes_standalone_has_label_edges(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;

    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, description, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, author_id, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 101, 'Add auth', 'Auth feature', 'feat-auth', 'main', 1, 'can_be_merged',
             false, false, 1000, 1, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_label_links
            (id, label_id, target_id, target_type, created_at, updated_at,
             namespace_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 5, 10, 'MergeRequest', '2024-01-15 10:00:00', '2024-01-15 10:00:00', 100, '1/100/', '2024-01-20 12:00:00'),
            (2, 6, 10, 'MergeRequest', '2024-01-15 10:00:00', '2024-01-15 10:00:00', 100, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_edges_have_traversal_path(ctx, "HAS_LABEL", "MergeRequest", "Label", "1/100/", 2).await;
}

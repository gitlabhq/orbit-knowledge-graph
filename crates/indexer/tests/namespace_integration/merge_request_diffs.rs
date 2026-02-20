//! Integration tests for merge request diff processing in the namespace handler.

use indexer::testkit::TestEnvelopeFactory;
use serial_test::serial;

use crate::common::{
    TestContext, assert_edges_have_traversal_path, create_namespace_payload,
    default_test_watermark, get_boolean_column, get_int64_column, get_namespace_handler,
    get_string_column,
};

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_request_diffs_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_request_diffs
                (id, merge_request_id, state, base_commit_sha, head_commit_sha, start_commit_sha,
                 commits_count, files_count, traversal_path, _siphon_replicated_at)
            VALUES
                (10, 1, 'collected', 'abc123', 'def456', 'ghi789', 3, 5, '1/100/', '2024-01-20 12:00:00'),
                (11, 1, 'collected', 'abc123', 'jkl012', 'ghi789', 4, 6, '1/100/', '2024-01-20 12:00:00')",
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
        .query("SELECT id, merge_request_id, state, commits_count, files_count FROM gl_merge_request_diff ORDER BY id")
        .await;
    assert!(!result.is_empty(), "merge request diffs should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 2);

    let states = get_string_column(batch, "state");
    assert_eq!(states.value(0), "collected");
    assert_eq!(states.value(1), "collected");

    assert_edges_have_traversal_path(
        &context,
        "HAS_DIFF",
        "MergeRequest",
        "MergeRequestDiff",
        "1/100/",
        2,
    )
    .await;
}

#[tokio::test]
#[serial]
async fn namespace_handler_processes_merge_request_diff_files_with_edges() {
    let context = TestContext::new().await;

    context
        .execute(
            "INSERT INTO hierarchy_merge_requests
                (id, iid, title, source_branch, target_branch, state_id, merge_status,
                 draft, squash, target_project_id, traversal_path, version)
            VALUES
                (1, 101, 'Add feature X', 'feature-x', 'main', 1, 'can_be_merged',
                 false, true, 1000, '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_request_diffs
                (id, merge_request_id, state, traversal_path, _siphon_replicated_at)
            VALUES
                (10, 1, 'collected', '1/100/', '2024-01-20 12:00:00')",
        )
        .await;

    context
        .execute(
            "INSERT INTO siphon_merge_request_diff_files
                (merge_request_diff_id, relative_order, old_path, new_path, new_file, renamed_file,
                 deleted_file, too_large, binary, a_mode, b_mode, _siphon_replicated_at)
            VALUES
                (10, 0, 'src/main.rs', 'src/main.rs', false, false, false, false, false, '100644', '100644', '2024-01-20 12:00:00'),
                (10, 1, '', 'src/new_file.rs', true, false, false, false, false, '000000', '100644', '2024-01-20 12:00:00'),
                (10, 2, 'src/old_file.rs', '', false, false, true, false, false, '100644', '000000', '2024-01-20 12:00:00')",
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
        .query("SELECT merge_request_id, merge_request_diff_id, old_path, new_path, new_file, deleted_file FROM gl_merge_request_diff_file ORDER BY old_path")
        .await;
    assert!(!result.is_empty(), "merge request diff files should exist");

    let batch = &result[0];
    assert_eq!(batch.num_rows(), 3);

    let merge_request_ids = get_int64_column(batch, "merge_request_id");
    for i in 0..batch.num_rows() {
        assert_eq!(
            merge_request_ids.value(i),
            1,
            "all diff files should have merge_request_id from the parent diff"
        );
    }

    let new_file_flags = get_boolean_column(batch, "new_file");

    let has_new_file = (0..batch.num_rows()).any(|i| new_file_flags.value(i));
    assert!(has_new_file, "should have at least one new file");

    assert_edges_have_traversal_path(
        &context,
        "HAS_FILE",
        "MergeRequestDiff",
        "MergeRequestDiffFile",
        "1/100/",
        3,
    )
    .await;
}

use arrow::array::{BooleanArray, Int64Array, StringArray};
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, assert_edges_have_traversal_path, assert_node_count, handler_context,
    namespace_envelope, namespace_handler,
};

pub async fn processes_merge_request_diffs_with_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 101, 'Add feature X', 'feature-x', 'main', 1, 'can_be_merged',
             false, true, 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_diffs
            (id, merge_request_id, state, base_commit_sha, head_commit_sha, start_commit_sha,
             commits_count, files_count, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 1, 'collected', 'abc123', 'def456', 'ghi789', 3, 5, '1/100/', '2024-01-20 12:00:00'),
            (11, 1, 'collected', 'abc123', 'jkl012', 'ghi789', 4, 6, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_merge_request_diff", 2).await;

    let result = ctx
        .query(&format!(
            "SELECT state FROM {} FINAL ORDER BY id",
            t("gl_merge_request_diff")
        ))
        .await;
    let states =
        ArrowUtils::get_column_by_name::<StringArray>(&result[0], "state").expect("state column");
    assert_eq!(states.value(0), "collected");
    assert_eq!(states.value(1), "collected");

    assert_edges_have_traversal_path(
        ctx,
        "HAS_DIFF",
        "MergeRequest",
        "MergeRequestDiff",
        "1/100/",
        2,
    )
    .await;
}

pub async fn processes_merge_request_diff_files_with_edges(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO merge_requests
            (id, iid, title, source_branch, target_branch, state_id, merge_status,
             draft, squash, target_project_id, traversal_path, _siphon_replicated_at)
        VALUES
            (1, 101, 'Add feature X', 'feature-x', 'main', 1, 'can_be_merged',
             false, true, 1000, '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_diffs
            (id, merge_request_id, state, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 1, 'collected', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    ctx.execute(
        "INSERT INTO siphon_merge_request_diff_files
            (merge_request_diff_id, relative_order, old_path, new_path, new_file, renamed_file,
             deleted_file, too_large, binary, a_mode, b_mode, traversal_path, _siphon_replicated_at)
        VALUES
            (10, 0, 'src/main.rs', 'src/main.rs', false, false, false, false, false, '100644', '100644', '1/100/', '2024-01-20 12:00:00'),
            (10, 1, '', 'src/new_file.rs', true, false, false, false, false, '000000', '100644', '1/100/', '2024-01-20 12:00:00'),
            (10, 2, 'src/old_file.rs', '', false, false, true, false, false, '100644', '000000', '1/100/', '2024-01-20 12:00:00')",
    )
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .unwrap();

    assert_node_count(ctx, "gl_merge_request_diff_file", 3).await;

    let result = ctx
        .query(&format!(
            "SELECT merge_request_id, new_file FROM {} FINAL ORDER BY old_path",
            t("gl_merge_request_diff_file")
        ))
        .await;
    let batch = &result[0];

    let merge_request_ids = ArrowUtils::get_column_by_name::<Int64Array>(batch, "merge_request_id")
        .expect("merge_request_id column");
    for i in 0..batch.num_rows() {
        assert_eq!(merge_request_ids.value(i), 1);
    }

    let new_file_flags =
        ArrowUtils::get_column_by_name::<BooleanArray>(batch, "new_file").expect("new_file column");
    assert!((0..batch.num_rows()).any(|i| new_file_flags.value(i)));

    assert_edges_have_traversal_path(
        ctx,
        "HAS_FILE",
        "MergeRequestDiff",
        "MergeRequestDiffFile",
        "1/100/",
        3,
    )
    .await;
}

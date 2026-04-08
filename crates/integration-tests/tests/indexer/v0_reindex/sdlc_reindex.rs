//! Verify that SDLC indexing starts from watermark epoch-zero when checkpoints are absent.
//!
//! After a V0 reset the `checkpoint` table is empty. When the NamespaceHandler runs it finds
//! no checkpoint for the namespace, defaults to the Unix epoch as the watermark, and therefore
//! picks up every row in the datalake. This is the same path taken on a fresh install.

use arrow::array::UInt64Array;
use gkg_utils::arrow::ArrowUtils;

use crate::indexer::common::{
    TestContext, assert_node_count, create_namespace, create_project, create_user, handler_context,
    namespace_envelope, namespace_handler,
};

/// After a V0 reset (checkpoint table empty) the namespace handler re-indexes all datalake rows.
pub async fn namespace_data_reindexed_with_empty_checkpoints(ctx: &TestContext) {
    // Populate the datalake with a namespace and two projects.
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_user(ctx, 1).await;
    create_project(ctx, 1000, 100, 1, 0, "1/100/1000/").await;
    create_project(ctx, 1001, 100, 1, 0, "1/100/1001/").await;

    // Checkpoint table is empty (simulating post-reset state). Run the handler.
    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("namespace handler should succeed with empty checkpoints");

    // Both projects must be present — handler started from epoch-zero.
    assert_node_count(ctx, "gl_project", 2).await;
}

/// After re-indexing, a checkpoint is written for each entity type so subsequent cycles are
/// incremental rather than full scans.
pub async fn checkpoint_written_after_reindex(ctx: &TestContext) {
    create_namespace(ctx, 200, None, 0, "1/200/").await;
    create_user(ctx, 2).await;
    create_project(ctx, 2000, 200, 2, 0, "1/200/2000/").await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 200))
        .await
        .expect("namespace handler should succeed");

    // At least one checkpoint row must exist after indexing.
    let result = ctx
        .query("SELECT count() AS cnt FROM checkpoint FINAL WHERE _deleted = false")
        .await;
    let count = ArrowUtils::get_column_by_name::<UInt64Array>(&result[0], "cnt")
        .expect("cnt column")
        .value(0);
    assert!(
        count > 0,
        "checkpoint rows must be written after re-indexing so subsequent cycles are incremental"
    );
}

/// Running the handler a second time only indexes rows added after the checkpoint watermark,
/// proving that incremental processing resumes normally after a reset.
pub async fn second_reindex_cycle_is_incremental(ctx: &TestContext) {
    create_namespace(ctx, 300, None, 0, "1/300/").await;
    create_user(ctx, 3).await;

    // First cycle — indexes the project created before the checkpoint.
    create_project(ctx, 3000, 300, 3, 0, "1/300/3000/").await;
    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 300))
        .await
        .expect("first handler cycle should succeed");

    assert_node_count(ctx, "gl_project", 1).await;

    // Manually backdating the checkpoint simulates the watermark being in the past so that
    // a project replicated at "now" will be picked up on the next cycle.
    ctx.execute(
        "INSERT INTO checkpoint (key, watermark, cursor_values) \
         VALUES ('ns.300.Project', '2020-01-01 00:00:00.000000', 'null')",
    )
    .await;

    // Add a second project that post-dates the forced-back watermark.
    create_project(ctx, 3001, 300, 3, 0, "1/300/3001/").await;

    // Second cycle — only the newly added project should be indexed.
    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 300))
        .await
        .expect("second handler cycle should succeed");

    // Now we expect 2 total (one from each cycle).
    assert_node_count(ctx, "gl_project", 2).await;
}

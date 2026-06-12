//! Incremental-extract windowing: watermark filtering and keyset-cursor resume.
//!
//! These exercise the shared `pull_window` + cursor-DNF path
//! (`crates/indexer/src/modules/sdlc/handler/entity.rs`), which is
//! entity-agnostic, so one representative entity per cursor arity covers it:
//! `User` for the single-column key, `Group` for the composite
//! `[traversal_path, id]` key.
//!
//! Each subtest is shaped as seed -> run -> assert so it maps onto a future
//! YAML scenario. These three need no format additions to port: the scenario
//! format already seeds the `checkpoint` table and asserts the surviving id-set
//! via `expect.nodes.rows`. They stay in Rust only to share a container with the
//! partitioning mechanics that are not yet portable.

use arrow::array::Int64Array;
use gkg_utils::arrow::ArrowUtils;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_namespace, global_envelope, global_handler, handler_context,
    namespace_envelope, namespace_handler,
};

pub(super) async fn insert_user_at(ctx: &TestContext, id: i64, replicated_at: &str) {
    ctx.execute(&format!(
        "INSERT INTO siphon_users \
         (id, email, username, name, state, organization_id, _siphon_replicated_at) \
         VALUES ({id}, 'u{id}@t', 'u{id}', 'User {id}', 'active', 1, '{replicated_at}')"
    ))
    .await;
}

async fn indexed_user_ids(ctx: &TestContext) -> Vec<i64> {
    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_user")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    (0..ids.len()).map(|i| ids.value(i)).collect()
}

pub async fn incremental_watermark_filters_old_rows(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-19 00:00:00.000000', 'null')",
        t("checkpoint")
    ))
    .await;

    insert_user_at(ctx, 1, "2024-01-18 12:00:00").await;
    insert_user_at(ctx, 2, "2024-01-20 12:00:00").await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    assert_eq!(
        indexed_user_ids(ctx).await,
        vec![2],
        "watermark at 2024-01-19 excludes user 1 (replicated 2024-01-18), includes user 2"
    );
}

/// One cursor-bearing checkpoint pins all three resume bounds at once. The
/// window is `(floor, target]` with a single-column cursor `id > 2`, so the
/// surviving id-set is correct only if the cursor DNF, the floor lower bound,
/// and the inclusive watermark upper bound all hold.
pub async fn resume_honors_cursor_floor_and_watermark_boundary(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('global.User', '2024-01-20 12:00:00.000000', \
                 '{{\"c\":[\"2\"],\"f\":\"2024-01-10T00:00:00Z\"}}')",
        t("checkpoint")
    ))
    .await;

    insert_user_at(ctx, 1, "2024-01-15 00:00:00").await;
    insert_user_at(ctx, 2, "2024-01-15 00:00:00").await;
    insert_user_at(ctx, 3, "2024-01-05 00:00:00").await;
    insert_user_at(ctx, 4, "2024-01-15 00:00:00").await;
    insert_user_at(ctx, 5, "2024-01-20 12:00:00").await;

    global_handler(ctx)
        .await
        .handle(handler_context(ctx), global_envelope())
        .await
        .expect("handler should succeed");

    assert_eq!(
        indexed_user_ids(ctx).await,
        vec![4, 5],
        "cursor drops 1-2; floor drops user 3 (replicated before 2024-01-10); \
         user 5 at exactly the target watermark must still index"
    );
}

/// Group plans sort by `[traversal_path, id]`, so a saved cursor
/// `["1/100/102/", "102"]` must emit the DNF
/// `(traversal_path > '1/100/102/') OR (traversal_path = '1/100/102/' AND id > '102')`.
/// Groups 100-102 (lexicographically <= cursor) must be skipped; 103-104 process.
pub async fn composite_keyset_resume_skips_processed_groups(ctx: &TestContext) {
    create_namespace(ctx, 100, None, 0, "1/100/").await;
    create_namespace(ctx, 101, Some(100), 0, "1/100/101/").await;
    create_namespace(ctx, 102, Some(100), 0, "1/100/102/").await;
    create_namespace(ctx, 103, Some(100), 0, "1/100/103/").await;
    create_namespace(ctx, 104, Some(100), 0, "1/100/104/").await;

    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values) \
         VALUES ('ns.100.Group', '2024-01-21 00:00:00.000000', '{{\"c\":[\"1/100/102/\", \"102\"]}}')",
        t("checkpoint")
    ))
    .await;

    namespace_handler(ctx)
        .await
        .handle(handler_context(ctx), namespace_envelope(1, 100))
        .await
        .expect("handler should succeed");

    let result = ctx
        .query(&format!(
            "SELECT id FROM {} FINAL ORDER BY id",
            t("gl_group")
        ))
        .await;
    let ids = ArrowUtils::get_column_by_name::<Int64Array>(&result[0], "id").expect("id column");
    let processed: Vec<i64> = (0..ids.len()).map(|i| ids.value(i)).collect();
    assert_eq!(
        processed,
        vec![103, 104],
        "composite cursor at (1/100/102/, 102) must skip groups 100-102 and process 103-104"
    );
}

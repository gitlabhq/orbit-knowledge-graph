//! Canary tests for the integration test infrastructure itself.
//!
//! These validate that `TestContext`, `optimize_all`, `run_subtests_shared!`,
//! `run_subtests!`, and stale container cleanup all work correctly.

use arrow::array::UInt64Array;
use gkg_utils::arrow::ArrowUtils;

use crate::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL};
use integration_testkit::{TestContext, run_subtests, run_subtests_shared, t};

async fn seed(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state) VALUES
         (1, 'canary', 'Canary Bird', 'active')",
        t("gl_user")
    ))
    .await;
    ctx.optimize_all().await;
}

async fn shared_subtest_reads_seeded_data(ctx: &TestContext) {
    let batches = ctx
        .query(&format!(
            "SELECT id, username FROM {} ORDER BY id",
            t("gl_user")
        ))
        .await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

async fn shared_subtest_sees_same_data(ctx: &TestContext) {
    let batches = ctx
        .query(&format!("SELECT count() AS cnt FROM {}", t("gl_user")))
        .await;
    let col =
        ArrowUtils::get_column_by_name::<UInt64Array>(&batches[0], "cnt").expect("cnt column");
    assert_eq!(col.value(0), 1);
}

async fn forked_subtest_can_write(ctx: &TestContext) {
    seed(ctx).await;
    ctx.execute(&format!(
        "INSERT INTO {} (id, username, name, state) VALUES
         (2, 'forked', 'Forked User', 'active')",
        t("gl_user")
    ))
    .await;
    let batches = ctx
        .query(&format!("SELECT count() AS cnt FROM {}", t("gl_user")))
        .await;
    let col =
        ArrowUtils::get_column_by_name::<UInt64Array>(&batches[0], "cnt").expect("cnt column");
    assert_eq!(col.value(0), 2);
}

async fn forked_write_does_not_leak_to_shared(ctx: &TestContext) {
    let batches = ctx
        .query(&format!("SELECT count() AS cnt FROM {}", t("gl_user")))
        .await;
    let col =
        ArrowUtils::get_column_by_name::<UInt64Array>(&batches[0], "cnt").expect("cnt column");
    assert_eq!(
        col.value(0),
        1,
        "shared DB should still have only the seed row"
    );
}

#[tokio::test]
async fn infra_canary() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, *GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    // Shared subtests see the seeded data, don't fork.
    run_subtests_shared!(
        &ctx,
        shared_subtest_reads_seeded_data,
        shared_subtest_sees_same_data,
    );

    // Forked subtest gets its own DB and can write without affecting shared.
    run_subtests!(&ctx, forked_subtest_can_write);

    // Verify the shared DB wasn't contaminated by the forked write,
    // and that fetch_arrow_with_summary returns ClickHouse stats.
    run_subtests_shared!(
        &ctx,
        forked_write_does_not_leak_to_shared,
        fetch_arrow_with_summary_returns_stats,
    );
}

async fn fetch_arrow_with_summary_returns_stats(ctx: &TestContext) {
    let client = ctx.create_client();
    let (batches, summary) = client
        .query(&format!("SELECT id, username FROM {}", t("gl_user")))
        .fetch_arrow_with_summary()
        .await
        .expect("query should succeed");

    assert!(!batches.is_empty(), "should return at least one batch");

    let summary = summary.expect("summary should be present");
    assert!(
        summary.read_rows().unwrap_or(0) > 0,
        "read_rows should be > 0"
    );
    assert!(
        summary.read_bytes().unwrap_or(0) > 0,
        "read_bytes should be > 0"
    );
}

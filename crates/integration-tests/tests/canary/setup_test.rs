//! Canary tests for the integration test infrastructure itself.
//!
//! These validate that `TestContext`, `optimize_all`, `run_subtests_shared!`,
//! `run_subtests!`, and stale container cleanup all work correctly.

use crate::common::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL};
use integration_testkit::{TestContext, run_subtests, run_subtests_shared};

async fn seed(ctx: &TestContext) {
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state) VALUES
         (1, 'canary', 'Canary Bird', 'active')",
    )
    .await;
    ctx.optimize_all().await;
}

async fn shared_subtest_reads_seeded_data(ctx: &TestContext) {
    let batches = ctx.query("SELECT id, username FROM gl_user ORDER BY id").await;
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
}

async fn shared_subtest_sees_same_data(ctx: &TestContext) {
    let batches = ctx.query("SELECT count() AS cnt FROM gl_user").await;
    let col = integration_testkit::get_uint64_column(&batches[0], "cnt");
    assert_eq!(col.value(0), 1);
}

async fn forked_subtest_can_write(ctx: &TestContext) {
    seed(ctx).await;
    ctx.execute(
        "INSERT INTO gl_user (id, username, name, state) VALUES
         (2, 'forked', 'Forked User', 'active')",
    )
    .await;
    let batches = ctx.query("SELECT count() AS cnt FROM gl_user").await;
    let col = integration_testkit::get_uint64_column(&batches[0], "cnt");
    assert_eq!(col.value(0), 2);
}

async fn forked_write_does_not_leak_to_shared(ctx: &TestContext) {
    let batches = ctx.query("SELECT count() AS cnt FROM gl_user").await;
    let col = integration_testkit::get_uint64_column(&batches[0], "cnt");
    assert_eq!(col.value(0), 1, "shared DB should still have only the seed row");
}

#[tokio::test]
async fn infra_canary() {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, GRAPH_SCHEMA_SQL]).await;
    seed(&ctx).await;

    // Shared subtests see the seeded data, don't fork.
    run_subtests_shared!(
        &ctx,
        shared_subtest_reads_seeded_data,
        shared_subtest_sees_same_data,
    );

    // Forked subtest gets its own DB and can write without affecting shared.
    run_subtests!(&ctx, forked_subtest_can_write);

    // Verify the shared DB wasn't contaminated by the forked write.
    run_subtests_shared!(&ctx, forked_write_does_not_leak_to_shared);
}

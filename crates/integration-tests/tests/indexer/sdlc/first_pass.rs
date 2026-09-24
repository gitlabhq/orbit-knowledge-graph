use clickhouse_client::FromArrowColumn;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_user, entity_handler_with_partitions, global_envelope, handler_context,
};

async fn attempts(ctx: &TestContext, key: &str) -> Vec<i64> {
    let result = ctx
        .query(&format!(
            "SELECT attempts FROM {} FINAL WHERE key = '{key}' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    i64::extract_column(&result, 0).expect("attempts")
}

async fn indexed_keys(ctx: &TestContext, key: &str) -> Vec<String> {
    let result = ctx
        .query(&format!(
            "SELECT key FROM {} FINAL \
             WHERE key = '{key}' AND indexed_at IS NOT NULL AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    String::extract_column(&result, 0).expect("key")
}

async fn cursor_values(ctx: &TestContext, key: &str) -> Vec<String> {
    let result = ctx
        .query(&format!(
            "SELECT cursor_values FROM {} FINAL WHERE key = '{key}' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    String::extract_column(&result, 0).expect("cursor_values")
}

async fn insert_checkpoint(ctx: &TestContext, key: &str, cursor_values: &str) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, _version) \
         VALUES ('{key}', '2024-01-20 12:00:00.000000', '{cursor_values}', \
                 '2024-01-20 12:00:00.000000')",
        t("checkpoint")
    ))
    .await;
}

pub async fn completed_first_pass_keeps_its_attempts_and_sets_indexed_at(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("partitioned handler should succeed");

    assert_eq!(attempts(ctx, "global.User").await, [1]);
    assert_eq!(indexed_keys(ctx, "global.User").await, ["global.User"]);
}

pub async fn unfinished_first_pass_counts_each_attempt(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }
    insert_checkpoint(ctx, "global.User.p5of6", r#"{"c":["6"]}"#).await;
    let handler = entity_handler_with_partitions(ctx, "User", 4).await;

    for _ in 0..2 {
        handler
            .handle(handler_context(), global_envelope())
            .await
            .expect("deferred consolidation should succeed");
    }

    assert_eq!(attempts(ctx, "global.User").await, [2]);
    assert_eq!(cursor_values(ctx, "global.User").await, [r#"{"c":[]}"#]);
    assert!(indexed_keys(ctx, "global.User").await.is_empty());
}

pub async fn incremental_run_records_no_attempt(ctx: &TestContext) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, indexed_at, _version) \
         VALUES ('global.User', '2024-01-20 12:00:00.000000', 'null', \
                 '2024-01-20 12:00:00.000000', '2024-01-20 12:00:00.000000')",
        t("checkpoint")
    ))
    .await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("incremental handler should succeed");

    assert_eq!(attempts(ctx, "global.User").await, [0]);
}

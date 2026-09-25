use std::sync::Arc;

use chrono::{DateTime, Utc};
use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::{CheckpointStore, ClickHouseCheckpointStore};
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_user, entity_handler_with_partitions, global_envelope, handler_context,
};

const COMPLETED_AT: &str = "'2024-01-20 12:00:00.000000'";

async fn checkpoint_column<T: FromArrowColumn>(
    ctx: &TestContext,
    key: &str,
    column: &str,
) -> Vec<T> {
    let result = ctx
        .query(&format!(
            "SELECT {column} FROM {} FINAL WHERE key = '{key}' AND _deleted = false",
            t("checkpoint")
        ))
        .await;
    T::extract_column(&result, 0).expect(column)
}

async fn insert_checkpoint(ctx: &TestContext, key: &str, cursor_values: &str, indexed_at: &str) {
    insert_checkpoint_row(ctx, key, cursor_values, indexed_at, 0, false).await;
}

async fn insert_checkpoint_row(
    ctx: &TestContext,
    key: &str,
    cursor_values: &str,
    indexed_at: &str,
    version_seconds: i64,
    deleted: bool,
) {
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, indexed_at, _version, _deleted) \
         VALUES ('{key}', '2024-01-20 12:00:00.000000', '{cursor_values}', {indexed_at}, \
                 toDateTime64('2024-01-20 12:00:00', 6, 'UTC') + {version_seconds}, {deleted})",
        t("checkpoint")
    ))
    .await;
}

fn checkpoint_store(ctx: &TestContext) -> ClickHouseCheckpointStore {
    ClickHouseCheckpointStore::new(Arc::new(ctx.config.build_client()))
}

pub async fn stale_page_write_after_completion_keeps_indexed_at(ctx: &TestContext) {
    insert_checkpoint_row(ctx, "global.User", "null", COMPLETED_AT, 0, false).await;
    insert_checkpoint_row(ctx, "global.User", r#"{"c":["6"]}"#, "NULL", 1, false).await;

    let checkpoint = checkpoint_store(ctx)
        .load("global.User")
        .await
        .expect("load")
        .expect("checkpoint exists");

    assert!(checkpoint.indexed_at.is_some());
    assert_eq!(checkpoint.cursor_values, Some(vec!["6".to_string()]));
}

pub async fn tombstoned_key_does_not_resurrect_an_old_completion(ctx: &TestContext) {
    insert_checkpoint_row(ctx, "global.User", "null", COMPLETED_AT, 0, false).await;
    insert_checkpoint_row(ctx, "global.User", "", "NULL", 1, true).await;
    insert_checkpoint_row(ctx, "global.User", "null", "NULL", 2, false).await;

    let checkpoint = checkpoint_store(ctx)
        .load("global.User")
        .await
        .expect("load")
        .expect("checkpoint exists");

    assert!(checkpoint.indexed_at.is_none());
}

pub async fn completed_first_pass_resets_attempts_and_sets_indexed_at(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("partitioned handler should succeed");

    assert_eq!(
        checkpoint_column::<i64>(ctx, "global.User", "attempts").await,
        [0]
    );
    assert!(matches!(
        checkpoint_column::<Option<DateTime<Utc>>>(ctx, "global.User", "indexed_at").await[..],
        [Some(_)]
    ));
}

pub async fn unfinished_first_pass_counts_each_attempt(ctx: &TestContext) {
    for id in 1..=12 {
        create_user(ctx, id).await;
    }
    insert_checkpoint(ctx, "global.User.p5of6", r#"{"c":["6"]}"#, "NULL").await;
    let handler = entity_handler_with_partitions(ctx, "User", 4).await;

    for _ in 0..2 {
        handler
            .handle(handler_context(), global_envelope())
            .await
            .expect("deferred consolidation should succeed");
    }

    assert_eq!(
        checkpoint_column::<i64>(ctx, "global.User", "attempts").await,
        [2]
    );
    assert_eq!(
        checkpoint_column::<Option<DateTime<Utc>>>(ctx, "global.User", "indexed_at").await,
        [None]
    );
}

pub async fn incremental_run_keeps_indexed_at_and_ends_with_zero_attempts(ctx: &TestContext) {
    insert_checkpoint(ctx, "global.User", "null", COMPLETED_AT).await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("incremental handler should succeed");

    assert_eq!(
        checkpoint_column::<i64>(ctx, "global.User", "attempts").await,
        [0]
    );
    assert!(matches!(
        checkpoint_column::<Option<DateTime<Utc>>>(ctx, "global.User", "indexed_at").await[..],
        [Some(_)]
    ));
}

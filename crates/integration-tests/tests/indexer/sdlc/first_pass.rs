use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use integration_testkit::t;

use crate::indexer::common::{
    TestContext, create_user, entity_handler_with_partitions, global_envelope, handler_context,
};

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
    ctx.execute(&format!(
        "INSERT INTO {} (key, watermark, cursor_values, indexed_at, _version) \
         VALUES ('{key}', '2024-01-20 12:00:00.000000', '{cursor_values}', {indexed_at}, \
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

    assert_eq!(
        checkpoint_column::<i64>(ctx, "global.User", "attempts").await,
        [1]
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

pub async fn incremental_run_records_no_attempt(ctx: &TestContext) {
    insert_checkpoint(ctx, "global.User", "null", "'2024-01-20 12:00:00.000000'").await;

    entity_handler_with_partitions(ctx, "User", 4)
        .await
        .handle(handler_context(), global_envelope())
        .await
        .expect("incremental handler should succeed");

    assert_eq!(
        checkpoint_column::<i64>(ctx, "global.User", "attempts").await,
        [0]
    );
}

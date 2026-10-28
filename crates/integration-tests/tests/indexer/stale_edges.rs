use clickhouse_client::FromArrowColumn;
use indexer::orchestrator::scheduled::ScheduledTask;
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};

use super::common::handlers::stale_edge_task;

const EMITTER_VERSION: &str = "2099-01-01 00:00:00.000000";

async fn count(context: &TestContext, sql: &str) -> i64 {
    let result = context.query(sql).await;
    i64::extract_column(&result, 0).unwrap()[0]
}

#[tokio::test]
async fn edge_written_at_the_emitter_version_outlives_a_sweep_tombstone_that_lands_after_it() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let edge = t("gl_ci_edge");
    let edge_columns = "traversal_path, source_id, source_kind, relationship_kind, target_id, \
                        target_kind, _version, _deleted";
    let current_edge = format!(
        "('1/9970/', 10, 'MergeRequest', 'HAS_HEAD_PIPELINE', 400, 'Pipeline', '{EMITTER_VERSION}', false)"
    );

    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, head_pipeline_id, _version, _deleted) \
             VALUES (10, '1/9970/', 400, '{EMITTER_VERSION}', false)",
            t("gl_merge_request")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {edge} ({edge_columns}) VALUES \
             ('1/9970/', 10, 'MergeRequest', 'HAS_HEAD_PIPELINE', 400, 'Pipeline', \
              '2024-01-15 00:00:00.000000', false)"
        ))
        .await;

    stale_edge_task(&context).run().await.unwrap();

    context
        .execute(&format!(
            "CREATE TABLE sweep_tombstones ENGINE = Memory \
             AS SELECT * FROM {edge} WHERE _deleted = true"
        ))
        .await;
    assert_eq!(
        count(&context, "SELECT toInt64(count()) FROM sweep_tombstones").await,
        1,
        "the sweep must tombstone the edge it read as stale"
    );
    context
        .execute(&format!(
            "DELETE FROM {edge} WHERE _deleted = true SETTINGS lightweight_deletes_sync = 1"
        ))
        .await;

    context
        .execute(&format!(
            "INSERT INTO {edge} ({edge_columns}) VALUES {current_edge}"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {edge} SELECT * FROM sweep_tombstones"
        ))
        .await;

    assert_eq!(
        count(
            &context,
            &format!(
                "SELECT toInt64(count()) FROM {edge} FINAL \
                 WHERE relationship_kind = 'HAS_HEAD_PIPELINE' AND source_id = 10 \
                   AND target_id = 400 AND _deleted = false"
            )
        )
        .await,
        1,
        "the indexer's edge at the emitter version must win over the sweep tombstone"
    );
}

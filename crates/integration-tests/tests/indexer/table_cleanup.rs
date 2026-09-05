use std::sync::Arc;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::modules::code::config::CodeTableNames;
use indexer::orchestrator::scheduled::table_cleanup::TableCleanup;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_server_config::TableCleanupConfig;

fn build_cleanup_task(context: &TestContext) -> TableCleanup {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let code_tables = CodeTableNames::from_ontology(&ontology).unwrap();
    let checkpoints = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
        context.config.build_client(),
    )));
    TableCleanup::new(
        context.config.build_client(),
        &ontology,
        &code_tables,
        checkpoints,
        ScheduledTaskMetrics::new(),
        TableCleanupConfig::default(),
    )
}

/// Two inserts, so the hidden rows survive insert-time collapsing and land in a part of their own.
async fn seed_users_with_tombstones(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES \
             (1, 'u1', now64(6) - INTERVAL 1 DAY, false), \
             (2, 'u2', now64(6) - INTERVAL 30 DAY, false), \
             (3, 'u3', now64(6) - INTERVAL 1 DAY, false)",
            t("gl_user")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES \
             (1, 'u1', now64(6) - INTERVAL 1 HOUR, true), \
             (2, 'u2', now64(6) - INTERVAL 10 DAY, true)",
            t("gl_user")
        ))
        .await;
}

async fn user_rows(context: &TestContext) -> Vec<(i64, i64)> {
    let result = context
        .query(&format!(
            "SELECT id, toInt64(_deleted) FROM {} ORDER BY id, _version",
            t("gl_user")
        ))
        .await;
    let ids = i64::extract_column(&result, 0).unwrap();
    let deleted = i64::extract_column(&result, 1).unwrap();
    ids.into_iter().zip(deleted).collect()
}

#[tokio::test]
async fn runs_on_every_table_of_an_empty_schema() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;

    build_cleanup_task(&context).run().await.unwrap();
}

#[tokio::test]
async fn collapses_fresh_tombstones_and_purges_expired_ones() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_users_with_tombstones(&context).await;

    build_cleanup_task(&context).run().await.unwrap();

    assert_eq!(user_rows(&context).await, vec![(1, 1), (3, 0)]);
}

#[tokio::test]
async fn second_pass_leaves_a_clean_table_unchanged() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_users_with_tombstones(&context).await;
    let task = build_cleanup_task(&context);

    task.run().await.unwrap();
    task.run().await.unwrap();

    assert_eq!(user_rows(&context).await, vec![(1, 1), (3, 0)]);
}

#[tokio::test]
async fn skips_tables_that_do_not_declare_both_block_columns() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    context
        .execute(&format!(
            "ALTER TABLE {} MODIFY SETTING enable_block_number_column = 0",
            t("gl_user")
        ))
        .await;
    seed_users_with_tombstones(&context).await;

    build_cleanup_task(&context).run().await.unwrap();

    assert_eq!(
        user_rows(&context).await,
        vec![(1, 0), (1, 1), (2, 0), (2, 1), (3, 0)]
    );
}

/// Mirrors ClickHouse Cloud, where merges persist `_block_offset` alone and rows from different blocks share a patch identity.
#[tokio::test]
async fn skips_tables_whose_merged_parts_persist_only_the_block_offset() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    context
        .execute(&format!(
            "ALTER TABLE {} MODIFY SETTING enable_block_number_column = 0",
            t("gl_user")
        ))
        .await;
    seed_users_with_tombstones(&context).await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, username, _version, _deleted) VALUES (4, 'u4', now64(6), false)",
            t("gl_user")
        ))
        .await;
    context
        .execute(&format!("OPTIMIZE TABLE {} FINAL", t("gl_user")))
        .await;
    context
        .execute(&format!(
            "ALTER TABLE {} MODIFY SETTING enable_block_number_column = 1",
            t("gl_user")
        ))
        .await;

    build_cleanup_task(&context).run().await.unwrap();

    assert_eq!(
        user_rows(&context).await,
        vec![(1, 1), (2, 1), (3, 0), (4, 0)]
    );
}

#[tokio::test]
async fn removes_the_superseded_code_snapshot_of_a_checkpointed_project() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, _version) \
             VALUES ('1/100/', 100, 'main', 7, 'abc', '2026-01-02 00:00:00', 1)",
            t("code_indexing_checkpoint")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, project_id, name, _version) \
             VALUES (1, '1/100/', 100, 'main', '2026-01-02 00:00:05')",
            t("gl_branch")
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, project_id, branch, fqn, name, _version) VALUES \
             (1, '1/100/', 100, 'main', 'old', 'old', '2026-01-01 00:00:00'), \
             (2, '1/100/', 100, 'main', 'new', 'new', '2026-01-02 00:00:00')",
            t("gl_definition")
        ))
        .await;

    build_cleanup_task(&context).run().await.unwrap();

    let result = context
        .query(&format!(
            "SELECT id FROM {} ORDER BY id",
            t("gl_definition")
        ))
        .await;
    assert_eq!(i64::extract_column(&result, 0).unwrap(), vec![2]);
}

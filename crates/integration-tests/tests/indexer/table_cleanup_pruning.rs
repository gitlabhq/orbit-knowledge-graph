use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::modules::code::config::CodeTableNames;
use indexer::orchestrator::scheduled::table_cleanup::TableCleanup;
use indexer::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics};
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext, t};
use orbit_migrations::version::{
    SCHEMA_VERSION, ensure_version_table, mark_version_active, mark_version_migrating,
};
use orbit_server_config::TableCleanupConfig;

const SCOPES: usize = 400;
const ROWS_PER_SCOPE: usize = 4096;

async fn build_cleanup_task(context: &TestContext, active: bool) -> TableCleanup {
    let client = context.config.build_client();
    ensure_version_table(&client).await.unwrap();
    if active {
        mark_version_active(&client, *SCHEMA_VERSION).await.unwrap();
    } else {
        mark_version_migrating(&client, *SCHEMA_VERSION)
            .await
            .unwrap();
    }
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

fn scope_path(scope: usize) -> String {
    format!("1/7000/{}/", 1000 + scope)
}

/// Many scopes of many granules each, so a statement that reads the whole table shows up in `SelectedMarks`.
async fn seed_definitions(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, project_id, branch, fqn, name, _version) \
             SELECT number, concat('1/7000/', toString(1000 + intDiv(number, {ROWS_PER_SCOPE})), '/'), \
                    1000 + intDiv(number, {ROWS_PER_SCOPE}), 'main', concat('fqn', toString(number)), 'n', \
                    toDateTime64('2026-01-02 00:00:00', 6, 'UTC') \
             FROM numbers({})",
            t("gl_definition"),
            SCOPES * ROWS_PER_SCOPE
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, project_id, branch, fqn, name, _version) \
             SELECT 10000000 + number, '{}', 1000, 'main', concat('old', toString(number)), 'n', \
                    toDateTime64('2026-01-01 00:00:00', 6, 'UTC') \
             FROM numbers({ROWS_PER_SCOPE})",
            t("gl_definition"),
            scope_path(0)
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, _version) \
             VALUES ('{}', 1000, 'main', 7, 'abc', '2026-01-02 00:00:00', 1)",
            t("code_indexing_checkpoint"),
            scope_path(0)
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, traversal_path, project_id, name, _version) \
             VALUES (1, '{}', 1000, 'main', '2026-01-02 00:00:05')",
            t("gl_branch"),
            scope_path(0)
        ))
        .await;
}

async fn seed_notes(context: &TestContext) {
    context
        .execute(&format!(
            "INSERT INTO {} (id, note, traversal_path, _version) \
             SELECT number, 'n', concat('1/7000/', toString(1000 + intDiv(number, {ROWS_PER_SCOPE})), '/'), \
                    now64(6) - INTERVAL 1 DAY \
             FROM numbers({})",
            t("gl_note"),
            SCOPES * ROWS_PER_SCOPE
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {} (id, note, traversal_path, _version, _deleted) \
             SELECT number, 'n', '{}', now64(6) - INTERVAL 1 HOUR, true FROM numbers(100)",
            t("gl_note"),
            scope_path(0)
        ))
        .await;
}

async fn count_rows(context: &TestContext, table: &str) -> i64 {
    let result = context
        .query(&format!("SELECT toInt64(count()) FROM {}", t(table)))
        .await;
    i64::extract_column(&result, 0).unwrap()[0]
}

/// `(selected marks of every DELETE on the table, marks of the table)`.
async fn selected_marks(context: &TestContext, table: &str) -> (i64, i64) {
    context.execute("SYSTEM FLUSH LOGS").await;
    let result = context
        .query(&format!(
            "SELECT toInt64(sum(ProfileEvents['SelectedMarks'])), toInt64(count()) FROM system.query_log \
             WHERE type = 'QueryFinish' AND query LIKE 'DELETE FROM {0} WHERE %' AND current_database = currentDatabase()",
            t(table)
        ))
        .await;
    let selected = i64::extract_column(&result, 0).unwrap()[0];
    let statements = i64::extract_column(&result, 1).unwrap()[0];
    assert!(statements > 0, "no DELETE statement on {table} was logged");
    let result = context
        .query(&format!(
            "SELECT toInt64(sum(marks)) FROM system.parts WHERE database = currentDatabase() AND table = '{}' AND active",
            t(table)
        ))
        .await;
    (selected, i64::extract_column(&result, 0).unwrap()[0])
}

#[tokio::test]
async fn code_snapshot_delete_reads_only_the_granules_of_its_scopes() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_definitions(&context).await;
    let before = count_rows(&context, "gl_definition").await;

    build_cleanup_task(&context, true).await.run().await.unwrap();

    assert_eq!(
        count_rows(&context, "gl_definition").await,
        before - ROWS_PER_SCOPE as i64
    );
    let (selected, total) = selected_marks(&context, "gl_definition").await;
    assert!(total > 1000, "table has {total} marks");
    assert!(
        selected * 10 < total,
        "the code snapshot statement selected {selected} of {total} marks"
    );
}

#[tokio::test]
async fn tombstone_collapse_reads_only_the_granules_of_its_paths() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_notes(&context).await;
    let before = count_rows(&context, "gl_note").await;

    build_cleanup_task(&context, true).await.run().await.unwrap();

    assert_eq!(count_rows(&context, "gl_note").await, before - 100);
    let (selected, total) = selected_marks(&context, "gl_note").await;
    assert!(total > 1000, "table has {total} marks");
    assert!(
        selected * 10 < total,
        "the collapse statement selected {selected} of {total} marks"
    );
}

#[tokio::test]
async fn waits_while_the_schema_version_is_still_migrating() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_notes(&context).await;
    let before = count_rows(&context, "gl_note").await;

    build_cleanup_task(&context, false).await.run().await.unwrap();

    assert_eq!(count_rows(&context, "gl_note").await, before);
}

/// A part still queued for a mutation is renamed when its turn comes, which would turn a fresh patch into join mode.
#[tokio::test]
async fn defers_a_table_with_a_pending_mutation_and_catches_up_afterwards() {
    let context = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    seed_notes(&context).await;
    let before = count_rows(&context, "gl_note").await;
    context
        .execute(&format!("SYSTEM STOP MERGES {}", t("gl_note")))
        .await;
    context
        .execute(&format!(
            "ALTER TABLE {} UPDATE note = note WHERE id = 0 SETTINGS mutations_sync = 0",
            t("gl_note")
        ))
        .await;
    let task = build_cleanup_task(&context, true).await;

    task.run().await.unwrap();
    assert_eq!(count_rows(&context, "gl_note").await, before);

    context
        .execute(&format!("SYSTEM START MERGES {}", t("gl_note")))
        .await;
    for _ in 0..100 {
        let result = context
            .query(&format!(
                "SELECT toInt64(count()) FROM system.mutations WHERE database = currentDatabase() AND table = '{}' AND NOT is_done",
                t("gl_note")
            ))
            .await;
        if i64::extract_column(&result, 0).unwrap()[0] == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    task.run().await.unwrap();

    assert_eq!(count_rows(&context, "gl_note").await, before - 100);
}

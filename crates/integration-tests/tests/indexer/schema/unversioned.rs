use std::time::Duration;

use clickhouse_client::FromArrowColumn;
use indexer::schema::migration::{
    create_unversioned_tables, drop_refreshable_views_for_version,
    replace_refreshable_views_for_version,
};
use indexer::schema::version::{SCHEMA_VERSION, ensure_version_table, write_schema_version};
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, t};

const ROUNDING_TOLERANCE_BYTES: i64 = 8;

#[tokio::test]
async fn creates_namespace_storage_table_and_refreshable_view() {
    let context = create_test_context_with_graph_tables().await;
    create_namespace_storage_schema(&context).await;

    for name in namespace_storage_table_and_view_names() {
        assert_eq!(get_table_or_view_count(&context, &name).await, 1, "{name}");
    }
}

#[tokio::test]
async fn replacing_refreshable_view_preserves_snapshot_rows() {
    let context = create_test_context_with_graph_tables().await;
    create_namespace_storage_schema(&context).await;
    context
        .execute(
            "INSERT INTO namespace_storage_snapshot \
             (snapshot_date, schema_version, logical_table, top_level_namespace, compressed_bytes) \
             VALUES (today(), 1, 'gl_note', '1/111', 42)",
        )
        .await;

    replace_namespace_storage_view(&context).await;

    assert_eq!(
        query_first_i64_or_zero(
            &context,
            "SELECT toInt64(compressed_bytes) FROM namespace_storage_snapshot \
             WHERE compressed_bytes = 42",
        )
        .await,
        42
    );
}

#[tokio::test]
async fn drops_and_recreates_versioned_refreshable_view() {
    let context = create_test_context_with_graph_tables().await;
    create_namespace_storage_schema(&context).await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let view_name = t("namespace_storage_snapshot_refresh");

    drop_refreshable_views_for_version(&context.create_client(), &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
    assert_eq!(get_table_or_view_count(&context, &view_name).await, 0);

    replace_namespace_storage_view(&context).await;
    assert_eq!(get_table_or_view_count(&context, &view_name).await, 1);
}

#[tokio::test]
async fn attributes_compressed_bytes_to_top_level_namespaces() {
    let context = create_test_context_with_graph_tables().await;
    create_namespace_storage_schema(&context).await;
    insert_rows_into_namespace_attributable_tables(&context).await;
    insert_note_and_edge_rows_for_two_namespaces(&context).await;
    context.optimize_all().await;
    refresh_namespace_storage_snapshot_and_wait(&context).await;

    assert_eq!(
        query_first_i64_or_zero(
            &context,
            "SELECT toInt64(count()) FROM namespace_storage_snapshot \
             WHERE compressed_bytes = 0",
        )
        .await,
        0
    );

    for logical_table in ["gl_note", "gl_edge"] {
        let attributed_bytes = query_first_i64_or_zero(
            &context,
            &format!(
                "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot \
                 WHERE logical_table = '{logical_table}'"
            ),
        )
        .await;
        let compressed_bytes = get_table_compressed_bytes(&context, &t(logical_table)).await;
        assert!(
            attributed_bytes <= compressed_bytes
                && compressed_bytes - attributed_bytes <= ROUNDING_TOLERANCE_BYTES,
            "{logical_table}: attributed {attributed_bytes} bytes, actual {compressed_bytes} bytes"
        );
        assert_eq!(
            query_first_i64_or_zero(
                &context,
                &format!(
                    "SELECT toInt64(count(DISTINCT top_level_namespace)) \
                     FROM namespace_storage_snapshot WHERE logical_table = '{logical_table}'"
                ),
            )
            .await,
            2
        );
    }

    assert!(
        query_first_i64_or_zero(
            &context,
            "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot \
             WHERE top_level_namespace = '__global' AND logical_table = 'gl_user'",
        )
        .await
            > 0
    );

    let rows_before_refresh = get_snapshot_row_count(&context).await;
    let bytes_before_refresh = get_snapshot_compressed_bytes(&context).await;
    context
        .execute("OPTIMIZE TABLE namespace_storage_snapshot FINAL")
        .await;
    refresh_namespace_storage_snapshot_and_wait(&context).await;
    context
        .execute("OPTIMIZE TABLE namespace_storage_snapshot FINAL")
        .await;

    assert_eq!(get_snapshot_row_count(&context).await, rows_before_refresh);
    assert_eq!(
        get_snapshot_compressed_bytes(&context).await,
        bytes_before_refresh
    );
}

async fn create_namespace_storage_schema(context: &TestContext) {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let client = context.create_client();
    create_unversioned_tables(&client, &ontology).await.unwrap();
    replace_refreshable_views_for_version(&client, &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
}

async fn replace_namespace_storage_view(context: &TestContext) {
    let ontology = ontology::Ontology::load_embedded().unwrap();
    replace_refreshable_views_for_version(&context.create_client(), &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
}

async fn insert_rows_into_namespace_attributable_tables(context: &TestContext) {
    let batches = context
        .query(
            "SELECT tables.name FROM system.tables AS tables \
             WHERE tables.database = currentDatabase() AND tables.engine LIKE '%MergeTree%' \
               AND tables.name IN (SELECT table FROM system.columns \
                 WHERE database = currentDatabase() AND name = 'traversal_path')",
        )
        .await;
    for table_name in String::extract_column(&batches, 0).unwrap() {
        context
            .execute(&format!(
                "INSERT INTO `{table_name}` (traversal_path) SELECT '1/111/'"
            ))
            .await;
    }

    for global_table in [t("gl_user"), t("gl_runner")] {
        context
            .execute(&format!(
                "INSERT INTO `{global_table}` (id) SELECT number FROM numbers(50)"
            ))
            .await;
    }
}

async fn insert_note_and_edge_rows_for_two_namespaces(context: &TestContext) {
    let note_table = t("gl_note");
    context
        .execute(&format!(
            "INSERT INTO {note_table} (traversal_path, id, note) \
             SELECT '1/111/', number, concat('note-', toString(number)) FROM numbers(5000)"
        ))
        .await;
    context
        .execute(&format!(
            "INSERT INTO {note_table} (traversal_path, id, note) \
             SELECT '1/222/', number, concat('note-', toString(number)) FROM numbers(5000, 5000)"
        ))
        .await;

    let edge_table = t("gl_edge");
    for traversal_path in ["1/111/", "1/222/"] {
        context
            .execute(&format!(
                "INSERT INTO {edge_table} \
                 (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind) \
                 SELECT '{traversal_path}', 'MENTIONS', number, 'WorkItem', number + 1, 'User' \
                 FROM numbers(3000)"
            ))
            .await;
    }
}

async fn refresh_namespace_storage_snapshot_and_wait(context: &TestContext) {
    let view_name = t("namespace_storage_snapshot_refresh");
    context
        .execute(&format!("SYSTEM REFRESH VIEW {view_name}"))
        .await;

    for _ in 0..300 {
        let batches = context
            .query(&format!(
                "SELECT status, exception FROM system.view_refreshes WHERE view = '{view_name}'"
            ))
            .await;
        let statuses = String::extract_column(&batches, 0).unwrap();
        let exceptions = String::extract_column(&batches, 1).unwrap();
        assert!(exceptions.iter().all(String::is_empty), "{exceptions:?}");
        if statuses.first().is_some_and(|status| status == "Scheduled") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{view_name} refresh did not finish");
}

async fn get_table_or_view_count(context: &TestContext, name: &str) -> i64 {
    query_first_i64_or_zero(
        context,
        &format!(
            "SELECT toInt64(count()) FROM system.tables \
             WHERE database = currentDatabase() AND name = '{name}'"
        ),
    )
    .await
}

async fn get_table_compressed_bytes(context: &TestContext, physical_table: &str) -> i64 {
    query_first_i64_or_zero(
        context,
        &format!(
            "SELECT toInt64(sum(data_compressed_bytes)) FROM system.parts \
             WHERE database = currentDatabase() AND active AND table = '{physical_table}'"
        ),
    )
    .await
}

async fn get_snapshot_row_count(context: &TestContext) -> i64 {
    query_first_i64_or_zero(
        context,
        "SELECT toInt64(count()) FROM namespace_storage_snapshot",
    )
    .await
}

async fn get_snapshot_compressed_bytes(context: &TestContext) -> i64 {
    query_first_i64_or_zero(
        context,
        "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot",
    )
    .await
}

async fn query_first_i64_or_zero(context: &TestContext, sql: &str) -> i64 {
    let batches = context.query(sql).await;
    i64::extract_column(&batches, 0)
        .unwrap()
        .first()
        .copied()
        .unwrap_or(0)
}

fn namespace_storage_table_and_view_names() -> [String; 2] {
    [
        "namespace_storage_snapshot".to_string(),
        t("namespace_storage_snapshot_refresh"),
    ]
}

async fn create_test_context_with_graph_tables() -> TestContext {
    let context = TestContext::new(&[SIPHON_SCHEMA_SQL, &GRAPH_SCHEMA_SQL]).await;
    let client = context.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();
    context
}

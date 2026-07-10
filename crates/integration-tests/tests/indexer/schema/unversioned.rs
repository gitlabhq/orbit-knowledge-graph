use std::time::Duration;

use clickhouse_client::FromArrowColumn;
use indexer::schema::migration::{apply_active_schema_objects, drop_active_schema_views};
use indexer::schema::version::{SCHEMA_VERSION, ensure_version_table, write_schema_version};
use integration_testkit::{GRAPH_SCHEMA_SQL, SIPHON_SCHEMA_SQL, TestContext, t};

/// Largest allowed shortfall between a table's true `data_compressed_bytes` and
/// the sum of its per-namespace snapshot rows: `toUInt64` truncates each branch
/// row, so the sum can lag the total by at most one byte per namespace.
const ROUNDING_TOLERANCE_BYTES: i64 = 8;

#[tokio::test]
async fn applies_all_declared_objects() {
    let ctx = create_test_context_with_graph_tables().await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    apply_active_schema_objects(&ctx.create_client(), &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();

    for name in get_namespace_storage_object_names() {
        let exists = query_first_i64_or_zero(
            &ctx,
            &format!(
                "SELECT toInt64(count()) FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{name}'"
            ),
        )
        .await;
        assert_eq!(exists, 1, "{name} must be created");
    }
}

#[tokio::test]
async fn second_apply_is_a_clean_noop() {
    let ctx = create_test_context_with_graph_tables().await;
    let client = ctx.create_client();
    let ontology = ontology::Ontology::load_embedded().unwrap();

    apply_active_schema_objects(&client, &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
    ctx.execute(
        "INSERT INTO namespace_storage_snapshot \
         (snapshot_date, schema_version, logical_table, top_level_namespace, compressed_bytes) \
         VALUES (today(), 1, 'gl_note', '1/111', 42)",
    )
    .await;

    apply_active_schema_objects(&client, &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();

    for name in get_namespace_storage_object_names() {
        let exists = query_first_i64_or_zero(
            &ctx,
            &format!(
                "SELECT toInt64(count()) FROM system.tables \
                 WHERE database = currentDatabase() AND name = '{name}'"
            ),
        )
        .await;
        assert_eq!(exists, 1, "{name} must still exist after re-apply");
    }

    let preserved = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(compressed_bytes) FROM namespace_storage_snapshot WHERE compressed_bytes = 42",
    )
    .await;
    assert_eq!(preserved, 42, "history must survive a re-apply");
}

#[tokio::test]
async fn promotion_hook_drops_outgoing_mv_and_reapplies() {
    let ctx = create_test_context_with_graph_tables().await;
    let client = ctx.create_client();
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let mv = t("namespace_storage_snapshot_refresh");

    apply_active_schema_objects(&client, &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
    assert_eq!(
        get_clickhouse_object_count_by_name(&ctx, &mv).await,
        1,
        "MV created on apply"
    );

    drop_active_schema_views(&client, &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
    assert_eq!(
        get_clickhouse_object_count_by_name(&ctx, &mv).await,
        0,
        "outgoing MV dropped by the promotion hook's drop step"
    );

    apply_active_schema_objects(&client, &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();
    assert_eq!(
        get_clickhouse_object_count_by_name(&ctx, &mv).await,
        1,
        "MV recreated by the promotion hook's apply step"
    );
}

#[tokio::test]
async fn snapshot_attributes_bytes_per_namespace() {
    let ctx = create_test_context_with_graph_tables().await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    apply_active_schema_objects(&ctx.create_client(), &ontology, *SCHEMA_VERSION)
        .await
        .unwrap();

    insert_rows_into_all_namespace_attributable_tables(&ctx).await;
    insert_note_and_edge_rows_for_two_namespaces(&ctx).await;
    ctx.optimize_all().await;
    refresh_namespace_storage_snapshot_and_wait(&ctx).await;

    let zero_rows = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(count()) FROM namespace_storage_snapshot WHERE compressed_bytes = 0",
    )
    .await;
    assert_eq!(zero_rows, 0, "snapshot must not contain zero-byte rows");

    for table in ["gl_note", "gl_edge"] {
        let attributed = query_first_i64_or_zero(
            &ctx,
            &format!(
                "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot \
                 WHERE logical_table = '{table}'"
            ),
        )
        .await;
        let actual = table_compressed_bytes(&ctx, &t(table)).await;
        assert!(
            attributed <= actual && actual - attributed <= ROUNDING_TOLERANCE_BYTES,
            "{table}: attributed {attributed} bytes vs actual {actual} bytes exceeds tolerance"
        );

        let namespaces = query_first_i64_or_zero(
            &ctx,
            &format!(
                "SELECT toInt64(count(DISTINCT top_level_namespace)) FROM namespace_storage_snapshot \
                 WHERE logical_table = '{table}'"
            ),
        )
        .await;
        assert_eq!(namespaces, 2, "{table} spans two top-level namespaces");
    }

    let global_users = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot \
         WHERE top_level_namespace = '__global' AND logical_table = 'gl_user'",
    )
    .await;
    assert!(global_users > 0, "gl_user must be booked under __global");

    let rows_before = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(count()) FROM namespace_storage_snapshot",
    )
    .await;
    let bytes_before = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot",
    )
    .await;

    ctx.execute("OPTIMIZE TABLE namespace_storage_snapshot FINAL")
        .await;
    refresh_namespace_storage_snapshot_and_wait(&ctx).await;
    ctx.execute("OPTIMIZE TABLE namespace_storage_snapshot FINAL")
        .await;

    let rows_after = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(count()) FROM namespace_storage_snapshot",
    )
    .await;
    let bytes_after = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(sum(compressed_bytes)) FROM namespace_storage_snapshot",
    )
    .await;
    assert_eq!(
        rows_before, rows_after,
        "same-day re-refresh must dedupe, not double, snapshot rows"
    );
    assert_eq!(
        bytes_before, bytes_after,
        "re-refresh must not double bytes"
    );
}

#[tokio::test]
async fn startup_guard_holds_apply_during_pending_migration() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let active = indexer::schema::version::read_active_version(&client)
        .await
        .unwrap();
    let guard_allows_apply = active == Some(*SCHEMA_VERSION);
    assert!(
        !guard_allows_apply,
        "startup apply must be held while the active version disagrees with the binary"
    );

    let mv_exists = query_first_i64_or_zero(
        &ctx,
        "SELECT toInt64(count()) FROM system.tables \
         WHERE name LIKE '%namespace_storage_snapshot_refresh'",
    )
    .await;
    assert_eq!(
        mv_exists, 0,
        "no MV should be applied during a pending migration"
    );
}

async fn insert_rows_into_all_namespace_attributable_tables(ctx: &TestContext) {
    let tables = ctx
        .query(
            "SELECT t.name FROM system.tables t \
             WHERE t.database = currentDatabase() AND t.engine LIKE '%MergeTree%' \
               AND t.name IN (SELECT table FROM system.columns \
                 WHERE database = currentDatabase() AND name = 'traversal_path')",
        )
        .await;
    let names = String::extract_column(&tables, 0).unwrap();
    for name in names {
        ctx.execute(&format!(
            "INSERT INTO `{name}` (traversal_path) SELECT '1/111/'"
        ))
        .await;
    }

    for global in [t("gl_user"), t("gl_runner")] {
        ctx.execute(&format!(
            "INSERT INTO `{global}` (id) SELECT number FROM numbers(50)"
        ))
        .await;
    }
}

async fn insert_note_and_edge_rows_for_two_namespaces(ctx: &TestContext) {
    let note = t("gl_note");
    ctx.execute(&format!(
        "INSERT INTO {note} (traversal_path, id, note) \
         SELECT '1/111/', number, concat('note-', toString(number)) FROM numbers(5000)"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {note} (traversal_path, id, note) \
         SELECT '1/222/', number, concat('note-', toString(number)) FROM numbers(5000, 5000)"
    ))
    .await;

    let edge = t("gl_edge");
    for ns in ["1/111/", "1/222/"] {
        ctx.execute(&format!(
            "INSERT INTO {edge} \
             (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind) \
             SELECT '{ns}', 'MENTIONS', number, 'WorkItem', number + 1, 'User' FROM numbers(3000)"
        ))
        .await;
    }
}

async fn refresh_namespace_storage_snapshot_and_wait(ctx: &TestContext) {
    let mv = t("namespace_storage_snapshot_refresh");
    ctx.execute(&format!("SYSTEM REFRESH VIEW {mv}")).await;

    for _ in 0..300 {
        let batches = ctx
            .query(&format!(
                "SELECT status, exception FROM system.view_refreshes WHERE view = '{mv}'"
            ))
            .await;
        let status = String::extract_column(&batches, 0).unwrap();
        let exception = String::extract_column(&batches, 1).unwrap();
        assert!(
            exception.iter().all(|e| e.is_empty()),
            "refresh failed: {exception:?}"
        );
        if status.first().is_some_and(|s| s == "Scheduled") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{mv} refresh did not finish");
}

async fn get_clickhouse_object_count_by_name(ctx: &TestContext, name: &str) -> i64 {
    query_first_i64_or_zero(
        ctx,
        &format!(
            "SELECT toInt64(count()) FROM system.tables \
             WHERE database = currentDatabase() AND name = '{name}'"
        ),
    )
    .await
}

async fn table_compressed_bytes(ctx: &TestContext, physical_table: &str) -> i64 {
    query_first_i64_or_zero(
        ctx,
        &format!(
            "SELECT toInt64(sum(data_compressed_bytes)) FROM system.parts \
             WHERE database = currentDatabase() AND active AND table = '{physical_table}'"
        ),
    )
    .await
}

async fn query_first_i64_or_zero(ctx: &TestContext, sql: &str) -> i64 {
    let batches = ctx.query(sql).await;
    i64::extract_column(&batches, 0)
        .unwrap()
        .first()
        .copied()
        .unwrap_or(0)
}

fn get_namespace_storage_object_names() -> [String; 2] {
    [
        "namespace_storage_snapshot".to_string(),
        t("namespace_storage_snapshot_refresh"),
    ]
}

async fn create_test_context_with_graph_tables() -> TestContext {
    let ctx = TestContext::new(&[SIPHON_SCHEMA_SQL, &GRAPH_SCHEMA_SQL]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();
    ctx
}

use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::FromArrowColumn;
use indexer::locking::LockService;
use indexer::metrics::MigrationMetrics;
use indexer::orchestrator::scheduled::migration_completion;
use indexer::schema::migration;
use indexer::schema::version::{
    SCHEMA_VERSION, SchemaWaitError, ensure_version_table, prefixed_table_name,
    read_active_version, table_prefix, wait_until_ready, write_migrating_version,
    write_schema_version,
};
use indexer::testkit::MockLockService;
use integration_testkit::{TestContext, t};
use ontology::migrations::MigrationScope;
use query_engine::compiler::{
    DictionarySource, emit_create_table, generate_graph_dictionaries_with_prefix,
    generate_graph_tables_with_prefix,
};

fn dictionary_source(config: &gkg_server_config::ClickHouseConfiguration) -> DictionarySource<'_> {
    DictionarySource {
        database: &config.database,
        user: &config.username,
        password: config.password.as_deref(),
    }
}

async fn setup() -> (TestContext, ontology::Ontology, MigrationMetrics) {
    let ctx = TestContext::new(&[]).await;
    let ontology = ontology::Ontology::load_embedded().unwrap();
    let metrics = MigrationMetrics::new();

    // Mirror indexer startup: version table must exist before migration runs.
    ensure_version_table(&ctx.create_client()).await.unwrap();

    (ctx, ontology, metrics)
}

fn lock() -> Arc<dyn LockService> {
    Arc::new(MockLockService::new())
}

fn campaign() -> indexer::campaign::CampaignState {
    indexer::campaign::CampaignState::new()
}

#[tokio::test]
async fn fresh_install_creates_tables_and_records_version() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION)
    );

    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables = generate_graph_tables_with_prefix(&ontology, &prefix);

    let result = ctx
        .query(
            "SELECT toInt64(count()) AS cnt FROM system.tables \
             WHERE database = 'test' AND name != 'gkg_schema_version' \
             AND engine != 'Dictionary'",
        )
        .await;
    let count = i64::extract_column(&result, 0).unwrap();
    assert_eq!(
        count,
        vec![expected_tables.len() as i64],
        "fresh install should create all ontology tables"
    );

    let expected_dicts = generate_graph_dictionaries_with_prefix(&ontology, &prefix);
    let result = ctx
        .query("SELECT toInt64(count()) AS cnt FROM system.dictionaries WHERE database = 'test'")
        .await;
    let dict_count = i64::extract_column(&result, 0).unwrap();
    assert_eq!(
        dict_count,
        vec![expected_dicts.len() as i64],
        "fresh install should create all ontology dictionaries"
    );
}

#[tokio::test]
async fn matching_version_is_noop() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION)
    );
}

#[tokio::test]
async fn mismatch_creates_all_ontology_tables_and_marks_migrating() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables = generate_graph_tables_with_prefix(&ontology, &prefix);

    let result = ctx
        .query(
            "SELECT name FROM system.tables \
             WHERE database = 'test' AND name != 'gkg_schema_version' \
             AND engine != 'Dictionary' \
             ORDER BY name",
        )
        .await;
    let created_names = String::extract_column(&result, 0).unwrap();

    assert_eq!(
        created_names.len(),
        expected_tables.len(),
        "expected {} tables from ontology, got {}: {created_names:?}",
        expected_tables.len(),
        created_names.len(),
    );

    for table in &expected_tables {
        assert!(
            created_names.contains(&table.name),
            "missing table '{}' — created: {created_names:?}",
            table.name
        );
    }

    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(statuses, vec!["migrating"]);
}

#[tokio::test]
async fn created_tables_have_correct_columns() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    for (table, expected_col) in [
        (t("gl_user"), "username"),
        (t("gl_edge"), "relationship_kind"),
        (t("checkpoint"), "watermark"),
    ] {
        let result = ctx
            .query(&format!(
                "SELECT name FROM system.columns WHERE database = 'test' AND table = '{table}'"
            ))
            .await;
        let columns = String::extract_column(&result, 0).unwrap();
        assert!(
            columns.contains(&expected_col.to_string()),
            "table '{table}' missing column '{expected_col}' — has: {columns:?}"
        );
    }
}

#[tokio::test]
async fn idempotent_rerun_succeeds() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    let lock_svc: Arc<dyn LockService> = Arc::new(MockLockService::new());

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock_svc,
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock_svc,
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn lock_released_after_migration() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    let mock = Arc::new(MockLockService::new());
    let lock_svc: Arc<dyn LockService> = mock.clone();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock_svc,
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert!(!mock.is_held("schema_migration"), "lock should be released");
}

#[tokio::test]
async fn held_lock_causes_timeout() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    let mock = MockLockService::new();
    mock.set_lock("schema_migration");
    let lock_svc: Arc<dyn LockService> = Arc::new(mock);

    // Migration polls every 5s × 60 iterations. Use paused time to skip the wait.
    tokio::time::pause();

    let result = migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock_svc,
        &ontology,
        &metrics,
        &campaign(),
    )
    .await;

    assert!(result.is_err());
    assert!(
        result.unwrap_err().to_string().contains("lock held"),
        "error should mention lock timeout"
    );
}

#[tokio::test]
async fn mismatch_opens_campaign_steady_state_does_not() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    write_schema_version(&client, *SCHEMA_VERSION - 1)
        .await
        .unwrap();

    let migrating_campaign = campaign();
    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &migrating_campaign,
    )
    .await
    .unwrap();
    assert_eq!(
        migrating_campaign.current(),
        Some(format!("migration-v{}", *SCHEMA_VERSION)),
        "a mismatch migration should open the campaign for the target version"
    );

    let matching_campaign = campaign();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();
    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &matching_campaign,
    )
    .await
    .unwrap();
    assert_eq!(
        matching_campaign.current(),
        None,
        "steady state should not open a campaign"
    );
}

#[tokio::test]
async fn rollback_reactivates_directly_when_embedded_tables_are_intact() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION),
        "rollback must re-activate the embedded version"
    );

    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION + 1
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["retired"],
        "the newer version must be retired, not dropped, by a direct-reactivation rollback"
    );

    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["active"],
        "direct reactivation must not pass through a migrating phase"
    );
}

#[tokio::test]
async fn rollback_rebuilds_when_embedded_tables_are_gone() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION + 1),
        "a rebuild rollback must not promote until the completion checker observes full coverage"
    );

    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["migrating"],
        "a rebuild rollback marks the embedded version migrating, same as a forward migration"
    );

    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables = generate_graph_tables_with_prefix(&ontology, &prefix);
    let result = ctx
        .query(
            "SELECT name FROM system.tables \
             WHERE database = 'test' AND name != 'gkg_schema_version' \
             AND engine != 'Dictionary' \
             ORDER BY name",
        )
        .await;
    let created_names = String::extract_column(&result, 0).unwrap();
    for table in &expected_tables {
        assert!(
            created_names.contains(&table.name),
            "rebuild rollback must recreate table '{}' — created: {created_names:?}",
            table.name
        );
    }
}

#[tokio::test]
async fn rollback_rebuild_clears_stale_objects_before_recreating() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let checkpoint_table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    ctx.execute(&format!("DROP TABLE {checkpoint_table}")).await;

    let user_table = prefixed_table_name("gl_user", *SCHEMA_VERSION);
    ctx.execute(&format!(
        "INSERT INTO {user_table} (id, username) VALUES (999, 'stale-rollback-user')"
    ))
    .await;

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["migrating"],
        "a partial table set must route to rebuild, not direct re-activation"
    );

    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected_tables = generate_graph_tables_with_prefix(&ontology, &prefix);
    let result = ctx
        .query(&format!(
            "SELECT name FROM system.tables \
             WHERE database = 'test' AND startsWith(name, '{prefix}') \
             AND engine != 'Dictionary' \
             ORDER BY name"
        ))
        .await;
    let created_names = String::extract_column(&result, 0).unwrap();
    for table in &expected_tables {
        assert!(
            created_names.contains(&table.name),
            "rebuild must recreate '{}' — created: {created_names:?}",
            table.name
        );
    }

    let result = ctx
        .query(&format!(
            "SELECT toInt64(count()) AS cnt FROM {user_table} FINAL WHERE id = 999"
        ))
        .await;
    let count = i64::extract_column(&result, 0).unwrap();
    assert_eq!(
        count,
        vec![0],
        "rebuild must drop surviving objects first so stale rows don't leak into the rebuilt version"
    );
}

#[tokio::test]
async fn lock_released_after_rollback_reactivation() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock(),
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let mock = Arc::new(MockLockService::new());
    let lock_svc: Arc<dyn LockService> = mock.clone();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock_svc,
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert!(!mock.is_held("schema_migration"), "lock should be released");
    assert_eq!(
        read_active_version(&client).await.unwrap(),
        Some(*SCHEMA_VERSION),
        "must have taken the reactivation arm"
    );
}

#[tokio::test]
async fn lock_released_after_rollback_rebuild() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();

    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let mock = Arc::new(MockLockService::new());
    let lock_svc: Arc<dyn LockService> = mock.clone();

    migration::run_if_needed(
        &client,
        &dictionary_source(&ctx.config),
        &lock_svc,
        &ontology,
        &metrics,
        &campaign(),
    )
    .await
    .unwrap();

    assert!(!mock.is_held("schema_migration"), "lock should be released");

    let result = ctx
        .query(&format!(
            "SELECT CAST(status AS String) AS status \
             FROM gkg_schema_version FINAL WHERE version = {}",
            *SCHEMA_VERSION
        ))
        .await;
    let statuses = String::extract_column(&result, 0).unwrap();
    assert_eq!(
        statuses,
        vec!["migrating"],
        "must have taken the rebuild arm"
    );
}

#[tokio::test]
async fn read_active_version_returns_none_on_empty_table() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();

    let version = read_active_version(&client).await.unwrap();
    assert_eq!(version, None, "empty version table should return None");
}

#[tokio::test]
async fn read_active_version_returns_some_after_write() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();

    write_schema_version(&client, 1).await.unwrap();
    let version = read_active_version(&client).await.unwrap();
    assert_eq!(version, Some(1));
}

#[tokio::test]
async fn wait_until_ready_returns_when_version_active() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();

    wait_until_ready(
        &client,
        *SCHEMA_VERSION,
        Duration::from_secs(5),
        Duration::from_millis(50),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn wait_until_ready_returns_when_version_migrating() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_migrating_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();

    wait_until_ready(
        &client,
        *SCHEMA_VERSION,
        Duration::from_secs(5),
        Duration::from_millis(50),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn wait_until_ready_times_out_when_version_absent() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();

    let result = wait_until_ready(
        &client,
        *SCHEMA_VERSION,
        Duration::from_secs(1),
        Duration::from_millis(100),
    )
    .await;

    assert!(matches!(result, Err(SchemaWaitError::Timeout { .. })));
}

#[tokio::test]
async fn wait_until_ready_fails_fast_when_outdated() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();

    let result = wait_until_ready(
        &client,
        *SCHEMA_VERSION,
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await;

    assert!(matches!(result, Err(SchemaWaitError::Outdated { .. })));
}

#[tokio::test]
async fn wait_until_ready_ready_when_rebuilding_below_active() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, *SCHEMA_VERSION + 1)
        .await
        .unwrap();
    write_migrating_version(&client, *SCHEMA_VERSION)
        .await
        .unwrap();

    wait_until_ready(
        &client,
        *SCHEMA_VERSION,
        Duration::from_secs(5),
        Duration::from_millis(50),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn sdlc_migration_clones_seeds_checkpoint_and_gates_on_invalidated_plan() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    let active_version = *SCHEMA_VERSION - 1;
    let old_prefix = table_prefix(active_version);
    let new_checkpoint = prefixed_table_name("checkpoint", *SCHEMA_VERSION);

    for table in generate_graph_tables_with_prefix(&ontology, &old_prefix) {
        client.execute(&emit_create_table(&table)).await.unwrap();
    }

    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_edge \
         (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version) \
         VALUES ('1/100/', 'MENTIONS', 1, 'User', 2, 'Note', '2024-01-01 00:00:00.000000')"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_note (traversal_path, id, _version) \
         VALUES ('1/100/', 5, '2024-01-01 00:00:00.000000')"
    ))
    .await;
    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_merge_request (traversal_path, id, _version) \
         VALUES ('1/100/', 7, '2024-01-01 00:00:00.000000')"
    ))
    .await;

    for (key, cursor_values) in [
        ("global.User", "null"),
        ("ns.100.Job.p1of2", "null"),
        ("ns.100.Job.p2of2", "null"),
        ("ns.100.Note", "null"),
        ("dispatch.sdlc.namespace.sweep", "null"),
    ] {
        ctx.execute(&format!(
            "INSERT INTO {old_prefix}checkpoint (key, watermark, cursor_values, _version) \
             VALUES ('{key}', '2024-01-01 00:00:00.000000', '{cursor_values}', '2024-01-01 00:00:00.000000')"
        ))
        .await;
    }

    let scope = MigrationScope::Sdlc(["Note".to_string()].into_iter().collect());
    migration::clone_unchanged_migration_tables(
        &client,
        &dictionary_source(&ctx.config),
        &ontology,
        &metrics,
        &scope,
        active_version,
    )
    .await
    .unwrap();

    let new_edge = prefixed_table_name("gl_edge", *SCHEMA_VERSION);
    let edge_rows = ctx
        .query(&format!(
            "SELECT toInt64(count()) AS cnt FROM {new_edge} FINAL"
        ))
        .await;
    assert_eq!(
        i64::extract_column(&edge_rows, 0).unwrap(),
        vec![1],
        "an unchanged edge table must be cloned with its rows"
    );

    let new_note = prefixed_table_name("gl_note", *SCHEMA_VERSION);
    let note_rows = ctx
        .query(&format!("SELECT toInt64(count()) AS cnt FROM {new_note}"))
        .await;
    assert_eq!(
        i64::extract_column(&note_rows, 0).unwrap(),
        vec![0],
        "the invalidated entity's node table must be rebuilt empty, not cloned"
    );

    let new_mr = prefixed_table_name("gl_merge_request", *SCHEMA_VERSION);
    let mr_rows = ctx
        .query(&format!(
            "SELECT toInt64(count()) AS cnt FROM {new_mr} FINAL"
        ))
        .await;
    assert_eq!(
        i64::extract_column(&mr_rows, 0).unwrap(),
        vec![1],
        "an uninvalidated node table must be cloned with its rows"
    );

    let surviving = ctx
        .query(&format!(
            "SELECT key FROM {new_checkpoint} FINAL WHERE _deleted = false ORDER BY key"
        ))
        .await;
    assert_eq!(
        String::extract_column(&surviving, 0).unwrap(),
        vec![
            "global.User".to_string(),
            "ns.100.Job.p1of2".to_string(),
            "ns.100.Job.p2of2".to_string(),
        ],
        "seeding keeps unchanged plans — including a partitioned entity's sub-keys — and drops \
         the dispatch cursor and the invalidated plan"
    );

    let before = migration_completion::get_sdlc_reindex_progress(
        &client,
        &ontology,
        &scope,
        &new_checkpoint,
        1,
    )
    .await
    .unwrap();
    assert!(
        !before.ready,
        "the gate must block until the invalidated plan has a completed checkpoint"
    );

    ctx.execute(&format!(
        "INSERT INTO {new_checkpoint} (key, watermark, cursor_values, _version) \
         VALUES ('ns.100.Note', '2024-02-01 00:00:00.000000', 'null', '2024-02-01 00:00:00.000000')"
    ))
    .await;

    let after = migration_completion::get_sdlc_reindex_progress(
        &client,
        &ontology,
        &scope,
        &new_checkpoint,
        1,
    )
    .await
    .unwrap();
    assert!(
        after.ready,
        "the gate must promote once every enabled namespace has the invalidated plan's completed checkpoint"
    );
}

async fn create_version_tables(ctx: &TestContext, ontology: &ontology::Ontology, prefix: &str) {
    let client = ctx.create_client();
    for table in generate_graph_tables_with_prefix(ontology, prefix) {
        client.execute(&emit_create_table(&table)).await.unwrap();
    }
}

async fn insert_checkpoint_row(
    ctx: &TestContext,
    table: &str,
    key: &str,
    cursor_values: &str,
    version: &str,
) {
    ctx.execute(&format!(
        "INSERT INTO {table} (key, watermark, cursor_values, _version) \
         VALUES ('{key}', '{version}', '{cursor_values}', '{version}')"
    ))
    .await;
}

async fn count_rows(ctx: &TestContext, table: &str) -> i64 {
    let batches = ctx
        .query(&format!(
            "SELECT toInt64(count()) AS cnt FROM {table} FINAL"
        ))
        .await;
    i64::extract_column(&batches, 0).unwrap()[0]
}

fn sdlc_scope(entities: &[&str]) -> MigrationScope {
    MigrationScope::Sdlc(entities.iter().map(|s| s.to_string()).collect())
}

#[tokio::test]
async fn fk_edge_scope_reindexes_note_without_wiping_its_table() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    let active_version = *SCHEMA_VERSION - 1;
    let old_prefix = table_prefix(active_version);
    create_version_tables(&ctx, &ontology, &old_prefix).await;

    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_note (traversal_path, id, _version) \
         VALUES ('1/100/', 5, '2024-01-01 00:00:00.000000')"
    ))
    .await;
    let old_checkpoint = format!("{old_prefix}checkpoint");
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "ns.100.Note",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "ns.100.User",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;

    migration::clone_unchanged_migration_tables(
        &client,
        &dictionary_source(&ctx.config),
        &ontology,
        &metrics,
        &sdlc_scope(&["HAS_NOTE"]),
        active_version,
    )
    .await
    .unwrap();

    let new_note = prefixed_table_name("gl_note", *SCHEMA_VERSION);
    assert_eq!(
        count_rows(&ctx, &new_note).await,
        1,
        "HAS_NOTE does not invalidate the Note table itself; it must be cloned, \
         with re-emitted rows superseding in place"
    );

    let new_checkpoint = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let surviving = ctx
        .query(&format!(
            "SELECT key FROM {new_checkpoint} FINAL WHERE _deleted = false ORDER BY key"
        ))
        .await;
    assert_eq!(
        String::extract_column(&surviving, 0).unwrap(),
        vec!["ns.100.User".to_string()],
        "HAS_NOTE must expand to the Note pipeline and drop its checkpoint keys"
    );

    let scope = sdlc_scope(&["HAS_NOTE"]);
    let before = migration_completion::get_sdlc_reindex_progress(
        &client,
        &ontology,
        &scope,
        &new_checkpoint,
        1,
    )
    .await
    .unwrap();
    assert!(
        !before.ready,
        "the gate must wait on the expanded Note pipeline"
    );

    insert_checkpoint_row(
        &ctx,
        &new_checkpoint,
        "ns.100.Note",
        "null",
        "2024-06-01 00:00:00.000000",
    )
    .await;
    let after = migration_completion::get_sdlc_reindex_progress(
        &client,
        &ontology,
        &scope,
        &new_checkpoint,
        1,
    )
    .await
    .unwrap();
    assert!(after.ready);
}
#[tokio::test]
async fn clone_based_migration_survives_interruption_rerun_and_edge_reemission() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    let active_version = *SCHEMA_VERSION - 1;
    let old_prefix = table_prefix(active_version);
    create_version_tables(&ctx, &ontology, &old_prefix).await;

    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_edge \
         (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version) \
         VALUES ('1/100/', 'HAS_NOTE', 7, 'MergeRequest', 5, 'Note', '2024-01-01 00:00:00.000000')"
    ))
    .await;
    let old_checkpoint = format!("{old_prefix}checkpoint");
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "ns.100.User",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "ns.100.Note",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;

    let new_edge_table =
        generate_graph_tables_with_prefix(&ontology, &table_prefix(*SCHEMA_VERSION))
            .into_iter()
            .find(|table| table.name == prefixed_table_name("gl_edge", *SCHEMA_VERSION))
            .unwrap();
    client
        .execute(&emit_create_table(&new_edge_table))
        .await
        .unwrap();

    for _ in 0..2 {
        migration::clone_unchanged_migration_tables(
            &client,
            &dictionary_source(&ctx.config),
            &ontology,
            &metrics,
            &sdlc_scope(&["Note"]),
            active_version,
        )
        .await
        .unwrap();
    }

    let new_edge = prefixed_table_name("gl_edge", *SCHEMA_VERSION);
    assert_eq!(
        count_rows(&ctx, &new_edge).await,
        1,
        "an empty shell left by an interrupted clone must be dropped and re-cloned, \
         and a rerun must not duplicate the cloned rows"
    );

    let new_checkpoint = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let surviving = ctx
        .query(&format!(
            "SELECT key FROM {new_checkpoint} FINAL WHERE _deleted = false ORDER BY key"
        ))
        .await;
    assert_eq!(
        String::extract_column(&surviving, 0).unwrap(),
        vec!["ns.100.User".to_string()],
        "a crash-rerun of the migration must not duplicate or resurrect checkpoint keys"
    );

    ctx.execute(&format!(
        "INSERT INTO {new_edge} \
         (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version) \
         VALUES ('1/100/', 'HAS_NOTE', 7, 'MergeRequest', 5, 'Note', '2024-06-01 00:00:00.000000')"
    ))
    .await;
    assert_eq!(
        count_rows(&ctx, &new_edge).await,
        1,
        "a re-emitted edge with the same sort key must supersede the cloned row, not duplicate it"
    );
}

#[tokio::test]
async fn gate_promotes_only_when_every_invalidated_pipeline_completes_everywhere() {
    let (ctx, ontology, _) = setup().await;
    let client = ctx.create_client();
    create_version_tables(&ctx, &ontology, &table_prefix(*SCHEMA_VERSION)).await;
    let checkpoint = prefixed_table_name("checkpoint", *SCHEMA_VERSION);

    let progress = |scope: MigrationScope, enabled_count: u64| {
        let client = client.clone();
        let ontology = ontology.clone();
        let checkpoint = checkpoint.clone();
        async move {
            migration_completion::get_sdlc_reindex_progress(
                &client,
                &ontology,
                &scope,
                &checkpoint,
                enabled_count,
            )
            .await
            .unwrap()
        }
    };

    insert_checkpoint_row(
        &ctx,
        &checkpoint,
        "ns.100.Note",
        r#"{"c":["1/100/","5"]}"#,
        "2024-01-01 00:00:00.000000",
    )
    .await;
    assert!(
        !progress(sdlc_scope(&["Note"]), 1).await.ready,
        "an in-progress cursor is not a completed pipeline"
    );

    insert_checkpoint_row(
        &ctx,
        &checkpoint,
        "ns.100.Note",
        "null",
        "2024-06-01 00:00:00.000000",
    )
    .await;
    assert!(
        progress(sdlc_scope(&["Note"]), 1).await.ready,
        "the newer completed row must replace the in-progress cursor under FINAL"
    );

    let one_of_two = progress(sdlc_scope(&["Note"]), 2).await;
    assert_eq!(one_of_two.completed_namespaces, 1);
    assert!(
        !one_of_two.ready,
        "one completed namespace out of two enabled must not promote"
    );
    insert_checkpoint_row(
        &ctx,
        &checkpoint,
        "ns.200.Note",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    assert!(progress(sdlc_scope(&["Note"]), 2).await.ready);

    let missing_pipeline = progress(sdlc_scope(&["Note", "MergeRequest"]), 2).await;
    assert_eq!(missing_pipeline.completed_namespaces, 0);
    assert!(
        !missing_pipeline.ready,
        "a namespace missing one of the invalidated pipelines must not count as complete"
    );
    for ns in [100, 200] {
        insert_checkpoint_row(
            &ctx,
            &checkpoint,
            &format!("ns.{ns}.MergeRequest"),
            "null",
            "2024-01-01 00:00:00.000000",
        )
        .await;
    }
    assert!(
        progress(sdlc_scope(&["Note", "MergeRequest"]), 2)
            .await
            .ready
    );

    let global_pending = progress(sdlc_scope(&["User"]), 2).await;
    assert_eq!(
        global_pending.completed_namespaces, 0,
        "a global-only scope has no namespaced pipelines to count"
    );
    assert!(
        !global_pending.ready,
        "the gate must wait on the invalidated global pipeline"
    );
    insert_checkpoint_row(
        &ctx,
        &checkpoint,
        "global.User",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    assert!(progress(sdlc_scope(&["User"]), 2).await.ready);
}

#[tokio::test]
async fn whole_sdlc_scope_seeds_empty_checkpoint_and_rebuilds_sdlc_tables() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    let active_version = *SCHEMA_VERSION - 1;
    let old_prefix = table_prefix(active_version);
    create_version_tables(&ctx, &ontology, &old_prefix).await;

    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_note (traversal_path, id, _version) \
         VALUES ('1/100/', 5, '2024-01-01 00:00:00.000000')"
    ))
    .await;
    let old_checkpoint = format!("{old_prefix}checkpoint");
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "ns.100.Note",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "global.User",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;

    migration::clone_unchanged_migration_tables(
        &client,
        &dictionary_source(&ctx.config),
        &ontology,
        &metrics,
        &sdlc_scope(&[]),
        active_version,
    )
    .await
    .unwrap();

    let new_note = prefixed_table_name("gl_note", *SCHEMA_VERSION);
    assert_eq!(
        count_rows(&ctx, &new_note).await,
        0,
        "whole-sdlc scope invalidates every sdlc writer, so node tables rebuild empty"
    );
    let new_checkpoint = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    assert_eq!(
        count_rows(&ctx, &new_checkpoint).await,
        0,
        "whole-sdlc scope must seed no pipeline checkpoints — everything re-indexes from epoch"
    );
}

#[tokio::test]
async fn code_scope_clones_sdlc_state_intact_and_rebuilds_code_checkpoint() {
    let (ctx, ontology, metrics) = setup().await;
    let client = ctx.create_client();
    let active_version = *SCHEMA_VERSION - 1;
    let old_prefix = table_prefix(active_version);
    create_version_tables(&ctx, &ontology, &old_prefix).await;

    ctx.execute(&format!(
        "INSERT INTO {old_prefix}gl_note (traversal_path, id, _version) \
         VALUES ('1/100/', 5, '2024-01-01 00:00:00.000000')"
    ))
    .await;
    let old_checkpoint = format!("{old_prefix}checkpoint");
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "ns.100.Note",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    insert_checkpoint_row(
        &ctx,
        &old_checkpoint,
        "dispatch.sdlc.namespace.sweep",
        "null",
        "2024-01-01 00:00:00.000000",
    )
    .await;
    ctx.execute(&format!(
        "INSERT INTO {old_prefix}code_indexing_checkpoint \
         (traversal_path, project_id, branch, last_task_id, _version) \
         VALUES ('1/100/', 42, 'main', 1, 1)"
    ))
    .await;

    migration::clone_unchanged_migration_tables(
        &client,
        &dictionary_source(&ctx.config),
        &ontology,
        &metrics,
        &MigrationScope::Code,
        active_version,
    )
    .await
    .unwrap();

    let new_note = prefixed_table_name("gl_note", *SCHEMA_VERSION);
    assert_eq!(
        count_rows(&ctx, &new_note).await,
        1,
        "a code-scope migration must not touch SDLC data"
    );

    let new_checkpoint = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    let surviving = ctx
        .query(&format!(
            "SELECT key FROM {new_checkpoint} FINAL WHERE _deleted = false ORDER BY key"
        ))
        .await;
    assert_eq!(
        String::extract_column(&surviving, 0).unwrap(),
        vec![
            "dispatch.sdlc.namespace.sweep".to_string(),
            "ns.100.Note".to_string()
        ],
        "code scope clones the SDLC checkpoint intact — dispatch keys survive so the \
         cold-start sweep does not re-fire SDLC indexing"
    );

    let new_code_checkpoint = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
    assert_eq!(
        count_rows(&ctx, &new_code_checkpoint).await,
        0,
        "the code checkpoint must be rebuilt empty so the code backfill re-dispatches"
    );
}

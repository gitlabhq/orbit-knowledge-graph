use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::FromArrowColumn;
use indexer::locking::LockService;
use indexer::metrics::MigrationMetrics;
use indexer::schema::migration;
use indexer::schema::version::{
    SCHEMA_VERSION, SchemaWaitError, ensure_version_table, read_active_version, table_prefix,
    wait_until_ready, write_migrating_version, write_schema_version,
};
use indexer::testkit::MockLockService;
use integration_testkit::{TestContext, t};
use query_engine::compiler::{
    DictionarySource, generate_graph_dictionaries_with_prefix, generate_graph_tables_with_prefix,
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

// Case 2 rollback: the dispatcher marks this older version migrating while a
// higher version stays active, so its indexer must become ready to backfill it.
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

use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::{ClickHouseConfigurationExt, FromArrowColumn};
use indexer::checkpoint::ClickHouseCheckpointStore;
use indexer::locking::LockService;
use indexer::metrics::MigrationMetrics;
use indexer::modules::code::config::CodeTableNames;
use indexer::orchestrator::scheduled::{CodeStaleSweep, migration_completion};
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

const SEED_VERSION: &str = "2024-01-01 00:00:00.000000";
const REINDEX_VERSION: &str = "2024-06-01 00:00:00.000000";

fn sdlc(entities: &[&str]) -> MigrationScope {
    MigrationScope::Sdlc(entities.iter().map(|s| s.to_string()).collect())
}

/// Drives a clone-based migration and its promotion gate, hiding table prefixes and seed timestamps.
struct MigrationScenario {
    ctx: TestContext,
    ontology: ontology::Ontology,
    metrics: MigrationMetrics,
    active_version: u32,
}

impl MigrationScenario {
    async fn migrating_from_active() -> Self {
        let (ctx, ontology, metrics) = setup().await;
        let active_version = *SCHEMA_VERSION - 1;
        let scenario = Self {
            ctx,
            ontology,
            metrics,
            active_version,
        };
        scenario.create_tables(&table_prefix(active_version)).await;
        scenario
    }

    async fn at_new_version() -> Self {
        let (ctx, ontology, metrics) = setup().await;
        let scenario = Self {
            ctx,
            ontology,
            metrics,
            active_version: *SCHEMA_VERSION,
        };
        scenario.create_tables(&table_prefix(*SCHEMA_VERSION)).await;
        scenario
    }

    async fn seed_note(&self) {
        let prefix = table_prefix(self.active_version);
        self.ctx
            .execute(&format!(
                "INSERT INTO {prefix}gl_note (traversal_path, id, _version) \
                 VALUES ('1/100/', 5, '{SEED_VERSION}')"
            ))
            .await;
    }

    async fn seed_user(&self) {
        let prefix = table_prefix(self.active_version);
        self.ctx
            .execute(&format!(
                "INSERT INTO {prefix}gl_user (id, username, name, state, _version) \
                 VALUES (9, 'user9', 'User Nine', 'active', '{SEED_VERSION}')"
            ))
            .await;
    }

    async fn seed_merge_request(&self) {
        let prefix = table_prefix(self.active_version);
        self.ctx
            .execute(&format!(
                "INSERT INTO {prefix}gl_merge_request (traversal_path, id, _version) \
                 VALUES ('1/100/', 7, '{SEED_VERSION}')"
            ))
            .await;
    }

    async fn seed_edge(&self, kind: &str, source: (u64, &str), target: (u64, &str)) {
        let prefix = table_prefix(self.active_version);
        self.ctx
            .execute(&format!(
                "INSERT INTO {prefix}gl_edge \
                 (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version) \
                 VALUES ('1/100/', '{kind}', {}, '{}', {}, '{}', '{SEED_VERSION}')",
                source.0, source.1, target.0, target.1
            ))
            .await;
    }

    async fn seed_checkpoint(&self, key: &str) {
        let table = format!("{}checkpoint", table_prefix(self.active_version));
        insert_completed_checkpoint(&self.ctx, &table, key, SEED_VERSION).await;
    }

    async fn seed_code_checkpoint(&self, project_id: u64) {
        let prefix = table_prefix(self.active_version);
        self.ctx
            .execute(&format!(
                "INSERT INTO {prefix}code_indexing_checkpoint \
                 (traversal_path, project_id, branch, last_task_id, _version) \
                 VALUES ('1/100/', {project_id}, 'main', 1, 1)"
            ))
            .await;
    }

    async fn precreate_empty_target(&self, unprefixed: &str) {
        let new_prefix = table_prefix(*SCHEMA_VERSION);
        let target = prefixed_table_name(unprefixed, *SCHEMA_VERSION);
        let table = generate_graph_tables_with_prefix(&self.ontology, &new_prefix)
            .into_iter()
            .find(|table| table.name == target)
            .unwrap();
        self.ctx
            .create_client()
            .execute(&emit_create_table(&table))
            .await
            .unwrap();
    }

    async fn migrate(&self, scope: MigrationScope) {
        migration::prepare_tables_for_migration(
            &self.ctx.create_client(),
            &dictionary_source(&self.ctx.config),
            &self.ontology,
            &self.metrics,
            &scope,
            self.active_version,
        )
        .await
        .unwrap();
    }

    async fn complete_reindex(&self, key: &str) {
        let table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
        insert_completed_checkpoint(&self.ctx, &table, key, REINDEX_VERSION).await;
    }

    async fn mark_reindexing(&self, key: &str, cursor: &str) {
        let table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
        self.ctx
            .execute(&format!(
                "INSERT INTO {table} (key, watermark, cursor_values, _version) \
                 VALUES ('{key}', '{SEED_VERSION}', '{cursor}', '{SEED_VERSION}')"
            ))
            .await;
    }

    async fn assert_table_row_count(&self, unprefixed: &str, expected: i64) {
        let table = prefixed_table_name(unprefixed, *SCHEMA_VERSION);
        assert_eq!(
            self.count_final(&table).await,
            expected,
            "{table} should hold {expected} cloned row(s)"
        );
    }

    async fn assert_table_empty(&self, unprefixed: &str) {
        let table = prefixed_table_name(unprefixed, *SCHEMA_VERSION);
        assert_eq!(
            self.count_final(&table).await,
            0,
            "{table} should be rebuilt empty, not cloned"
        );
    }

    async fn assert_surviving_checkpoints(&self, expected: &[&str]) {
        let table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
        let surviving = self
            .ctx
            .query(&format!(
                "SELECT key FROM {table} FINAL WHERE _deleted = false ORDER BY key"
            ))
            .await;
        let expected: Vec<String> = expected.iter().map(|key| key.to_string()).collect();
        assert_eq!(
            String::extract_column(&surviving, 0).unwrap(),
            expected,
            "surviving checkpoint keys in {table} do not match the expected plan set"
        );
    }

    async fn assert_checkpoint_empty(&self) {
        let table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
        assert_eq!(
            self.count_final(&table).await,
            0,
            "{table} should seed no plans — everything re-indexes from epoch"
        );
    }

    async fn assert_code_checkpoint_empty(&self) {
        let table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
        assert_eq!(
            self.count_final(&table).await,
            0,
            "{table} should be rebuilt empty so the code backfill re-dispatches"
        );
    }

    async fn gate(
        &self,
        scope: MigrationScope,
        enabled_namespace_ids: &[i64],
    ) -> migration_completion::SdlcReindexProgress {
        migration_completion::get_sdlc_reindex_progress_for_enabled_namespaces(
            &self.ctx.create_client(),
            &self.ontology,
            &scope,
            &prefixed_table_name("checkpoint", *SCHEMA_VERSION),
            enabled_namespace_ids,
        )
        .await
        .unwrap()
    }

    async fn create_tables(&self, prefix: &str) {
        let client = self.ctx.create_client();
        for table in generate_graph_tables_with_prefix(&self.ontology, prefix) {
            client.execute(&emit_create_table(&table)).await.unwrap();
        }
    }

    async fn count_final(&self, table: &str) -> i64 {
        let batches = self
            .ctx
            .query(&format!(
                "SELECT toInt64(count()) AS cnt FROM {table} FINAL"
            ))
            .await;
        i64::extract_column(&batches, 0).unwrap()[0]
    }

    async fn seed_new_code_checkpoint_watermark(&self, project_id: u64, indexed_at: &str) {
        let table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
        self.ctx
            .execute(&format!(
                "INSERT INTO {table} (traversal_path, project_id, branch, last_task_id, indexed_at) \
                 VALUES ('1/100/', {project_id}, 'main', 1, '{indexed_at}')"
            ))
            .await;
    }

    async fn run_code_stale_sweep(&self) {
        let table_names =
            CodeTableNames::from_ontology(&self.ontology).expect("code tables must resolve");
        let store = Arc::new(ClickHouseCheckpointStore::new(Arc::new(
            self.ctx.config.build_client(),
        )));
        CodeStaleSweep::new(self.ctx.config.build_client(), &table_names, store)
            .run_for_drained(&["1/100/".to_string()])
            .await
            .expect("sweep failed");
    }

    async fn live_edge_exists(&self, kind: &str, source_id: u64) -> bool {
        let table = prefixed_table_name("gl_edge", *SCHEMA_VERSION);
        let rows = self
            .ctx
            .query(&format!(
                "SELECT source_id FROM {table} FINAL \
                 WHERE relationship_kind = '{kind}' AND source_id = {source_id} AND _deleted = false"
            ))
            .await;
        rows.first().is_some_and(|b| b.num_rows() > 0)
    }
}

async fn insert_completed_checkpoint(ctx: &TestContext, table: &str, key: &str, version: &str) {
    ctx.execute(&format!(
        "INSERT INTO {table} (key, watermark, cursor_values, _version) \
         VALUES ('{key}', '{version}', 'null', '{version}')"
    ))
    .await;
}

#[tokio::test]
async fn table_local_sdlc_scope_clones_unchanged_and_rebuilds_invalidated_table() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_user().await;
    scenario.seed_note().await;
    scenario.seed_merge_request().await;
    scenario
        .seed_edge("MENTIONS", (1, "User"), (2, "Note"))
        .await;

    scenario.migrate(sdlc(&["User"])).await;

    scenario.assert_table_row_count("gl_edge", 1).await;
    scenario.assert_table_empty("gl_user").await;
    scenario.assert_table_row_count("gl_note", 1).await;
    scenario.assert_table_row_count("gl_merge_request", 1).await;
}

#[tokio::test]
async fn table_local_sdlc_scope_seeds_only_unchanged_checkpoints() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_checkpoint("global.User").await;
    scenario.seed_checkpoint("ns.100.Job.p1of2").await;
    scenario.seed_checkpoint("ns.100.Job.p2of2").await;
    scenario.seed_checkpoint("ns.100.Note").await;
    scenario
        .seed_checkpoint("dispatch.sdlc.namespace.sweep")
        .await;

    scenario.migrate(sdlc(&["User"])).await;

    scenario
        .assert_surviving_checkpoints(&["ns.100.Job.p1of2", "ns.100.Job.p2of2", "ns.100.Note"])
        .await;
}

#[tokio::test]
async fn global_sdlc_gate_blocks_until_the_invalidated_plan_reindexes() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_checkpoint("global.User").await;

    scenario.migrate(sdlc(&["User"])).await;

    assert!(
        !scenario.gate(sdlc(&["User"]), &[100]).await.ready,
        "the gate must block until the invalidated plan has a completed checkpoint"
    );

    scenario.complete_reindex("global.User").await;
    assert!(
        scenario.gate(sdlc(&["User"]), &[100]).await.ready,
        "the gate must promote once the invalidated global plan has a completed checkpoint"
    );
}

#[tokio::test]
async fn shared_edge_scope_rebuilds_every_table() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_note().await;
    scenario.seed_checkpoint("ns.100.Note").await;
    scenario.seed_checkpoint("ns.100.User").await;

    scenario.migrate(sdlc(&["HAS_NOTE"])).await;

    scenario.assert_table_empty("gl_note").await;
    scenario.assert_table_empty("gl_edge").await;
    scenario.assert_checkpoint_empty().await;
}

#[tokio::test]
async fn interrupted_clone_is_recloned_without_duplicates_on_rerun() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_note().await;
    scenario.seed_user().await;
    scenario.seed_checkpoint("ns.100.User").await;
    scenario.seed_checkpoint("global.User").await;
    scenario.precreate_empty_target("gl_note").await;

    scenario.migrate(sdlc(&["User"])).await;
    scenario.migrate(sdlc(&["User"])).await;

    scenario.assert_table_row_count("gl_note", 1).await;
    scenario
        .assert_surviving_checkpoints(&["ns.100.User"])
        .await;
}

#[tokio::test]
async fn whole_sdlc_scope_rebuilds_tables_and_seeds_no_checkpoints() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_note().await;
    scenario.seed_checkpoint("ns.100.Note").await;
    scenario.seed_checkpoint("global.User").await;

    scenario.migrate(sdlc(&[])).await;

    scenario.assert_table_empty("gl_note").await;
    scenario.assert_checkpoint_empty().await;
}

#[tokio::test]
async fn code_scope_clones_sdlc_intact_and_drops_only_the_code_stale_sweep_gate() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario.seed_note().await;
    scenario.seed_checkpoint("ns.100.Note").await;
    scenario
        .seed_checkpoint("dispatch.sdlc.namespace.sweep")
        .await;
    scenario
        .seed_checkpoint("maintenance.code_stale_sweep")
        .await;
    scenario.seed_code_checkpoint(42).await;
    scenario
        .seed_edge("MENTIONS", (9, "User"), (5, "Note"))
        .await;
    scenario
        .seed_edge("CONTAINS", (1, "Directory"), (2, "File"))
        .await;

    scenario.migrate(MigrationScope::Code).await;

    scenario.assert_table_row_count("gl_note", 1).await;
    scenario.assert_table_row_count("gl_edge", 2).await;
    scenario
        .assert_surviving_checkpoints(&["dispatch.sdlc.namespace.sweep", "ns.100.Note"])
        .await;
    scenario.assert_code_checkpoint_empty().await;
}

#[tokio::test]
async fn code_migration_clone_converges_after_reindex_and_sweep() {
    let scenario = MigrationScenario::migrating_from_active().await;
    scenario
        .seed_edge("CONTAINS", (10, "Directory"), (20, "File"))
        .await;
    scenario
        .seed_edge("CONTAINS", (1, "Branch"), (30, "Directory"))
        .await;
    scenario
        .seed_edge("MENTIONS", (9, "User"), (5, "Note"))
        .await;

    scenario.migrate(MigrationScope::Code).await;
    scenario
        .seed_new_code_checkpoint_watermark(40, REINDEX_VERSION)
        .await;
    scenario.run_code_stale_sweep().await;

    assert!(
        !scenario.live_edge_exists("CONTAINS", 10).await,
        "the cloned Directory-source code edge must be tombstoned once its project re-indexes"
    );
    assert!(
        scenario.live_edge_exists("MENTIONS", 9).await,
        "SDLC edges must never be touched by the code stale sweep"
    );
    assert!(
        scenario.live_edge_exists("CONTAINS", 1).await,
        "known gap: Branch-source code edges are not swept until the sweep covers Branch/Project"
    );
}

#[tokio::test]
async fn gate_requires_every_enabled_namespace_to_complete() {
    let scenario = MigrationScenario::at_new_version().await;

    scenario
        .mark_reindexing("ns.100.Note", r#"{"c":["1/100/","5"]}"#)
        .await;
    assert!(
        !scenario.gate(sdlc(&["Note"]), &[100]).await.ready,
        "an in-progress cursor is not a completed pipeline"
    );

    scenario.complete_reindex("ns.100.Note").await;
    assert!(
        scenario.gate(sdlc(&["Note"]), &[100]).await.ready,
        "the newer completed row must replace the in-progress cursor under FINAL"
    );

    let one_of_two = scenario.gate(sdlc(&["Note"]), &[100, 200]).await;
    assert_eq!(one_of_two.completed_namespaces, 1);
    assert!(
        !one_of_two.ready,
        "one completed namespace out of two enabled must not promote"
    );

    scenario.complete_reindex("ns.200.Note").await;
    assert!(scenario.gate(sdlc(&["Note"]), &[100, 200]).await.ready);
}

#[tokio::test]
async fn gate_ignores_completed_checkpoints_for_disabled_namespaces() {
    let scenario = MigrationScenario::at_new_version().await;
    scenario.complete_reindex("ns.100.Note").await;

    let progress = scenario.gate(sdlc(&["Note"]), &[200]).await;

    assert_eq!(progress.completed_namespaces, 0);
    assert!(!progress.ready);
}

#[tokio::test]
async fn gate_requires_every_invalidated_pipeline_including_global() {
    let scenario = MigrationScenario::at_new_version().await;
    scenario.complete_reindex("ns.100.Note").await;
    scenario.complete_reindex("ns.200.Note").await;

    let missing_pipeline = scenario
        .gate(sdlc(&["Note", "MergeRequest"]), &[100, 200])
        .await;
    assert_eq!(missing_pipeline.completed_namespaces, 0);
    assert!(
        !missing_pipeline.ready,
        "a namespace missing one of the invalidated pipelines must not count as complete"
    );

    scenario.complete_reindex("ns.100.MergeRequest").await;
    scenario.complete_reindex("ns.200.MergeRequest").await;
    assert!(
        scenario
            .gate(sdlc(&["Note", "MergeRequest"]), &[100, 200])
            .await
            .ready
    );

    let global_pending = scenario.gate(sdlc(&["User"]), &[100, 200]).await;
    assert_eq!(
        global_pending.completed_namespaces, 0,
        "a global-only scope has no namespaced pipelines to count"
    );
    assert!(
        !global_pending.ready,
        "the gate must wait on the invalidated global pipeline"
    );

    scenario.complete_reindex("global.User").await;
    assert!(scenario.gate(sdlc(&["User"]), &[100, 200]).await.ready);
}

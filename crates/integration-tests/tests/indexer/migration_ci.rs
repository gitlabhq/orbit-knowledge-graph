use async_trait::async_trait;
use migration_framework::{
    Migration, MigrationContext, MigrationLedger, MigrationRegistry, MigrationType,
    build_migration_registry,
};

use crate::common::{GRAPH_SCHEMA_SQL, TestContext};

struct TestTableMigration {
    version: u64,
    name: &'static str,
    sql: &'static str,
}

#[async_trait]
impl Migration for TestTableMigration {
    fn version(&self) -> u64 {
        self.version
    }

    fn name(&self) -> &str {
        self.name
    }

    fn migration_type(&self) -> MigrationType {
        MigrationType::Additive
    }

    async fn prepare(&self, ctx: &MigrationContext) -> anyhow::Result<()> {
        ctx.execute_ddl(self.sql).await
    }
}

fn graph_sql_fixture_registry() -> MigrationRegistry {
    let mut registry = MigrationRegistry::new();
    registry.register(Box::new(TestTableMigration {
        version: 1,
        name: "checkpoint",
        sql: r#"
CREATE TABLE IF NOT EXISTS checkpoint (
    key String CODEC(ZSTD(1)),
    watermark DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    cursor_values String DEFAULT '' CODEC(ZSTD(1)),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1)),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (key)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1
"#,
    }));
    registry.register(Box::new(TestTableMigration {
        version: 2,
        name: "namespace_deletion_schedule",
        sql: r#"
CREATE TABLE IF NOT EXISTS namespace_deletion_schedule (
    namespace_id Int64 CODEC(ZSTD(1)),
    traversal_path String CODEC(ZSTD(1)),
    scheduled_deletion_date DateTime64(6, 'UTC') CODEC(Delta(8), ZSTD(1)),
    _version DateTime64(6, 'UTC') DEFAULT now64(6) CODEC(ZSTD(1)),
    _deleted Bool DEFAULT false
) ENGINE = ReplacingMergeTree(_version, _deleted)
ORDER BY (namespace_id, traversal_path)
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1
"#,
    }));
    registry
}

async fn run_all_migrations(ctx: &TestContext, registry: &MigrationRegistry) {
    let ledger = MigrationLedger::new(ctx.create_client());
    let migration_ctx = MigrationContext::new(ctx.create_client());
    ledger.ensure_table().await.expect("ledger table");

    for migration in registry.migrations() {
        ledger
            .mark_pending(migration.as_ref())
            .await
            .expect("pending");
        ledger
            .mark_preparing(migration.as_ref(), 0)
            .await
            .expect("preparing");
        migration
            .prepare(&migration_ctx)
            .await
            .expect("migration apply");
        ledger
            .mark_completed(migration.as_ref(), 0)
            .await
            .expect("completed");
    }
}

async fn create_statement(ctx: &TestContext, table: &str) -> String {
    let sql = format!("SHOW CREATE TABLE `{table}`");
    let result = ctx.query(&sql).await;
    let text = format!("{:?}", result[0].column(0));
    normalize_show_create(&text)
}

fn normalize_show_create(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace("\\n", " ")
        .replace("\\t", " ")
}

#[tokio::test]
async fn graph_sql_then_registered_migrations_are_idempotent_noops() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let registry = build_migration_registry();

    run_all_migrations(&ctx, &registry).await;

    let rows = MigrationLedger::new(ctx.create_client())
        .list()
        .await
        .expect("list rows");
    assert_eq!(rows.len(), registry.migrations().len());
    assert!(registry.is_empty());
}

#[tokio::test]
async fn migrations_only_schema_matches_graph_sql_for_covered_tables() {
    let ctx_from_graph_sql = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let ctx_from_migrations = TestContext::new(&[]).await;
    let registry = graph_sql_fixture_registry();

    run_all_migrations(&ctx_from_migrations, &registry).await;

    for table in ["checkpoint", "namespace_deletion_schedule"] {
        let graph_sql_schema = create_statement(&ctx_from_graph_sql, table).await;
        let migration_schema = create_statement(&ctx_from_migrations, table).await;
        assert_eq!(
            migration_schema, graph_sql_schema,
            "schema drift for {table}"
        );
    }
}

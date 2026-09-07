use std::sync::Arc;
use std::time::Duration;

use orbit_migrations::execute::{self, MigrationError};
use orbit_migrations::schema::{DictionaryCredentials, GraphSchema};
use thiserror::Error;
use tracing::{info, warn};

use super::metrics::MigrationMetrics;
use crate::campaign::{CampaignState, campaign_id_for_version};
use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::{LockError, LockGuard, LockService};
use orbit_migrations::ledger::MigrationLedger;
use orbit_migrations::version::{
    SCHEMA_VERSION, SchemaVersionError, mark_version_active, mark_version_migrating,
    read_active_version, table_prefix, version_tables_complete,
};

pub use orbit_migrations::execute::CHECKPOINT_TABLE;

const MIGRATION_LOCK_KEY: &str = "schema_migration";
const MIGRATION_LOCK_TTL: Duration = Duration::from_secs(120);
const LOCK_POLL_INTERVAL: Duration = Duration::from_secs(5);
const MAX_LOCK_WAIT_ITERATIONS: u32 = 60;

#[derive(Debug, Error)]
pub enum DispatcherMigrationError {
    #[error(transparent)]
    Migration(#[from] MigrationError),

    #[error(transparent)]
    SchemaVersion(#[from] SchemaVersionError),

    #[error("lock error: {0}")]
    Lock(#[from] LockError),

    #[error("migration lock held by another pod after {seconds}s; giving up")]
    LockTimeout { seconds: u64 },
}

pub async fn run_if_needed(
    graph: &ArrowClickHouseClient,
    credentials: &DictionaryCredentials,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
) -> Result<(), DispatcherMigrationError> {
    let active = read_active_version(graph).await?;
    let schema = GraphSchema::from_ontology(ontology);

    match active {
        None => {
            info!(
                version = *SCHEMA_VERSION,
                "fresh install — creating tables from ontology and recording initial schema version"
            );
            let prefix = table_prefix(*SCHEMA_VERSION);
            execute::create_all_versioned_tables(graph, &schema, credentials, &prefix).await?;
            metrics.record("create_tables", "success");
            mark_version_active(graph, *SCHEMA_VERSION).await?;
            metrics.record("complete", "fresh_install");
        }
        Some(version) if version == *SCHEMA_VERSION => {
            info!(
                version = *SCHEMA_VERSION,
                "schema version matches — no migration needed"
            );
            metrics.record("complete", "skipped");
        }
        Some(active_version) => {
            let guard = acquire_migration_lock(lock_service, metrics).await?;
            let result = if active_version > *SCHEMA_VERSION {
                warn!(
                    active_version,
                    embedded_version = *SCHEMA_VERSION,
                    "active schema version is newer than this binary — rolling back to the embedded version"
                );
                run_rollback(
                    graph,
                    credentials,
                    ontology,
                    &schema,
                    metrics,
                    campaign,
                    active_version,
                )
                .await
            } else {
                info!(
                    active_version,
                    target_version = *SCHEMA_VERSION,
                    "schema version mismatch detected — starting migration"
                );
                run_forward_migration(
                    graph,
                    credentials,
                    ontology,
                    &schema,
                    metrics,
                    campaign,
                    active_version,
                )
                .await
            };
            let _ = guard.release().await;
            result?;
        }
    }

    Ok(())
}

async fn run_forward_migration(
    graph: &ArrowClickHouseClient,
    credentials: &DictionaryCredentials,
    ontology: &ontology::Ontology,
    schema: &GraphSchema,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), DispatcherMigrationError> {
    if read_active_version(graph).await? == Some(*SCHEMA_VERSION) {
        info!(
            version = *SCHEMA_VERSION,
            "migration already completed by another pod — releasing lock"
        );
        metrics.record("complete", "skipped");
        return Ok(());
    }
    metrics.record("drain", "success");

    let ledger = MigrationLedger::load_embedded().map_err(MigrationError::Ledger)?;
    let requested_scope = ledger.resolve_scope_between(active_version, *SCHEMA_VERSION);
    info!(version = *SCHEMA_VERSION, %requested_scope, "preparing tables for schema migration");
    execute::create_tables_with_selective_cloning(
        graph,
        ontology,
        schema,
        credentials,
        &requested_scope,
        active_version,
        *SCHEMA_VERSION,
    )
    .await?;
    metrics.record("create_tables", "success");

    info!(
        version = *SCHEMA_VERSION,
        "marking schema version as migrating"
    );
    mark_version_migrating(graph, *SCHEMA_VERSION).await?;
    metrics.record("mark_migrating", "success");

    campaign.set(campaign_id_for_version(*SCHEMA_VERSION));
    metrics.record("complete", "success");
    info!(
        active_version,
        target_version = *SCHEMA_VERSION,
        new_prefix = %table_prefix(*SCHEMA_VERSION),
        "migration complete — indexer will write to new-prefix tables; \
         dispatcher backfill will repopulate via normal namespace poll cycle"
    );

    Ok(())
}

async fn run_rollback(
    graph: &ArrowClickHouseClient,
    credentials: &DictionaryCredentials,
    ontology: &ontology::Ontology,
    schema: &GraphSchema,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), DispatcherMigrationError> {
    if read_active_version(graph).await? == Some(*SCHEMA_VERSION) {
        info!(
            version = *SCHEMA_VERSION,
            "rollback already completed by another pod — releasing lock"
        );
        metrics.record("complete", "skipped");
        return Ok(());
    }

    let expected_names = schema.prefixed_table_names(&table_prefix(*SCHEMA_VERSION));

    if version_tables_complete(graph, *SCHEMA_VERSION, &expected_names).await? {
        info!(
            active_version,
            target_version = *SCHEMA_VERSION,
            "embedded version's table set is complete — rolling back via direct re-activation"
        );
        execute::reactivate_version(graph, *SCHEMA_VERSION).await?;
        metrics.record("complete", "rollback_reactivated");
        info!(
            version = *SCHEMA_VERSION,
            "rollback complete — resuming on existing tables"
        );
        return Ok(());
    }

    info!(
        active_version,
        target_version = *SCHEMA_VERSION,
        "embedded version's table set is incomplete — rolling back via rebuild"
    );
    execute::drop_all_version_entities(graph, *SCHEMA_VERSION).await?;

    run_forward_migration(
        graph,
        credentials,
        ontology,
        schema,
        metrics,
        campaign,
        active_version,
    )
    .await
}

async fn acquire_migration_lock(
    lock_service: &Arc<dyn LockService>,
    metrics: &MigrationMetrics,
) -> Result<LockGuard, DispatcherMigrationError> {
    for attempt in 0..MAX_LOCK_WAIT_ITERATIONS {
        if let Some(guard) =
            LockGuard::acquire(lock_service.clone(), MIGRATION_LOCK_KEY, MIGRATION_LOCK_TTL).await?
        {
            info!("acquired schema migration lock");
            metrics.record("acquire_lock", "success");
            return Ok(guard);
        }
        if attempt == 0 {
            info!("schema migration lock held by another pod — waiting for it to complete");
        }
        tokio::time::sleep(LOCK_POLL_INTERVAL).await;
    }

    let seconds = MAX_LOCK_WAIT_ITERATIONS as u64 * LOCK_POLL_INTERVAL.as_secs();
    metrics.record("acquire_lock", "failure");
    Err(DispatcherMigrationError::LockTimeout { seconds })
}

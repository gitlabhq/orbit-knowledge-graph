//! Schema migration orchestrator for zero-downtime table-prefix migrations.
//!
//! When the indexer starts and detects a mismatch between the embedded
//! `SCHEMA_VERSION` and the active version in `gkg_schema_version`, it runs
//! this migration flow before accepting any NATS messages:
//!
//! 1. **Acquire lock** — NATS KV `indexing_locks/schema_migration` (TTL-based).
//!    If another pod holds the lock, wait and skip — it is handling the migration.
//! 2. **Drain** — The engine has not started yet; no in-flight jobs exist.
//!    This phase is a no-op today but is reserved for future dual-write scenarios.
//! 3. **Create new-prefix tables** — Generate DDL from the ontology via
//!    `generate_graph_tables_with_prefix()` with the version prefix applied.
//! 4. **Mark migrating** — Insert the new version with status `migrating` in
//!    `gkg_schema_version`. The Webserver cutover (issue #441) switches reads.
//! 5. **Release lock** — Allow other pods to detect the migration is done.
//!
//! After this function returns, the indexer starts normally and writes to the
//! new-prefix tables. Because all new-prefix checkpoints are empty, the
//! dispatcher's normal namespace poll cycle re-dispatches backfill work
//! automatically — no explicit trigger is needed.

use std::sync::Arc;
use std::time::Duration;

use query_engine::compiler::{emit_create_table, generate_graph_tables_with_prefix};
use thiserror::Error;
use tracing::{info, warn};

use super::metrics::MigrationMetrics;

use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::{LockError, LockService};
use crate::schema::version::{
    SCHEMA_VERSION, SchemaVersionError, read_active_version, table_prefix, write_migrating_version,
    write_schema_version,
};

/// NATS KV key used to serialize schema migrations across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// TTL for the migration lock. Set high enough to cover DDL execution across all graph tables.
const MIGRATION_LOCK_TTL: Duration = Duration::from_secs(120);

/// How long to wait between polling for an active lock held by another pod.
const LOCK_POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Maximum number of lock-poll iterations before giving up.
const MAX_LOCK_WAIT_ITERATIONS: u32 = 60;

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("schema version error: {0}")]
    SchemaVersion(#[from] SchemaVersionError),

    #[error("lock error: {0}")]
    Lock(#[from] LockError),

    #[error("ClickHouse DDL error for table '{table}': {reason}")]
    Ddl { table: String, reason: String },

    #[error("migration lock held by another pod after {seconds}s; giving up")]
    LockTimeout { seconds: u64 },
}

/// Runs the pre-engine migration check.
///
/// Called by `indexer::run()` after NATS and ClickHouse are connected but
/// **before** `Engine::run()` starts consuming messages. This ensures there
/// are never in-flight NATS messages to drain.
///
/// # Behaviour
///
/// - **No mismatch** (active version == embedded version): returns immediately.
/// - **Fresh install** (no active version): creates all graph tables from the
///   ontology (using the version prefix), records embedded version as active,
///   and returns. No manual DDL (`graph.sql`) is needed.
/// - **Version mismatch**: acquires lock, creates new-prefix tables, marks
///   migrating, releases lock.
pub async fn run_if_needed(
    graph: &ArrowClickHouseClient,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    let active = read_active_version(graph).await?;

    match active {
        None => {
            info!(
                version = *SCHEMA_VERSION,
                "fresh install — creating tables from ontology and recording initial schema version"
            );
            create_prefixed_tables(graph, ontology, metrics).await?;
            write_schema_version(graph, *SCHEMA_VERSION).await?;
            metrics.record("complete", "fresh_install");
            Ok(())
        }
        Some(v) if v == *SCHEMA_VERSION => {
            info!(
                version = *SCHEMA_VERSION,
                "schema version matches embedded version — no migration needed"
            );
            metrics.record("complete", "skipped");
            Ok(())
        }
        Some(active_version) => {
            info!(
                active_version,
                target_version = *SCHEMA_VERSION,
                "schema version mismatch detected — starting migration"
            );
            run_migration(graph, lock_service, ontology, metrics, active_version).await
        }
    }
}

async fn run_migration(
    graph: &ArrowClickHouseClient,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    active_version: u32,
) -> Result<(), MigrationError> {
    // Phase 1: acquire distributed lock.
    acquire_migration_lock(lock_service, metrics).await?;

    // Re-read after acquiring the lock — another pod may have completed the
    // migration while we were waiting.
    let current_active = read_active_version(graph).await?;
    if let Some(v) = current_active
        && v == *SCHEMA_VERSION
    {
        info!(
            version = *SCHEMA_VERSION,
            "migration already completed by another pod — releasing lock"
        );
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "skipped");
        return Ok(());
    }

    // Phase 2: drain.
    // The engine has not started — no in-flight messages exist. This phase is
    // a no-op today and is reserved for future dual-write migration scenarios.
    info!(
        active_version,
        target_version = *SCHEMA_VERSION,
        "drain phase: engine not yet started — no in-flight messages to drain"
    );
    metrics.record("drain", "success");

    // Phase 3: create new-prefix tables.
    let create_result = create_prefixed_tables(graph, ontology, metrics).await;
    if let Err(ref e) = create_result {
        warn!(error = %e, "failed to create new-prefix tables — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return create_result;
    }

    // Phase 4: mark migrating in gkg_schema_version.
    info!(
        version = *SCHEMA_VERSION,
        "marking schema version as migrating"
    );
    let mark_result = write_migrating_version(graph, *SCHEMA_VERSION).await;
    if let Err(e) = mark_result {
        warn!(error = %e, "failed to mark migrating version — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return Err(e.into());
    }
    metrics.record("mark_migrating", "success");

    // Phase 5: release lock.
    let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
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

async fn acquire_migration_lock(
    lock_service: &Arc<dyn LockService>,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    for attempt in 0..MAX_LOCK_WAIT_ITERATIONS {
        match lock_service
            .try_acquire(MIGRATION_LOCK_KEY, MIGRATION_LOCK_TTL)
            .await?
        {
            true => {
                info!("acquired schema migration lock");
                metrics.record("acquire_lock", "success");
                return Ok(());
            }
            false => {
                if attempt == 0 {
                    info!("schema migration lock held by another pod — waiting for it to complete");
                }
                tokio::time::sleep(LOCK_POLL_INTERVAL).await;
            }
        }
    }

    let seconds = MAX_LOCK_WAIT_ITERATIONS as u64 * LOCK_POLL_INTERVAL.as_secs();
    metrics.record("acquire_lock", "failure");
    Err(MigrationError::LockTimeout { seconds })
}

async fn create_prefixed_tables(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
) -> Result<(), MigrationError> {
    let new_prefix = table_prefix(*SCHEMA_VERSION);
    let tables = generate_graph_tables_with_prefix(ontology, &new_prefix);

    for table in &tables {
        info!(table = %table.name, "creating table");
        graph
            .execute(&emit_create_table(table))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: table.name.clone(),
                reason: e.to_string(),
            })?;
    }

    info!(count = tables.len(), prefix = %new_prefix, "new-prefix tables created");
    metrics.record("create_tables", "success");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_graph_tables_produces_ddl_for_all_ontology_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = generate_graph_tables_with_prefix(&ontology, "");
        assert!(
            !tables.is_empty(),
            "generate_graph_tables must produce at least one table"
        );
        for table in &tables {
            let ddl = emit_create_table(table);
            assert!(
                ddl.contains("CREATE TABLE IF NOT EXISTS"),
                "DDL for '{}' must start with CREATE TABLE IF NOT EXISTS: {ddl}",
                table.name
            );
        }
    }

    #[test]
    fn generate_graph_tables_includes_auxiliary_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = generate_graph_tables_with_prefix(&ontology, "");
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in [
            "checkpoint",
            "code_indexing_checkpoint",
            "namespace_deletion_schedule",
        ] {
            assert!(
                names.contains(&expected),
                "expected auxiliary table '{expected}' in generated tables; got: {names:?}"
            );
        }
    }

    #[test]
    fn prefixed_tables_have_correct_names() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let prefix = "v2_";
        let tables = generate_graph_tables_with_prefix(&ontology, prefix);
        for table in &tables {
            assert!(
                table.name.starts_with(prefix),
                "table '{}' should be prefixed with '{prefix}'",
                table.name
            );
            let ddl = emit_create_table(table);
            assert!(
                ddl.contains(&format!("CREATE TABLE IF NOT EXISTS {}", table.name)),
                "DDL should contain prefixed table name '{}': {ddl}",
                table.name
            );
        }
    }

    #[test]
    fn migration_metrics_new_does_not_panic() {
        let _metrics = MigrationMetrics::new();
    }

    #[test]
    fn lock_key_is_stable() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }
}

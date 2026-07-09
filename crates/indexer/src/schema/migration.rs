//! Schema migration orchestrator for zero-downtime table-prefix migrations.
//!
//! The dispatcher runs this flow at boot, before starting its task loops, when
//! the embedded `SCHEMA_VERSION` differs from the active version in
//! `gkg_schema_version`. Indexers do not migrate; they wait for the version to
//! become ready via [`crate::schema::version::wait_until_ready`].
//!
//! **Forward migration** (`active < SCHEMA_VERSION`):
//!
//! 1. **Acquire lock** — NATS KV `indexing_locks/schema_migration` (TTL-based),
//!    serializing migration across dispatcher replicas.
//! 2. **Create new-prefix tables** — DDL from the ontology via
//!    `generate_graph_tables_with_prefix()` with the version prefix applied.
//! 3. **Mark migrating** — Insert the new version with status `migrating` in
//!    `gkg_schema_version`, signalling indexers that the tables exist.
//! 4. **Release lock**.
//!
//! New-prefix checkpoints start empty, so the dispatcher's normal namespace
//! poll cycle re-dispatches backfill work automatically.
//!
//! **Rollback** (`active > SCHEMA_VERSION`, an older binary was deployed):
//! see [`run_rollback`] for the two cases (tables retained vs. already GC'd).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::UInt64Type;
use gkg_utils::arrow::ArrowUtils;
use ontology::migrations::{MigrationLedger, MigrationScope};
use query_engine::compiler::{
    DictionarySource, emit_create_dictionary, emit_create_materialized_view, emit_create_table,
    generate_graph_dictionaries_with_prefix, generate_graph_materialized_views_with_prefix,
    generate_graph_tables_with_prefix,
};
use thiserror::Error;
use tracing::{info, warn};

use super::metrics::MigrationMetrics;

use crate::campaign::{CampaignState, campaign_id_for_version};
use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::{LockError, LockService};
use crate::schema::invalidation::find_invalidated_pipelines;
use crate::schema::invalidation::{TableMigrationAction, classify_tables_for_scope};
use crate::schema::version::{
    SCHEMA_VERSION, SchemaVersionError, drop_kind_for_engine, list_version_objects,
    mark_version_active, mark_version_retired, read_active_version, read_all_versions,
    table_prefix, version_tables_complete, write_migrating_version, write_schema_version,
};

/// NATS KV key used to serialize schema migrations across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// TTL for the migration lock. Set high enough to cover DDL execution across all graph tables.
const MIGRATION_LOCK_TTL: Duration = Duration::from_secs(120);

const LOCK_POLL_INTERVAL: Duration = Duration::from_secs(5);

const MAX_LOCK_WAIT_ITERATIONS: u32 = 60;

// TODO: move to the ontology as the single source for checkpoint table names.
pub(crate) const CHECKPOINT_TABLE: &str = "checkpoint";

/// Clones the active checkpoint into the new version, dropping dispatch cursors (so the
/// cold-start sweep re-fires) and the invalidated plans' keys (so they re-index from epoch).
const SEED_CHECKPOINT_SQL: &str = "\
INSERT INTO {new_table:Identifier} \
SELECT * FROM {old_table:Identifier} FINAL \
WHERE _deleted = false \
  AND NOT startsWith(key, 'dispatch.') \
  AND NOT ( \
    (splitByChar('.', key)[1] = 'ns' AND splitByChar('.', key)[3] IN {ns_plans:Array(String)}) \
    OR (splitByChar('.', key)[1] = 'global' AND splitByChar('.', key)[2] IN {global_plans:Array(String)}) \
  )";

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

    #[error("migration ledger error: {0}")]
    Ledger(String),
}

/// Runs the migration check. Must be called at boot, before the task loops
/// start. Fresh install (no active version) creates all graph tables from the
/// ontology; no manual DDL (`graph.sql`) is needed.
pub async fn run_if_needed(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
) -> Result<(), MigrationError> {
    let active = read_active_version(graph).await?;

    match active {
        None => {
            info!(
                version = *SCHEMA_VERSION,
                "fresh install — creating tables from ontology and recording initial schema version"
            );
            create_prefixed_tables(graph, source, ontology, metrics).await?;
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
        Some(active_version) if active_version > *SCHEMA_VERSION => {
            warn!(
                active_version,
                embedded_version = *SCHEMA_VERSION,
                "active schema version is newer than this binary — rolling back to the embedded version"
            );
            run_rollback(
                graph,
                source,
                lock_service,
                ontology,
                metrics,
                campaign,
                active_version,
            )
            .await
        }
        Some(active_version) => {
            info!(
                active_version,
                target_version = *SCHEMA_VERSION,
                "schema version mismatch detected — starting migration"
            );
            run_migration(
                graph,
                source,
                lock_service,
                ontology,
                metrics,
                campaign,
                active_version,
            )
            .await
        }
    }
}

pub async fn clone_unchanged_migration_tables(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    scope: &MigrationScope,
    active_version: u32,
) -> Result<(), MigrationError> {
    let new_prefix = table_prefix(*SCHEMA_VERSION);
    let old_prefix = table_prefix(active_version);
    let actions = classify_tables_for_scope(ontology, scope);

    let existing_old = version_object_names(graph, active_version).await?;
    let existing_new = version_object_names(graph, *SCHEMA_VERSION).await?;

    let tables = generate_graph_tables_with_prefix(ontology, &new_prefix);
    let mut cloned = 0;
    let mut rebuilt = 0;
    let mut seeded = 0;
    for table in &tables {
        let base = table.name.strip_prefix(&new_prefix).unwrap_or(&table.name);

        if base == CHECKPOINT_TABLE && matches!(scope, MigrationScope::Sdlc(_)) {
            seed_sdlc_checkpoint(graph, ontology, table, &old_prefix, scope).await?;
            seeded += 1;
            continue;
        }

        let old_name = format!("{old_prefix}{base}");
        let clone_from_active = matches!(
            actions.get(base),
            Some(TableMigrationAction::CloneFromActive)
        ) && existing_old.contains(&old_name);

        if clone_from_active {
            clone_table(graph, &old_name, &table.name, &existing_new).await?;
            cloned += 1;
        } else {
            info!(table = %table.name, "rebuilding table empty");
            graph
                .execute(&emit_create_table(table))
                .await
                .map_err(|e| MigrationError::Ddl {
                    table: table.name.clone(),
                    reason: e.to_string(),
                })?;
            rebuilt += 1;
        }
    }

    info!(cloned, rebuilt, seeded, prefix = %new_prefix, "clone-based migration tables prepared");

    create_dictionaries_and_views(graph, source, ontology, &new_prefix).await?;
    metrics.record("create_tables", "success");
    Ok(())
}

/// Rolls back to the embedded `SCHEMA_VERSION` after an older binary is deployed over a newer
/// active version; re-activate vs. rebuild is decided by table-set completeness, not lag-prone status.
async fn run_rollback(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), MigrationError> {
    acquire_migration_lock(lock_service, metrics).await?;

    let current_active = read_active_version(graph).await?;
    if current_active == Some(*SCHEMA_VERSION) {
        info!(
            version = *SCHEMA_VERSION,
            "rollback already completed by another pod — releasing lock"
        );
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "skipped");
        return Ok(());
    }

    let prefix = table_prefix(*SCHEMA_VERSION);
    let expected = embedded_object_names(ontology, &prefix);

    let tables_complete = match version_tables_complete(graph, *SCHEMA_VERSION, &expected).await {
        Ok(complete) => complete,
        Err(e) => {
            warn!(error = %e, "failed to check embedded version's table-set completeness — releasing lock");
            let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
            return Err(e.into());
        }
    };

    if tables_complete {
        info!(
            active_version,
            target_version = *SCHEMA_VERSION,
            "embedded version's table set is complete — rolling back via direct re-activation"
        );

        let reactivate_result = reactivate_version(graph, *SCHEMA_VERSION).await;
        if let Err(e) = reactivate_result {
            warn!(error = %e, "failed to re-activate embedded version — releasing lock");
            let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
            return Err(e.into());
        }

        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
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

    // Creation is IF NOT EXISTS: a surviving checkpoint would make the backfill skip indexed projects.
    if let Err(e) = drop_stale_version_objects(graph, *SCHEMA_VERSION).await {
        warn!(error = %e, "failed to clear stale embedded-version objects before rebuild — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return Err(e);
    }

    run_migration_locked(
        graph,
        source,
        lock_service,
        ontology,
        metrics,
        campaign,
        active_version,
    )
    .await
}

/// Every object name `create_prefixed_tables` creates for `prefix` — tables, dictionaries,
/// and materialized views all land in `system.tables`, so all three count for completeness.
fn embedded_object_names(ontology: &ontology::Ontology, prefix: &str) -> Vec<String> {
    let mut names: Vec<String> = generate_graph_tables_with_prefix(ontology, prefix)
        .into_iter()
        .map(|t| t.name)
        .collect();
    names.extend(
        generate_graph_dictionaries_with_prefix(ontology, prefix)
            .into_iter()
            .map(|d| d.name),
    );
    names.extend(
        generate_graph_materialized_views_with_prefix(ontology, prefix)
            .into_iter()
            .map(|v| v.name),
    );
    names
}

/// Drops every surviving `v<version>_*` object; views and dictionaries select from tables, so they go first.
async fn drop_stale_version_objects(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), MigrationError> {
    let mut objects = list_version_objects(graph, version).await?;
    objects.sort_by_key(|o| match drop_kind_for_engine(&o.engine) {
        "VIEW" => 0,
        "DICTIONARY" => 1,
        _ => 2,
    });

    for object in &objects {
        let kind = drop_kind_for_engine(&object.engine);
        info!(object = %object.name, kind, "dropping stale object before rollback rebuild");
        graph
            .execute(&format!("DROP {kind} IF EXISTS {}", object.name))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: object.name.clone(),
                reason: e.to_string(),
            })?;
    }

    Ok(())
}

async fn reactivate_version(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<(), SchemaVersionError> {
    // Activate first: a crash between the two writes leaves a duplicate active row, never zero.
    mark_version_active(graph, version).await?;
    let versions = read_all_versions(graph).await?;
    for entry in &versions {
        if entry.status == "active" && entry.version != version {
            mark_version_retired(graph, entry.version).await?;
        }
    }
    Ok(())
}

async fn run_migration(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), MigrationError> {
    acquire_migration_lock(lock_service, metrics).await?;

    let current_active = read_active_version(graph).await?;
    if current_active == Some(*SCHEMA_VERSION) {
        info!(
            version = *SCHEMA_VERSION,
            "migration already completed by another pod — releasing lock"
        );
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        metrics.record("complete", "skipped");
        return Ok(());
    }

    run_migration_locked(
        graph,
        source,
        lock_service,
        ontology,
        metrics,
        campaign,
        active_version,
    )
    .await
}

/// Caller must hold the migration lock; released on every exit path.
async fn run_migration_locked(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    lock_service: &Arc<dyn LockService>,
    ontology: &ontology::Ontology,
    metrics: &MigrationMetrics,
    campaign: &CampaignState,
    active_version: u32,
) -> Result<(), MigrationError> {
    // Drain is a no-op: the dispatcher runs no engine, so no in-flight messages
    // exist. Reserved for future dual-write scenarios.
    metrics.record("drain", "success");

    let scope = match MigrationLedger::load_embedded() {
        Ok(ledger) => ledger.resolve_migration_scope_between(active_version, *SCHEMA_VERSION),
        Err(e) => {
            warn!(error = %e, "failed to load migration ledger — releasing lock");
            let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
            return Err(MigrationError::Ledger(e));
        }
    };

    let create_result = if matches!(scope, MigrationScope::Full) {
        create_prefixed_tables(graph, source, ontology, metrics).await
    } else {
        info!(version = *SCHEMA_VERSION, %scope, "clone-based migration — cloning unchanged tables");
        clone_unchanged_migration_tables(graph, source, ontology, metrics, &scope, active_version)
            .await
    };
    if let Err(ref e) = create_result {
        warn!(error = %e, "failed to create new-prefix tables — releasing lock");
        let _ = lock_service.release(MIGRATION_LOCK_KEY).await;
        return create_result;
    }

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

    campaign.set(campaign_id_for_version(*SCHEMA_VERSION));

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
    source: &DictionarySource<'_>,
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

    create_dictionaries_and_views(graph, source, ontology, &new_prefix).await?;
    metrics.record("create_tables", "success");
    Ok(())
}

async fn seed_sdlc_checkpoint(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    new_table: &query_engine::compiler::ast::ddl::CreateTable,
    old_prefix: &str,
    scope: &MigrationScope,
) -> Result<(), MigrationError> {
    graph
        .execute(&emit_create_table(new_table))
        .await
        .map_err(|e| MigrationError::Ddl {
            table: new_table.name.clone(),
            reason: e.to_string(),
        })?;

    let pipelines = find_invalidated_pipelines(ontology, scope);

    let old_table = format!("{old_prefix}{CHECKPOINT_TABLE}");
    info!(from = %old_table, to = %new_table.name, "seeding checkpoint from active version");
    graph
        .query(SEED_CHECKPOINT_SQL)
        .param("new_table", &new_table.name)
        .param("old_table", &old_table)
        .param("ns_plans", &pipelines.namespaced)
        .param("global_plans", &pipelines.global)
        .execute()
        .await
        .map_err(|e| MigrationError::Ddl {
            table: new_table.name.clone(),
            reason: e.to_string(),
        })
}

/// ClickHouse `CLONE AS` can leave an empty shell after an interrupted attach.
async fn clone_table(
    graph: &ArrowClickHouseClient,
    old_name: &str,
    new_name: &str,
    existing_new: &HashSet<String>,
) -> Result<(), MigrationError> {
    if existing_new.contains(new_name)
        && count_rows(graph, new_name).await? == 0
        && count_rows(graph, old_name).await? > 0
    {
        warn!(table = %new_name, "re-cloning empty shell left by an interrupted migration");
        graph
            .execute(&format!("DROP TABLE IF EXISTS {new_name}"))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: new_name.to_string(),
                reason: e.to_string(),
            })?;
    }

    info!(from = %old_name, to = %new_name, "cloning table from active version");
    graph
        .execute(&format!(
            "CREATE TABLE IF NOT EXISTS {new_name} CLONE AS {old_name}"
        ))
        .await
        .map_err(|e| MigrationError::Ddl {
            table: new_name.to_string(),
            reason: e.to_string(),
        })
}

async fn create_dictionaries_and_views(
    graph: &ArrowClickHouseClient,
    source: &DictionarySource<'_>,
    ontology: &ontology::Ontology,
    new_prefix: &str,
) -> Result<(), MigrationError> {
    let dicts = generate_graph_dictionaries_with_prefix(ontology, new_prefix);
    for dict in &dicts {
        info!(dictionary = %dict.name, source = %dict.source_table, "creating dictionary");
        graph
            .execute(&emit_create_dictionary(dict, source))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: dict.name.clone(),
                reason: e.to_string(),
            })?;
    }

    // Materialized views depend on the tables they SELECT FROM, so they
    // must be created after all tables exist.
    let views = generate_graph_materialized_views_with_prefix(ontology, new_prefix);
    for view in &views {
        info!(view = %view.name, "creating materialized view");
        graph
            .execute(&emit_create_materialized_view(view))
            .await
            .map_err(|e| MigrationError::Ddl {
                table: view.name.clone(),
                reason: e.to_string(),
            })?;
    }

    info!(
        dictionaries = dicts.len(),
        views = views.len(),
        prefix = %new_prefix,
        "dictionaries and materialized views created"
    );
    Ok(())
}

async fn version_object_names(
    graph: &ArrowClickHouseClient,
    version: u32,
) -> Result<HashSet<String>, MigrationError> {
    Ok(list_version_objects(graph, version)
        .await?
        .into_iter()
        .map(|object| object.name)
        .collect())
}

async fn count_rows(graph: &ArrowClickHouseClient, table: &str) -> Result<u64, MigrationError> {
    let batches = graph
        .query(&format!("SELECT count() AS cnt FROM {table}"))
        .fetch_arrow()
        .await
        .map_err(|e| MigrationError::Ddl {
            table: table.to_string(),
            reason: e.to_string(),
        })?;
    Ok(batches
        .first()
        .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "cnt", 0))
        .unwrap_or(0))
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

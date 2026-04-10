//! Migration completion detection and old table cleanup.
//!
//! Runs as a scheduled task in the DispatchIndexing mode. On each tick:
//!
//! 1. **Completion detection** — If a version is in `migrating` state, check
//!    whether all enabled namespaces have checkpoint entries in the new-prefix
//!    tables. If so, promote the version to `active` and demote the previously
//!    active version to `retired`.
//!
//! 2. **Retention cleanup** — Drop tables for versions outside the
//!    `max_retained_versions` window that have status `retired`, then mark
//!    them `dropped`.

use std::sync::Arc;

use async_trait::async_trait;
use gkg_server_config::{MigrationCompletionConfig, ScheduleConfiguration, SchemaConfig};
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::LockService;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use query_engine::compiler::generate_graph_tables;

use crate::schema::version::{
    VersionEntry, mark_version_active, mark_version_dropped, mark_version_retired,
    read_all_versions, read_migrating_version, table_prefix,
};

/// NATS KV key used to serialize migration-completion checks across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// NATS KV lock TTL for the completion check.
const LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(120);

/// SQL to count distinct namespace prefixes in the new-prefix checkpoint table.
/// A completed namespace has at least one checkpoint key starting with `ns.`.
const COUNT_CHECKPOINT_NAMESPACES: &str = "\
SELECT count(DISTINCT extractAll(key, '^ns\\.(\\d+)')[1]) AS ns_count \
FROM {table:Identifier} FINAL \
WHERE key LIKE 'ns.%' AND _deleted = false";

/// SQL to count enabled namespaces from the datalake.
const COUNT_ENABLED_NAMESPACES: &str = "\
SELECT count(DISTINCT root_namespace_id) AS ns_count \
FROM siphon_knowledge_graph_enabled_namespaces \
WHERE _siphon_deleted = false";

/// Pre-built OTel instruments for migration completion observability.
#[derive(Clone)]
pub struct CompletionMetrics {
    migration_completed: Counter<u64>,
    cleanup_total: Counter<u64>,
}

impl CompletionMetrics {
    pub fn new() -> Self {
        let meter = global::meter("gkg_schema_migration");
        let migration_completed = meter
            .u64_counter("gkg_schema_migration_completed_total")
            .with_description("Total successful schema migration completions")
            .build();
        let cleanup_total = meter
            .u64_counter("gkg_schema_cleanup_total")
            .with_description("Total schema table cleanup operations by version and result")
            .build();
        Self {
            migration_completed,
            cleanup_total,
        }
    }

    fn record_migration_completed(&self) {
        self.migration_completed.add(1, &[]);
    }

    fn record_cleanup(&self, version: u32, result: &'static str) {
        self.cleanup_total.add(
            1,
            &[
                KeyValue::new("version", version.to_string()),
                KeyValue::new("result", result),
            ],
        );
    }
}

impl Default for CompletionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Scheduled task that detects migration completion and cleans up old tables.
pub struct MigrationCompletionChecker {
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    lock_service: Arc<dyn LockService>,
    ontology: Arc<ontology::Ontology>,
    schema_config: SchemaConfig,
    config: MigrationCompletionConfig,
    metrics: CompletionMetrics,
    _task_metrics: ScheduledTaskMetrics,
}

impl MigrationCompletionChecker {
    pub fn new(
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        lock_service: Arc<dyn LockService>,
        ontology: Arc<ontology::Ontology>,
        schema_config: SchemaConfig,
        config: MigrationCompletionConfig,
        task_metrics: ScheduledTaskMetrics,
    ) -> Self {
        Self {
            graph,
            datalake,
            lock_service,
            ontology,
            schema_config,
            config,
            metrics: CompletionMetrics::new(),
            _task_metrics: task_metrics,
        }
    }
}

#[async_trait]
impl ScheduledTask for MigrationCompletionChecker {
    fn name(&self) -> &str {
        "migration_completion"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        // Acquire the migration lock to prevent concurrent checks.
        let acquired = self
            .lock_service
            .try_acquire(MIGRATION_LOCK_KEY, LOCK_TTL)
            .await
            .map_err(|e| TaskError::new(format!("lock error: {e}")))?;

        if !acquired {
            return Ok(());
        }

        let result = self.run_inner().await;

        let _ = self.lock_service.release(MIGRATION_LOCK_KEY).await;

        result
    }
}

impl MigrationCompletionChecker {
    async fn run_inner(&self) -> Result<(), TaskError> {
        // Phase 1: detect completion of any migrating version.
        self.check_completion().await?;

        // Phase 2: clean up old retired versions outside retention window.
        self.cleanup_old_versions().await?;

        Ok(())
    }

    /// Checks whether a `migrating` version has been fully re-indexed and
    /// should be promoted to `active`.
    async fn check_completion(&self) -> Result<(), TaskError> {
        let migrating = read_migrating_version(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read migrating version: {e}")))?;

        let Some(migrating_version) = migrating else {
            return Ok(());
        };

        info!(
            version = migrating_version,
            "checking migration completion for migrating version"
        );

        let complete = self
            .is_migration_complete(migrating_version)
            .await
            .map_err(|e| {
                TaskError::new(format!("completion check for v{migrating_version}: {e}"))
            })?;

        if !complete {
            info!(
                version = migrating_version,
                "migration not yet complete — namespaces still being indexed"
            );
            return Ok(());
        }

        // Promote: migrating → active, old active → retired.
        let versions = read_all_versions(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read all versions: {e}")))?;

        let old_active: Vec<&VersionEntry> = versions
            .iter()
            .filter(|v| v.status == "active" && v.version != migrating_version)
            .collect();

        for entry in &old_active {
            info!(
                version = entry.version,
                "marking old active version as retired"
            );
            mark_version_retired(&self.graph, entry.version)
                .await
                .map_err(|e| TaskError::new(format!("mark v{} retired: {e}", entry.version)))?;
        }

        info!(
            version = migrating_version,
            "marking migrating version as active — schema migration complete"
        );
        mark_version_active(&self.graph, migrating_version)
            .await
            .map_err(|e| TaskError::new(format!("mark v{migrating_version} active: {e}")))?;

        self.metrics.record_migration_completed();

        info!(
            version = migrating_version,
            "schema migration to v{migrating_version} complete"
        );

        Ok(())
    }

    /// Returns `true` if all enabled namespaces have checkpoint entries in the
    /// new-prefix tables.
    async fn is_migration_complete(&self, version: u32) -> Result<bool, String> {
        let prefix = table_prefix(version);
        let checkpoint_table = format!("{prefix}checkpoint");

        // Count namespaces that have been indexed into the new checkpoint table.
        let indexed_count = self
            .count_checkpoint_namespaces(&checkpoint_table)
            .await
            .map_err(|e| format!("count checkpoint namespaces: {e}"))?;

        // Count enabled namespaces from the datalake.
        let enabled_count = self
            .count_enabled_namespaces()
            .await
            .map_err(|e| format!("count enabled namespaces: {e}"))?;

        info!(
            version,
            indexed_namespaces = indexed_count,
            enabled_namespaces = enabled_count,
            "migration completion status"
        );

        if enabled_count == 0 {
            // No namespaces enabled — nothing to index.
            return Ok(true);
        }

        // All enabled namespaces must have at least one checkpoint entry.
        Ok(indexed_count >= enabled_count)
    }

    async fn count_checkpoint_namespaces(&self, table: &str) -> Result<u64, String> {
        let batches = self
            .graph
            .query(COUNT_CHECKPOINT_NAMESPACES)
            .param("table", table)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let col = batch
                .column_by_name("ns_count")
                .ok_or_else(|| "missing ns_count column".to_string())?;
            let col = col
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| "ns_count is not UInt64".to_string())?;
            return Ok(col.value(0));
        }

        Ok(0)
    }

    async fn count_enabled_namespaces(&self) -> Result<u64, String> {
        let batches = self
            .datalake
            .query_arrow(COUNT_ENABLED_NAMESPACES)
            .await
            .map_err(|e| e.to_string())?;

        for batch in &batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let col = batch
                .column_by_name("ns_count")
                .ok_or_else(|| "missing ns_count column".to_string())?;
            let col = col
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .ok_or_else(|| "ns_count is not UInt64".to_string())?;
            return Ok(col.value(0));
        }

        Ok(0)
    }

    /// Drops tables for retired versions outside the retention window, then
    /// marks them `dropped`.
    async fn cleanup_old_versions(&self) -> Result<(), TaskError> {
        let versions = read_all_versions(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read all versions: {e}")))?;

        // Keep the top `max_retained_versions` non-dropped entries.
        let retained: Vec<&VersionEntry> =
            versions.iter().filter(|v| v.status != "dropped").collect();

        let max = self.schema_config.max_retained_versions as usize;
        if retained.len() <= max {
            return Ok(());
        }

        let to_cleanup: Vec<&VersionEntry> = retained[max..].to_vec();

        for entry in to_cleanup {
            if entry.status != "retired" {
                // Only drop tables for retired versions.
                continue;
            }

            info!(
                version = entry.version,
                "dropping tables for retired version outside retention window"
            );

            match self.drop_version_tables(entry.version).await {
                Ok(()) => {
                    mark_version_dropped(&self.graph, entry.version)
                        .await
                        .map_err(|e| {
                            TaskError::new(format!("mark v{} dropped: {e}", entry.version))
                        })?;
                    self.metrics.record_cleanup(entry.version, "success");
                    info!(
                        version = entry.version,
                        "version tables dropped and marked as dropped"
                    );
                }
                Err(e) => {
                    self.metrics.record_cleanup(entry.version, "failure");
                    warn!(
                        version = entry.version,
                        error = %e,
                        "failed to drop tables for retired version"
                    );
                }
            }
        }

        Ok(())
    }

    /// Drops all graph tables for a given schema version.
    async fn drop_version_tables(&self, version: u32) -> Result<(), String> {
        let prefix = table_prefix(version);
        let tables: Vec<String> = generate_graph_tables(&self.ontology)
            .into_iter()
            .map(|t| t.name)
            .collect();

        for table_name in &tables {
            let prefixed = format!("{prefix}{table_name}");
            let ddl = format!("DROP TABLE IF EXISTS {prefixed}");

            info!(
                version,
                table = %prefixed,
                "dropping table"
            );

            self.graph
                .execute(&ddl)
                .await
                .map_err(|e| format!("DROP TABLE {prefixed}: {e}"))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_metrics_new_does_not_panic() {
        let _metrics = CompletionMetrics::new();
    }

    #[test]
    fn default_config_has_cron() {
        let config = MigrationCompletionConfig::default();
        assert!(config.schedule.cron.is_some());
    }

    #[test]
    fn count_checkpoint_query_uses_identifier_param() {
        assert!(
            COUNT_CHECKPOINT_NAMESPACES.contains("{table:Identifier}"),
            "checkpoint query must use Identifier param for table name"
        );
    }

    #[test]
    fn count_checkpoint_query_filters_deleted() {
        assert!(COUNT_CHECKPOINT_NAMESPACES.contains("_deleted = false"));
    }

    #[test]
    fn count_enabled_namespaces_query_filters_deleted() {
        assert!(COUNT_ENABLED_NAMESPACES.contains("_siphon_deleted = false"));
    }

    #[test]
    fn migration_lock_key_matches_schema_migration() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }
}

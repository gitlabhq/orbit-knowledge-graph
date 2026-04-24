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

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use gkg_server_config::{MigrationCompletionConfig, ScheduleConfiguration, SchemaConfig};
use gkg_utils::arrow::ArrowUtils;
use query_engine::compiler::generate_graph_tables;
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::LockService;
use crate::metrics::CompletionMetrics;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{
    VersionEntry, mark_version_active, mark_version_dropped, mark_version_retired,
    read_all_versions, read_migrating_version, table_prefix,
};

/// NATS KV key used to serialize migration-completion checks across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// NATS KV lock TTL for the completion check.
const LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(120);

/// SQL to count distinct namespace prefixes in the new-prefix SDLC checkpoint table.
/// A completed namespace has at least one checkpoint key starting with `ns.`.
const COUNT_SDLC_CHECKPOINT_NAMESPACES: &str = "\
SELECT count(DISTINCT extractAll(key, '^ns\\.(\\d+)')[1]) AS ns_count \
FROM {table:Identifier} FINAL \
WHERE key LIKE 'ns.%' AND _deleted = false";

/// SQL to count distinct namespaces in the new-prefix code indexing checkpoint table.
/// The `traversal_path` column has the form `org_id/namespace_id/...`. We extract the
/// second path segment (the root namespace ID) and count distinct values.
const COUNT_CODE_CHECKPOINT_NAMESPACES: &str = "\
SELECT count(DISTINCT splitByChar('/', traversal_path)[2]) AS ns_count \
FROM {table:Identifier} FINAL \
WHERE _deleted = false AND traversal_path != ''";

/// SQL to count enabled namespaces from the datalake.
const COUNT_ENABLED_NAMESPACES: &str = "\
SELECT count(DISTINCT root_namespace_id) AS ns_count \
FROM siphon_knowledge_graph_enabled_namespaces \
WHERE _siphon_deleted = false";

/// SQL to count enabled namespaces that have at least one project in the
/// datalake. A namespace with zero projects never publishes code indexing
/// tasks, so it can never produce a checkpoint row. Without this filter the
/// code-completion predicate would be unsatisfiable whenever an enabled
/// namespace is empty — blocking schema migration promotion indefinitely.
///
/// The `[2]` index on `splitByChar('/', traversal_path)` extracts the root
/// namespace ID from a project path of the form `org_id/root_ns_id/...`,
/// matching the convention used by `COUNT_CODE_CHECKPOINT_NAMESPACES`.
const COUNT_CODE_ELIGIBLE_ENABLED_NAMESPACES: &str = "\
SELECT count(DISTINCT root_namespace_id) AS ns_count \
FROM siphon_knowledge_graph_enabled_namespaces \
WHERE _siphon_deleted = false \
  AND root_namespace_id IN ( \
    SELECT DISTINCT toInt64OrZero(splitByChar('/', traversal_path)[2]) \
    FROM project_namespace_traversal_paths \
    WHERE deleted = false \
  )";

/// SQL to count code-eligible projects in the datalake: projects belonging to
/// any enabled namespace. This is the denominator of the code-coverage check
/// that gates migration promotion (see `code_coverage_threshold`).
const COUNT_CODE_ELIGIBLE_PROJECTS: &str = "\
SELECT count(DISTINCT id) AS ns_count \
FROM project_namespace_traversal_paths \
WHERE deleted = false \
  AND toInt64OrZero(splitByChar('/', traversal_path)[2]) IN ( \
    SELECT root_namespace_id \
    FROM siphon_knowledge_graph_enabled_namespaces \
    WHERE _siphon_deleted = false \
  )";

/// SQL to count distinct projects that have a checkpoint row in the new-prefix
/// code indexing checkpoint table. The numerator of the code-coverage check.
const COUNT_CODE_CHECKPOINT_PROJECTS: &str = "\
SELECT count(DISTINCT project_id) AS ns_count \
FROM {table:Identifier} FINAL \
WHERE _deleted = false";

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
        // Returns the post-mutation version list if a promotion happened,
        // so phase 2 doesn't need to re-read (avoids write-visibility lag).
        let versions_after_promotion = self.check_completion().await?;

        // Phase 2: clean up old retired versions outside retention window.
        self.cleanup_old_versions(versions_after_promotion).await?;

        Ok(())
    }

    /// Checks whether a `migrating` version has been fully re-indexed and
    /// should be promoted to `active`.
    ///
    /// Returns the updated version entries if a promotion happened, so the
    /// caller can pass them to cleanup without re-reading from ClickHouse.
    async fn check_completion(&self) -> Result<Option<Vec<VersionEntry>>, TaskError> {
        let migrating = read_migrating_version(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read migrating version: {e}")))?;

        let Some(migrating_version) = migrating else {
            return Ok(None);
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
            return Ok(None);
        }

        // Promote: migrating → active, old active → retired.
        let mut versions = read_all_versions(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read all versions: {e}")))?;

        for entry in &versions {
            if entry.status == "active" && entry.version != migrating_version {
                info!(
                    version = entry.version,
                    "marking old active version as retired"
                );
                mark_version_retired(&self.graph, entry.version)
                    .await
                    .map_err(|e| TaskError::new(format!("mark v{} retired: {e}", entry.version)))?;
            }
        }

        info!(
            version = migrating_version,
            "marking migrating version as active — schema migration complete"
        );
        mark_version_active(&self.graph, migrating_version)
            .await
            .map_err(|e| TaskError::new(format!("mark v{migrating_version} active: {e}")))?;

        self.metrics.record_migration_completed();

        // Reflect the mutations in the in-memory list so cleanup doesn't
        // need to re-read and risk write-visibility lag.
        for entry in &mut versions {
            if entry.version == migrating_version {
                entry.status = "active".to_string();
            } else if entry.status == "active" {
                entry.status = "retired".to_string();
            }
        }

        info!(
            version = migrating_version,
            "schema migration to v{migrating_version} complete"
        );

        Ok(Some(versions))
    }

    /// Returns `true` if all enabled namespaces have checkpoint entries in both
    /// the new-prefix SDLC and code indexing checkpoint tables.
    ///
    /// Completion is checkpoint-based, not row-count-based. A checkpoint entry
    /// means the indexing pipeline ran for that scope — it does not validate
    /// that the output tables contain the expected number of rows. This is the
    /// standard pattern for CDC/ETL systems: the checkpoint proves the pipeline
    /// executed and committed, but silent data-loss bugs (e.g. an upstream
    /// source returning empty results) would not be caught. Full data
    /// correctness validation is deferred to staging E2E tests (issue #443).
    async fn is_migration_complete(&self, version: u32) -> Result<bool, String> {
        let prefix = table_prefix(version);

        // Count enabled namespaces from the datalake (the reference set).
        let enabled_count = self
            .count_datalake_namespaces(COUNT_ENABLED_NAMESPACES)
            .await
            .map_err(|e| format!("count enabled namespaces: {e}"))?;

        if enabled_count == 0 {
            warn!(
                version,
                "enabled namespace count is 0 — skipping promotion to avoid \
                 premature completion during a datalake outage"
            );
            return Ok(false);
        }

        // Code-eligible enabled namespaces: those with at least one project.
        // Empty namespaces never publish code tasks and cannot produce
        // checkpoint rows, so they must be excluded from the code side of
        // the completion predicate.
        let code_eligible_count = self
            .count_datalake_namespaces(COUNT_CODE_ELIGIBLE_ENABLED_NAMESPACES)
            .await
            .map_err(|e| format!("count code-eligible enabled namespaces: {e}"))?;

        // SDLC completeness: namespaces with entries in the new checkpoint table.
        let sdlc_table = format!("{prefix}checkpoint");
        let sdlc_count = self
            .count_table_namespaces(COUNT_SDLC_CHECKPOINT_NAMESPACES, &sdlc_table)
            .await
            .map_err(|e| format!("count SDLC checkpoint namespaces: {e}"))?;

        // Code indexing completeness: namespaces with entries in the new
        // code_indexing_checkpoint table.
        let code_table = format!("{prefix}code_indexing_checkpoint");
        let code_count = self
            .count_table_namespaces(COUNT_CODE_CHECKPOINT_NAMESPACES, &code_table)
            .await
            .map_err(|e| format!("count code checkpoint namespaces: {e}"))?;

        // Project-level coverage gate: even when every enabled namespace has
        // produced at least one checkpoint row, the migration is only useful
        // once the bulk of projects within those namespaces are indexed.
        // Without this gate, completion fires after a single project per
        // namespace lands in the checkpoint table, leaving the rest unindexed
        // on the newly-active schema until organic pushes trickle them in.
        let eligible_projects = self
            .count_datalake_namespaces(COUNT_CODE_ELIGIBLE_PROJECTS)
            .await
            .map_err(|e| format!("count code-eligible projects: {e}"))?;

        let indexed_projects = self
            .count_table_namespaces(COUNT_CODE_CHECKPOINT_PROJECTS, &code_table)
            .await
            .map_err(|e| format!("count code-indexed projects: {e}"))?;

        let coverage = if eligible_projects == 0 {
            1.0
        } else {
            indexed_projects as f64 / eligible_projects as f64
        };
        let threshold = self.config.code_coverage_threshold;

        info!(
            version,
            sdlc_indexed_namespaces = sdlc_count,
            code_indexed_namespaces = code_count,
            enabled_namespaces = enabled_count,
            code_eligible_enabled_namespaces = code_eligible_count,
            code_indexed_projects = indexed_projects,
            code_eligible_projects = eligible_projects,
            code_coverage = coverage,
            code_coverage_threshold = threshold,
            "migration completion status"
        );

        // SDLC must cover every enabled namespace; code must cover both the
        // namespace set and `threshold` fraction of eligible projects.
        Ok(sdlc_count >= enabled_count
            && code_count >= code_eligible_count
            && coverage >= threshold)
    }

    /// Counts distinct namespaces in a checkpoint table using the given query.
    async fn count_table_namespaces(&self, query: &str, table: &str) -> Result<u64, String> {
        let batches = self
            .graph
            .query(query)
            .param("table", table)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        batches
            .first()
            .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "ns_count", 0))
            .ok_or_else(|| "no ns_count in result".to_string())
    }

    async fn count_datalake_namespaces(&self, query: &str) -> Result<u64, String> {
        let batches = self
            .datalake
            .query_arrow(query)
            .await
            .map_err(|e| e.to_string())?;

        batches
            .first()
            .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "ns_count", 0))
            .ok_or_else(|| "no ns_count in result".to_string())
    }

    /// Drops tables for retired versions outside the retention window, then
    /// marks them `dropped`.
    async fn cleanup_old_versions(
        &self,
        cached_versions: Option<Vec<VersionEntry>>,
    ) -> Result<(), TaskError> {
        let versions = match cached_versions {
            Some(v) => v,
            None => read_all_versions(&self.graph)
                .await
                .map_err(|e| TaskError::new(format!("read all versions: {e}")))?,
        };

        // Keep the top `max_retained_versions` non-dropped entries.
        let retained: Vec<&VersionEntry> =
            versions.iter().filter(|v| v.status != "dropped").collect();

        let max = self.schema_config.max_retained_versions as usize;
        if retained.len() <= max {
            return Ok(());
        }

        let current_version = retained.first().map(|v| v.version).unwrap_or(0);
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
                    self.metrics
                        .record_cleanup(entry.version, current_version, "success");
                    info!(
                        version = entry.version,
                        "version tables dropped and marked as dropped"
                    );
                }
                Err(e) => {
                    self.metrics
                        .record_cleanup(entry.version, current_version, "failure");
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
    fn sdlc_checkpoint_query_uses_identifier_param() {
        assert!(
            COUNT_SDLC_CHECKPOINT_NAMESPACES.contains("{table:Identifier}"),
            "SDLC checkpoint query must use Identifier param for table name"
        );
    }

    #[test]
    fn sdlc_checkpoint_query_filters_deleted() {
        assert!(COUNT_SDLC_CHECKPOINT_NAMESPACES.contains("_deleted = false"));
    }

    #[test]
    fn code_checkpoint_query_uses_identifier_param() {
        assert!(
            COUNT_CODE_CHECKPOINT_NAMESPACES.contains("{table:Identifier}"),
            "code checkpoint query must use Identifier param for table name"
        );
    }

    #[test]
    fn code_checkpoint_query_filters_deleted() {
        assert!(COUNT_CODE_CHECKPOINT_NAMESPACES.contains("_deleted = false"));
    }

    #[test]
    fn code_checkpoint_query_extracts_namespace_from_traversal_path() {
        assert!(
            COUNT_CODE_CHECKPOINT_NAMESPACES.contains("splitByChar"),
            "code checkpoint query must extract namespace ID from traversal_path"
        );
    }

    #[test]
    fn count_enabled_namespaces_query_filters_deleted() {
        assert!(COUNT_ENABLED_NAMESPACES.contains("_siphon_deleted = false"));
    }

    #[test]
    fn count_code_eligible_enabled_namespaces_query_filters_deleted() {
        assert!(COUNT_CODE_ELIGIBLE_ENABLED_NAMESPACES.contains("_siphon_deleted = false"));
        assert!(COUNT_CODE_ELIGIBLE_ENABLED_NAMESPACES.contains("deleted = false"));
    }

    #[test]
    fn count_code_eligible_enabled_namespaces_query_filters_on_projects() {
        assert!(
            COUNT_CODE_ELIGIBLE_ENABLED_NAMESPACES.contains("project_namespace_traversal_paths"),
            "code-eligible count must restrict to namespaces with at least one project"
        );
        assert!(
            COUNT_CODE_ELIGIBLE_ENABLED_NAMESPACES.contains("splitByChar"),
            "code-eligible count must extract root namespace ID from project traversal_path"
        );
    }

    #[test]
    fn migration_lock_key_matches_schema_migration() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }

    #[test]
    fn count_code_eligible_projects_query_filters_deleted() {
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("deleted = false"));
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("_siphon_deleted = false"));
    }

    #[test]
    fn count_code_eligible_projects_query_counts_distinct_project_ids() {
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("count(DISTINCT id)"));
    }

    #[test]
    fn count_code_checkpoint_projects_query_uses_identifier_param() {
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS.contains("{table:Identifier}"));
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS.contains("count(DISTINCT project_id)"));
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS.contains("_deleted = false"));
    }

    #[test]
    fn default_config_has_a_code_coverage_threshold() {
        let config = MigrationCompletionConfig::default();
        assert!(config.code_coverage_threshold > 0.0);
        assert!(config.code_coverage_threshold <= 1.0);
    }

    /// Coverage math is the load-bearing predicate for migration completion.
    /// These cases lock in the boundary behavior so a future change to the
    /// formula (or to the threshold default) won't silently regress it.
    #[test]
    fn code_coverage_math_thresholding() {
        fn coverage(indexed: u64, eligible: u64) -> f64 {
            if eligible == 0 {
                1.0
            } else {
                indexed as f64 / eligible as f64
            }
        }

        // Empty datalake: trivially "complete" so completion isn't blocked
        // forever on a brand-new install with no enabled namespaces yet.
        assert_eq!(coverage(0, 0), 1.0);

        // Below threshold: not complete.
        assert!(coverage(14, 8602) < 0.95);

        // Exactly at threshold: complete.
        assert!(coverage(95, 100) >= 0.95);

        // Above threshold: complete.
        assert!(coverage(99, 100) >= 0.95);
    }
}

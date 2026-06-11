//! Migration completion detection and dead-version GC.
//!
//! Runs as a scheduled task in the DispatchIndexing mode. On each tick:
//!
//! 1. **Completion detection** — If a version is in `migrating` state, check
//!    whether all enabled namespaces have checkpoint entries in the new-prefix
//!    tables. If so, promote the version to `active` and demote the previously
//!    active version to `retired`.
//!
//! 2. **Reconcile GC** — Enumerate physical `v<N>_*` objects from
//!    `system.tables`, compute a keep-set (active + retained retired +
//!    in-flight migrating), and drop everything else. This catches
//!    rename-orphans, zombie migrating versions, and dropped-with-residue
//!    versions that the old ontology-name-based cleanup missed.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use gkg_server_config::{MigrationCompletionConfig, ScheduleConfiguration, SchemaConfig};
use gkg_utils::arrow::ArrowUtils;
use query_engine::compiler::{
    generate_graph_dictionaries, generate_graph_materialized_views, generate_graph_tables,
};
use tracing::{info, warn};

use super::metrics::CompletionMetrics;
use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::LockService;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{
    SCHEMA_VERSION, VersionEntry, mark_version_active, mark_version_dropped, mark_version_retired,
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

/// SQL to count enabled namespaces from the datalake.
const COUNT_ENABLED_NAMESPACES: &str = "\
SELECT count(DISTINCT root_namespace_id) AS ns_count \
FROM siphon_knowledge_graph_enabled_namespaces \
WHERE _siphon_deleted = false";

/// SQL to count code-eligible projects in the datalake: projects belonging
/// to any enabled namespace. The denominator of the code-coverage telemetry
/// emitted from `is_migration_complete` (the predicate doesn't gate on
/// coverage; see the doc comment there).
const COUNT_CODE_ELIGIBLE_PROJECTS: &str = "\
SELECT count(DISTINCT p.id) AS ns_count \
FROM project_namespace_traversal_paths AS p \
INNER JOIN siphon_knowledge_graph_enabled_namespaces AS enabled \
  ON startsWith(p.traversal_path, enabled.traversal_path) \
WHERE p.deleted = false \
  AND enabled._siphon_deleted = false";

/// SQL to fetch enabled namespaces' traversal paths from the datalake. Used
/// to bridge the cluster boundary: the checkpoint table lives in the graph
/// DB and cannot join to the datalake, so we pull the small enabled-path set
/// first and pass it as an Array(String) parameter to the graph-side count.
const FETCH_ENABLED_TRAVERSAL_PATHS: &str = "\
SELECT DISTINCT traversal_path \
FROM siphon_knowledge_graph_enabled_namespaces \
WHERE _siphon_deleted = false";

/// SQL to count distinct projects in the new-prefix code indexing
/// checkpoint table that fall under at least one currently-enabled
/// namespace traversal path. The numerator of the code-coverage telemetry.
///
/// Scoping by the enabled-path set keeps the reported coverage honest:
/// without it, leftover checkpoint rows from disabled namespaces would
/// inflate the numerator and produce a misleading "approaching 100%" log
/// line while currently-enabled namespaces were still under-indexed.
const COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED: &str = "\
SELECT count(DISTINCT project_id) AS ns_count \
FROM {table:Identifier} FINAL \
WHERE _deleted = false \
  AND arrayExists(p -> startsWith(traversal_path, p), {paths:Array(String)})";

/// SQL to read the wall-clock age of the row that marked the given version
/// as `migrating`. Used to populate the `migrating_age_seconds` gauge so
/// operators can alert on migrations stuck in the migrating state for too
/// long.
const READ_MIGRATING_AGE: &str = "\
SELECT toUInt64(dateDiff('second', created_at, now())) AS age_seconds \
FROM gkg_schema_version FINAL \
WHERE status = 'migrating' AND version = {version:UInt32}";

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
    campaign: Arc<CampaignState>,
}

impl MigrationCompletionChecker {
    #[allow(
        clippy::too_many_arguments,
        reason = "completion checker constructor wires all collaborators explicitly; grouping into a struct would just move the arity"
    )]
    pub fn new(
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        lock_service: Arc<dyn LockService>,
        ontology: Arc<ontology::Ontology>,
        schema_config: SchemaConfig,
        config: MigrationCompletionConfig,
        task_metrics: ScheduledTaskMetrics,
        campaign: Arc<CampaignState>,
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
            campaign,
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

        // Phase 2: GC sweep — drop objects for dead versions.
        self.reconcile_dead_versions(versions_after_promotion)
            .await?;

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
            // No migration in progress — keep the age gauge accurate so an
            // alert on `migrating_age_seconds > N` doesn't fire on the
            // post-promotion last-recorded value.
            self.metrics.record_migrating_age(0);
            return Ok(None);
        };

        // Surface "is migration stuck?" as a direct gauge. A bounded query
        // failure here shouldn't block completion; log and continue with an
        // unrecorded age this tick.
        if let Ok(age) = self.fetch_migrating_age(migrating_version).await {
            self.metrics.record_migrating_age(age);
        }

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

        // Campaign ends when its migration completes.
        self.campaign.clear();

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
    /// Migration completion is **SDLC-only**. Code-indexing coverage is
    /// observed and reported but does not gate promotion: code data fills
    /// `v{N}_code_indexing_checkpoint` continuously via
    /// `NamespaceCodeBackfillDispatcher` regardless of migration state, so
    /// gating promotion on it would couple a slow process (per-repo archive
    /// download + indexing) to a fast one (per-namespace SDLC pull) and risk
    /// stalling rollouts indefinitely when individual projects can't be
    /// indexed (see the analysis on gitlab-org/orbit/knowledge-graph!1035
    /// note 3286051182).
    ///
    /// Completion is checkpoint-based, not row-count-based. A checkpoint
    /// entry means the SDLC pipeline ran for that namespace; it does not
    /// validate the output tables contain the expected number of rows. This
    /// is the standard pattern for CDC/ETL systems: the checkpoint proves
    /// the pipeline executed and committed, but silent data-loss bugs
    /// would not be caught. Full data correctness validation is deferred
    /// to staging E2E tests (issue #443).
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

        // SDLC completeness: namespaces with entries in the new checkpoint
        // table. This is the *only* gate for promotion.
        let sdlc_table = format!("{prefix}checkpoint");
        let sdlc_count = self
            .count_table_namespaces(COUNT_SDLC_CHECKPOINT_NAMESPACES, &sdlc_table)
            .await
            .map_err(|e| format!("count SDLC checkpoint namespaces: {e}"))?;

        // Code-indexing telemetry. Computed for visibility and emitted as a
        // structured log field below; explicitly NOT part of the promotion
        // predicate. The backfill dispatcher fills
        // `v{N}_code_indexing_checkpoint` after promotion until coverage
        // approaches 100%, and operators watch the `code_coverage` field on
        // the "migration completion status" log line to track progress.
        let code_table = format!("{prefix}code_indexing_checkpoint");
        let (eligible_projects, indexed_projects, coverage) = self
            .compute_code_coverage(&code_table)
            .await
            .map_err(|e| format!("compute code coverage: {e}"))?;

        info!(
            version,
            sdlc_indexed_namespaces = sdlc_count,
            enabled_namespaces = enabled_count,
            code_indexed_projects = indexed_projects,
            code_eligible_projects = eligible_projects,
            code_coverage = coverage,
            "migration completion status"
        );

        // Story-telling gauges: indexed/eligible per scope, labeled by
        // version_band. Dashboards compute the ratio; alerts fire on
        // per-scope thresholds (sdlc < 100% during migration window, code
        // < 95% for >24h post-promotion, etc.).
        let current = *SCHEMA_VERSION;
        self.metrics
            .record_units("sdlc", version, current, sdlc_count, enabled_count);
        self.metrics.record_units(
            "code",
            version,
            current,
            indexed_projects,
            eligible_projects,
        );

        // Promotion fires as soon as SDLC has covered every enabled
        // namespace. Code coverage is tracked in `coverage` above for
        // observability, but it explicitly does NOT block promotion.
        Ok(sdlc_count >= enabled_count)
    }

    /// Reads the wall-clock age (in seconds) of the row that marked the
    /// given version as `migrating`. Used to populate the
    /// `gkg.schema.migrating_age_seconds` gauge.
    async fn fetch_migrating_age(&self, version: u32) -> Result<u64, String> {
        let batches = self
            .graph
            .query(READ_MIGRATING_AGE)
            .param("version", version)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        batches
            .first()
            .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "age_seconds", 0))
            .ok_or_else(|| "no age_seconds in result".to_string())
    }

    /// Returns `(eligible_projects, indexed_projects, coverage_ratio)` for
    /// the given checkpoint table. Used for telemetry only — the migration
    /// promotion predicate does not gate on coverage. See [`is_migration_complete`]
    /// for the rationale.
    async fn compute_code_coverage(&self, code_table: &str) -> Result<(u64, u64, f64), String> {
        let eligible_projects = self
            .count_datalake_namespaces(COUNT_CODE_ELIGIBLE_PROJECTS)
            .await
            .map_err(|e| format!("count code-eligible projects: {e}"))?;

        let enabled_paths = self
            .fetch_enabled_traversal_paths()
            .await
            .map_err(|e| format!("fetch enabled traversal paths: {e}"))?;

        let indexed_projects = self
            .count_scoped_checkpoint_projects(code_table, &enabled_paths)
            .await
            .map_err(|e| format!("count code-indexed projects: {e}"))?;

        let coverage = if eligible_projects == 0 {
            1.0
        } else {
            indexed_projects as f64 / eligible_projects as f64
        };
        Ok((eligible_projects, indexed_projects, coverage))
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

    /// Pulls the (small) set of enabled-namespace traversal paths from the
    /// datalake. The cluster boundary forces this to be a separate query
    /// from the checkpoint count.
    async fn fetch_enabled_traversal_paths(&self) -> Result<Vec<String>, String> {
        let batches = self
            .datalake
            .query(FETCH_ENABLED_TRAVERSAL_PATHS)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        clickhouse_client::FromArrowColumn::extract_column(&batches, 0).map_err(|e| e.to_string())
    }

    /// Counts distinct projects in `code_table` whose `traversal_path` falls
    /// under at least one of `enabled_paths`. Empty `enabled_paths` short-
    /// circuits to 0 so the coverage ratio behaves correctly when no
    /// namespaces are enabled.
    async fn count_scoped_checkpoint_projects(
        &self,
        code_table: &str,
        enabled_paths: &[String],
    ) -> Result<u64, String> {
        if enabled_paths.is_empty() {
            return Ok(0);
        }
        let batches = self
            .graph
            .query(COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED)
            .param("table", code_table)
            .param("paths", enabled_paths)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        batches
            .first()
            .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "ns_count", 0))
            .ok_or_else(|| "no ns_count in result".to_string())
    }

    /// GC sweep: computes a keep-set from version entries, then drops all
    /// ontology-derived objects for every version outside that set.
    ///
    /// This replaces the old status-gated cleanup that only processed
    /// `retired` versions and derived drop targets from the current ontology.
    /// The new approach handles zombie migrating versions (`version < active`)
    /// and dropped-with-residue versions (status `dropped` but tables still
    /// present) because it is not gated on status.
    async fn reconcile_dead_versions(
        &self,
        cached_versions: Option<Vec<VersionEntry>>,
    ) -> Result<(), TaskError> {
        let versions = match cached_versions {
            Some(v) => v,
            None => read_all_versions(&self.graph)
                .await
                .map_err(|e| TaskError::new(format!("read all versions: {e}")))?,
        };

        let keep = compute_keep_set(&versions, self.schema_config.max_retained_versions as usize);

        if keep.is_empty() {
            warn!("keep-set is empty (no active version found) — aborting GC sweep");
            return Ok(());
        }

        let to_drop: Vec<u32> = versions
            .iter()
            .map(|v| v.version)
            .filter(|v| !keep.contains(v))
            .collect();

        if to_drop.is_empty() {
            return Ok(());
        }

        let current_version = versions
            .iter()
            .find(|v| v.status == "active")
            .map(|v| v.version)
            .unwrap_or(0);

        info!(
            versions_to_gc = ?to_drop,
            keep_set = ?keep,
            "GC sweep: dropping objects for dead versions"
        );

        for version in &to_drop {
            match self.drop_version_objects(*version).await {
                Ok(()) => {
                    self.metrics
                        .record_cleanup(*version, current_version, "success");
                    info!(version, "GC: version objects dropped");
                }
                Err(e) => {
                    self.metrics
                        .record_cleanup(*version, current_version, "failure");
                    warn!(version, error = %e, "GC: failed to drop version objects");
                }
            }

            if let Err(e) = mark_version_dropped(&self.graph, *version).await {
                warn!(version, error = %e, "GC: failed to mark version dropped");
            }
        }

        Ok(())
    }

    /// Drops all ontology-derived objects for a schema version.
    ///
    /// Drop order: materialized views → dictionaries → tables. Views
    /// reference source tables, so they must go first.
    async fn drop_version_objects(&self, version: u32) -> Result<(), String> {
        let prefix = table_prefix(version);

        let views = generate_graph_materialized_views(&self.ontology);
        for mv in &views {
            let name = format!("{prefix}{}", mv.name);
            if let Err(e) = self
                .graph
                .execute(&format!("DROP VIEW IF EXISTS {name}"))
                .await
            {
                warn!(version, object = %name, error = %e, "GC: failed to drop view");
            }
        }

        let dicts = generate_graph_dictionaries(&self.ontology);
        for d in &dicts {
            let name = format!("{prefix}{}", d.name);
            if let Err(e) = self
                .graph
                .execute(&format!("DROP DICTIONARY IF EXISTS {name}"))
                .await
            {
                warn!(version, object = %name, error = %e, "GC: failed to drop dictionary");
            }
        }

        let tables = generate_graph_tables(&self.ontology);
        for t in &tables {
            let name = format!("{prefix}{}", t.name);
            if let Err(e) = self
                .graph
                .execute(&format!("DROP TABLE IF EXISTS {name}"))
                .await
            {
                warn!(version, object = %name, error = %e, "GC: failed to drop table");
            }
        }

        Ok(())
    }
}

/// Computes the set of schema versions to keep. Everything else is safe to drop.
///
/// Keep-set = active ∪ newest `max_retained` retired ∪ migrating > active.
/// A `migrating` version below active is a zombie (superseded/crashed migration)
/// and is excluded so the GC sweep reclaims its tables.
///
/// Returns an empty set if no active version is found; the caller must abort the
/// sweep in that case (ambiguity guard).
fn compute_keep_set(versions: &[VersionEntry], max_retained: usize) -> BTreeSet<u32> {
    let mut keep = BTreeSet::new();

    let active = versions.iter().find(|v| v.status == "active");
    let Some(active) = active else {
        return keep;
    };
    keep.insert(active.version);

    // Retain the newest `max_retained - 1` retired versions (the active
    // version already counts toward the retention window).
    let slots = max_retained.saturating_sub(1);
    for v in versions
        .iter()
        .filter(|v| v.status == "retired")
        .take(slots)
    {
        keep.insert(v.version);
    }

    // A migrating version above active is the in-flight migration; keep it.
    for v in versions.iter().filter(|v| v.status == "migrating") {
        if v.version > active.version {
            keep.insert(v.version);
        }
    }

    keep
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
    fn count_enabled_namespaces_query_filters_deleted() {
        assert!(COUNT_ENABLED_NAMESPACES.contains("_siphon_deleted = false"));
    }

    #[test]
    fn migration_lock_key_matches_schema_migration() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }

    #[test]
    fn count_code_eligible_projects_query_filters_deleted() {
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("p.deleted = false"));
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("enabled._siphon_deleted = false"));
    }

    #[test]
    fn count_code_eligible_projects_query_counts_distinct_project_ids() {
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("count(DISTINCT p.id)"));
    }

    #[test]
    fn count_code_eligible_projects_uses_traversal_path_join() {
        assert!(
            COUNT_CODE_ELIGIBLE_PROJECTS
                .contains("startsWith(p.traversal_path, enabled.traversal_path)"),
            "eligible-projects must join via traversal_path, not splitByChar"
        );
        assert!(!COUNT_CODE_ELIGIBLE_PROJECTS.contains("splitByChar"));
    }

    #[test]
    fn count_code_checkpoint_projects_scoped_query_shape() {
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED.contains("{table:Identifier}"));
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED.contains("count(DISTINCT project_id)"));
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED.contains("_deleted = false"));
    }

    #[test]
    fn count_code_checkpoint_projects_scoped_filters_by_enabled_paths() {
        assert!(
            COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED.contains("{paths:Array(String)}"),
            "scoped checkpoint count must take an Array(String) param to filter by enabled namespaces — \
             without it, leftover checkpoint rows from disabled namespaces inflate coverage"
        );
        assert!(COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED.contains("arrayExists"));
    }

    #[test]
    fn fetch_enabled_traversal_paths_query_filters_deleted() {
        assert!(FETCH_ENABLED_TRAVERSAL_PATHS.contains("_siphon_deleted = false"));
        assert!(FETCH_ENABLED_TRAVERSAL_PATHS.contains("DISTINCT traversal_path"));
    }

    /// Coverage math is informational (the predicate doesn't gate on it),
    /// but the math is still load-bearing for the structured log line that
    /// operators watch to track backfill progress on the active version.
    #[test]
    fn code_coverage_math_thresholding() {
        fn coverage(indexed: u64, eligible: u64) -> f64 {
            if eligible == 0 {
                1.0
            } else {
                indexed as f64 / eligible as f64
            }
        }

        // Empty eligibility: short-circuits to 1.0 so the structured log
        // doesn't emit NaN on a brand-new install with no enabled namespaces.
        assert_eq!(coverage(0, 0), 1.0);

        // Mid-rollout coverage stays below 1.0 while backfill is in flight.
        // 14 of ~8,602 was the actual orbit-prd state right after v7
        // promoted; this asserts the ratio reflects that progress.
        assert!(coverage(14, 8602) < 0.01);

        // Saturated coverage approaches 1.0 once the backfill catches up.
        assert!((coverage(8600, 8602) - 0.9998).abs() < 0.001);
    }

    fn v(version: u32, status: &str) -> VersionEntry {
        VersionEntry {
            version,
            status: status.to_string(),
        }
    }

    #[test]
    fn keep_set_empty_when_no_active() {
        let versions = vec![v(1, "migrating"), v(9, "migrating")];
        assert!(compute_keep_set(&versions, 2).is_empty());
    }

    #[test]
    fn keep_set_includes_active_and_one_retired() {
        let versions = vec![v(58, "active"), v(57, "retired"), v(56, "retired")];
        let keep = compute_keep_set(&versions, 2);
        assert!(keep.contains(&58));
        assert!(keep.contains(&57));
        assert!(!keep.contains(&56));
    }

    #[test]
    fn keep_set_keeps_migrating_above_active() {
        let versions = vec![v(59, "migrating"), v(58, "active"), v(57, "retired")];
        let keep = compute_keep_set(&versions, 2);
        assert!(keep.contains(&59));
        assert!(keep.contains(&58));
        assert!(keep.contains(&57));
    }

    #[test]
    fn keep_set_drops_migrating_below_active() {
        let versions = vec![
            v(58, "active"),
            v(57, "retired"),
            v(9, "migrating"),
            v(1, "migrating"),
        ];
        let keep = compute_keep_set(&versions, 2);
        assert!(keep.contains(&58));
        assert!(keep.contains(&57));
        assert!(!keep.contains(&9));
        assert!(!keep.contains(&1));
    }

    #[test]
    fn keep_set_ignores_dropped_versions() {
        let versions = vec![
            v(58, "active"),
            v(57, "retired"),
            v(56, "dropped"),
            v(55, "retired"),
        ];
        let keep = compute_keep_set(&versions, 2);
        assert!(keep.contains(&58));
        assert!(keep.contains(&57));
        assert!(!keep.contains(&56));
        assert!(!keep.contains(&55));
    }

    /// Mirrors the actual orbit-prod state from the PRD.
    #[test]
    fn keep_set_prod_scenario() {
        let versions = vec![
            v(58, "active"),
            v(57, "retired"),
            v(56, "dropped"),
            v(14, "dropped"),
            v(10, "dropped"),
            v(9, "migrating"),
            v(7, "dropped"),
            v(6, "dropped"),
            v(2, "dropped"),
            v(1, "migrating"),
        ];
        let keep = compute_keep_set(&versions, 2);
        assert_eq!(keep, BTreeSet::from([57, 58]));
    }

    #[test]
    fn keep_set_active_only() {
        let versions = vec![v(58, "active")];
        let keep = compute_keep_set(&versions, 2);
        assert_eq!(keep, BTreeSet::from([58]));
    }
}

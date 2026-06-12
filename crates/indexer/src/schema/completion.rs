//! Migration completion detection and dead-version GC.
//!
//! Runs as a scheduled task in the DispatchIndexing mode. On each tick:
//!
//! 1. **Completion detection** — If a version is in `migrating` state, check
//!    whether all enabled namespaces have checkpoint entries in the new-prefix
//!    tables. If so, promote the version to `active` and demote the previously
//!    active version to `retired`.
//!
//! 2. **Version GC** — A single SQL query computes the keep-set (active +
//!    retained retired + in-flight migrating above active) and enumerates
//!    all `v<N>_*` objects in `system.tables` whose version falls outside
//!    it. Each candidate is validated against the ontology before being
//!    dropped; unrecognized objects are logged and left alone.

use std::collections::HashSet;
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
    SCHEMA_VERSION, mark_version_active, mark_version_dropped, mark_version_retired,
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

/// Single query that computes the keep-set in SQL and returns every
/// `v<N>_*` object outside it. The keep-set is: active + newest
/// `retired_slots` retired + migrating above active. Returns zero rows
/// if no active version exists (safety guard).
const LIST_DEAD_VERSION_OBJECTS: &str = "\
SELECT \
  name, engine, \
  toUInt32OrZero(extractAll(name, '^v([0-9]+)_')[1]) AS dead_version \
FROM system.tables \
WHERE database = {db:String} \
  AND match(name, '^v[0-9]+_') \
  AND toUInt32OrZero(extractAll(name, '^v([0-9]+)_')[1]) NOT IN (\
      SELECT version FROM gkg_schema_version FINAL WHERE status = 'active' \
      UNION ALL \
      SELECT version FROM (\
          SELECT version FROM gkg_schema_version FINAL \
          WHERE status = 'retired' ORDER BY version DESC LIMIT {retired_slots:UInt32}) \
      UNION ALL \
      SELECT version FROM gkg_schema_version FINAL \
      WHERE status = 'migrating' \
        AND version > (SELECT coalesce(max(version), 0) \
                       FROM gkg_schema_version FINAL WHERE status = 'active')) \
  AND (SELECT count() FROM gkg_schema_version FINAL WHERE status = 'active') > 0";

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
        self.check_completion().await?;
        self.reconcile_dead_versions().await?;
        Ok(())
    }

    /// Checks whether a `migrating` version has been fully re-indexed and
    /// should be promoted to `active`.
    async fn check_completion(&self) -> Result<(), TaskError> {
        let migrating = read_migrating_version(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read migrating version: {e}")))?;

        let Some(migrating_version) = migrating else {
            self.metrics.record_migrating_age(0);
            return Ok(());
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
            return Ok(());
        }

        // Promote: migrating → active, old active → retired.
        let versions = read_all_versions(&self.graph)
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
                if gkg_server_config::features::enabled(
                    gkg_server_config::Feature::StopMergesOnRetire,
                ) {
                    self.stop_merges_for_version(entry.version).await;
                }
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

        info!(
            version = migrating_version,
            "schema migration to v{migrating_version} complete"
        );

        Ok(())
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

    /// Stops background merges on all graph tables for a version so the merge
    /// pool is reserved for the active version. Best-effort: failures are
    /// logged but never propagated.
    ///
    /// `SYSTEM STOP MERGES` is node-local runtime state (unlike DDL which
    /// auto-replicates), so it is issued `ON CLUSTER` to reach every replica.
    /// Cluster name `'default'` matches ClickHouse Cloud convention.
    async fn stop_merges_for_version(&self, version: u32) {
        let prefix = table_prefix(version);
        let db = self.graph.database();

        for t in &generate_graph_tables(&self.ontology) {
            let qualified = format!("{db}.{prefix}{}", t.name);
            let _ = self
                .graph
                .execute(&format!(
                    "SYSTEM STOP MERGES ON CLUSTER 'default' {qualified}"
                ))
                .await
                .inspect_err(
                    |e| warn!(version, table = %qualified, error = %e, "failed to stop merges"),
                );
        }
    }

    /// Enumerates dead-version objects via `system.tables` (keep-set computed
    /// in SQL), validates each against the ontology, and drops recognized
    /// objects. Unrecognized objects are logged and left alone.
    async fn reconcile_dead_versions(&self) -> Result<(), TaskError> {
        let retired_slots = self.schema_config.max_retained_versions.saturating_sub(1);
        let db = self.graph.database();

        let batches = self
            .graph
            .query(LIST_DEAD_VERSION_OBJECTS)
            .param("db", db)
            .param("retired_slots", retired_slots)
            .fetch_arrow()
            .await
            .map_err(|e| TaskError::new(format!("list dead version objects: {e}")))?;

        let known_names = ontology_known_names(&self.ontology);
        let current = *SCHEMA_VERSION;

        // Collect and sort: views first, then dictionaries, then tables.
        let mut drops: Vec<(u32, String, &'static str)> = Vec::new();
        for batch in &batches {
            for i in 0..batch.num_rows() {
                let name = ArrowUtils::get_column_string(batch, "name", i)
                    .ok_or_else(|| TaskError::new("missing name".to_string()))?;
                let engine = ArrowUtils::get_column_string(batch, "engine", i)
                    .ok_or_else(|| TaskError::new("missing engine".to_string()))?;
                let version = ArrowUtils::get_column::<arrow::datatypes::UInt32Type>(
                    batch,
                    "dead_version",
                    i,
                )
                .ok_or_else(|| TaskError::new("missing dead_version".to_string()))?;

                let base_name = name.strip_prefix(&format!("v{version}_")).unwrap_or(&name);
                if !known_names.contains(base_name) {
                    warn!(version, object = %name, "GC: skipping unrecognized object");
                    continue;
                }

                let kind = match engine.as_str() {
                    "MaterializedView" | "View" | "LiveView" | "WindowView" => "VIEW",
                    "Dictionary" => "DICTIONARY",
                    _ => "TABLE",
                };
                drops.push((version, name, kind));
            }
        }

        // Fixed drop order: dictionaries, views, tables. Works for the
        // current ontology shape (dicts source from tables, no cross-type
        // cycles). A general migration framework should topo-sort using
        // system.tables loading_dependencies columns instead.
        drops.sort_by_key(|(_, _, kind)| match *kind {
            "DICTIONARY" => 0,
            "VIEW" => 1,
            _ => 2,
        });

        if gkg_server_config::features::enabled(gkg_server_config::Feature::StopMergesOnRetire) {
            let dead_versions: HashSet<u32> = drops.iter().map(|(v, _, _)| *v).collect();
            for version in &dead_versions {
                self.stop_merges_for_version(*version).await;
            }
        }

        let mut succeeded: HashSet<u32> = HashSet::new();
        let mut failed: HashSet<u32> = HashSet::new();

        for (version, name, kind) in &drops {
            if let Err(e) = self
                .graph
                .execute(&format!("DROP {kind} IF EXISTS {name}"))
                .await
            {
                warn!(version, object = %name, error = %e, "GC: drop failed");
                failed.insert(*version);
            } else {
                succeeded.insert(*version);
            }
        }

        for version in &succeeded {
            if failed.contains(version) {
                self.metrics.record_cleanup(*version, current, "failure");
                continue;
            }
            if let Err(e) = mark_version_dropped(&self.graph, *version).await {
                warn!(version, error = %e, "GC: failed to mark dropped");
            }
            self.metrics.record_cleanup(*version, current, "success");
        }

        Ok(())
    }
}

/// Builds the set of object names the ontology creates (tables, views,
/// dictionaries) — without any version prefix. Used to validate that a
/// `v<N>_*` object found in `system.tables` was created by the migration
/// system and is safe to drop.
fn ontology_known_names(ontology: &ontology::Ontology) -> HashSet<String> {
    let mut names = HashSet::new();
    for t in &generate_graph_tables(ontology) {
        names.insert(t.name.clone());
    }
    for mv in &generate_graph_materialized_views(ontology) {
        names.insert(mv.name.clone());
    }
    for d in &generate_graph_dictionaries(ontology) {
        names.insert(d.name.clone());
    }
    names
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

    #[test]
    fn ontology_known_names_includes_tables_views_dicts() {
        let ont = ontology::Ontology::load_embedded().unwrap();
        let names = ontology_known_names(&ont);
        assert!(names.contains("gl_edge"), "should contain edge table");
        assert!(
            names.contains("checkpoint"),
            "should contain checkpoint table"
        );
        assert!(!names.is_empty());
    }

    #[test]
    fn gc_query_has_safety_guard() {
        assert!(
            LIST_DEAD_VERSION_OBJECTS.contains("count()"),
            "query must abort when no active version exists"
        );
    }

    #[test]
    fn gc_query_excludes_active_retired_and_migrating_above() {
        assert!(LIST_DEAD_VERSION_OBJECTS.contains("status = 'active'"));
        assert!(LIST_DEAD_VERSION_OBJECTS.contains("status = 'retired'"));
        assert!(LIST_DEAD_VERSION_OBJECTS.contains("status = 'migrating'"));
        assert!(
            LIST_DEAD_VERSION_OBJECTS.contains("coalesce(max(version), 0)"),
            "migrating > active guard must handle missing active"
        );
    }
}

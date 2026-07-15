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
//!    retained retired + every migrating version) and enumerates all
//!    `v<N>_*` objects in `system.tables` whose version falls outside it.
//!    Ontology-known objects are always dropped. Objects not in the
//!    ontology (rename-orphans, removed entities) are also dropped unless
//!    their base name matches a `gc_preserve_patterns` regex.

use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use clickhouse_client::FromArrowColumn;
use gkg_server_config::{MigrationCompletionConfig, ScheduleConfiguration, SchemaConfig};
use gkg_utils::arrow::ArrowUtils;
use gkg_utils::traversal_path::TopLevelSplit;
use ontology::migrations::{MigrationLedger, MigrationScope};
use query_engine::compiler::{
    generate_graph_dictionaries, generate_graph_materialized_views, generate_graph_tables,
};
use tracing::{info, warn};

use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::LockService;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::invalidation::{
    CODE_INDEXING_CHECKPOINT_TABLE, find_invalidated_pipelines,
    get_migration_scope_for_table_writers,
};
use crate::schema::metrics::CompletionMetrics;
use crate::schema::migration::CHECKPOINT_TABLE;
use crate::schema::version::{
    SCHEMA_VERSION, drop_kind_for_engine, mark_version_active, mark_version_dropped,
    mark_version_retired, read_active_version, read_all_versions, read_migrating_version,
    table_prefix,
};

/// ClickHouse Cloud cluster name. Used for `ON CLUSTER` in commands that
/// need cross-replica propagation (e.g. `SYSTEM STOP MERGES`).
const CLICKHOUSE_CLUSTER: &str = "default";

/// NATS KV key used to serialize migration-completion checks across pods.
const MIGRATION_LOCK_KEY: &str = "schema_migration";

/// NATS KV lock TTL for the completion check.
const LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(120);

/// Enabled namespaces (id + traversal path) from the datalake.
static FETCH_ENABLED_NAMESPACES: LazyLock<String> = LazyLock::new(|| {
    let del = ontology::siphon_deleted_column();
    format!(
        "SELECT DISTINCT root_namespace_id, traversal_path \
         FROM siphon_knowledge_graph_enabled_namespaces \
         WHERE {del} = false"
    )
});

/// Count distinct projects whose top-level namespace is enabled.
static COUNT_CODE_ELIGIBLE_PROJECTS: LazyLock<String> = LazyLock::new(|| {
    let del = ontology::siphon_deleted_column();
    let top = gkg_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
    format!(
        "SELECT count(DISTINCT p.id) AS ns_count \
         FROM project_namespace_traversal_paths AS p \
         WHERE p.deleted = false \
           AND extract(p.traversal_path, '{top}') IN (\
               SELECT traversal_path FROM siphon_knowledge_graph_enabled_namespaces \
               WHERE {del} = false AND match(traversal_path, '{top}$'))"
    )
});

/// SQL to count distinct projects in the new-prefix code indexing
/// checkpoint table that fall under at least one currently-enabled
/// namespace traversal path. The numerator of the code-coverage telemetry.
///
/// Scoping by the enabled-path set keeps the reported coverage honest:
/// without it, leftover checkpoint rows from disabled namespaces would
/// inflate the numerator and produce a misleading "approaching 100%" log
/// line while currently-enabled namespaces were still under-indexed.
static COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED: LazyLock<String> = LazyLock::new(|| {
    let top = gkg_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
    format!(
        "SELECT count(DISTINCT project_id) AS ns_count \
         FROM {{table:Identifier}} FINAL \
         WHERE _deleted = false \
           AND extract(traversal_path, '{top}') IN {{paths:Array(String)}}"
    )
});

/// Returns every `v<N>_*` object outside the keep-set (active + newest `retired_slots`
/// retired + every migrating version — a rebuild-rollback migrates *below* active), and
/// zero rows if no active version exists (safety guard).
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
      SELECT version FROM gkg_schema_version FINAL WHERE status = 'migrating') \
  AND (SELECT count() FROM gkg_schema_version FINAL WHERE status = 'active') > 0";

/// SQL to read the wall-clock age of the row that marked the given version
/// as `migrating`. Used to populate the `migrating_age_seconds` gauge so
/// operators can alert on migrations stuck in the migrating state for too
/// long.
const READ_MIGRATING_AGE: &str = "\
SELECT toUInt64(dateDiff('second', created_at, now())) AS age_seconds \
FROM gkg_schema_version FINAL \
WHERE status = 'migrating' AND version = {version:UInt32}";

/// Returns namespace IDs whose checkpoint marks every given plan complete (null cursor).
const GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS: &str = "\
SELECT toInt64(splitByChar('.', key)[2]) AS namespace_id \
FROM {table:Identifier} FINAL \
WHERE _deleted = false \
  AND cursor_values IN ('null', '') \
  AND length(splitByChar('.', key)) = 3 \
  AND splitByChar('.', key)[1] = 'ns' \
  AND match(splitByChar('.', key)[2], '^[0-9]+$') \
  AND splitByChar('.', key)[3] IN {plans:Array(String)} \
GROUP BY namespace_id \
HAVING uniqExact(splitByChar('.', key)[3]) = {plan_count:UInt64}";

/// Counts how many of the given global plans have a completed checkpoint (null cursor).
const COUNT_COMPLETE_GLOBAL_PLANS: &str = "\
SELECT count(DISTINCT splitByChar('.', key)[2]) AS plan_count \
FROM {table:Identifier} FINAL \
WHERE _deleted = false \
  AND cursor_values IN ('null', '') \
  AND length(splitByChar('.', key)) = 2 \
  AND splitByChar('.', key)[1] = 'global' \
  AND splitByChar('.', key)[2] IN {plans:Array(String)}";

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
    nats_client: async_nats::Client,
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
        nats_client: async_nats::Client,
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
            nats_client,
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

#[derive(Debug)]
pub struct SdlcReindexProgress {
    pub completed_namespaces: u64,
    pub ready: bool,
}

pub async fn get_sdlc_reindex_progress_for_enabled_namespaces(
    graph: &ArrowClickHouseClient,
    ontology: &ontology::Ontology,
    scope: &MigrationScope,
    checkpoint_table: &str,
    enabled_namespace_ids: &[i64],
) -> Result<SdlcReindexProgress, String> {
    let pipelines = find_invalidated_pipelines(ontology, scope);

    let namespace_ids_with_completed_plans =
        get_namespace_ids_with_completed_plans(graph, checkpoint_table, &pipelines.namespaced)
            .await?;
    let completed_namespaces = enabled_namespace_ids
        .iter()
        .filter(|namespace_id| namespace_ids_with_completed_plans.contains(namespace_id))
        .count() as u64;
    let namespaced_ready = pipelines.namespaced.is_empty()
        || completed_namespaces == enabled_namespace_ids.len() as u64;

    let completed_global =
        count_completed_global_plans(graph, checkpoint_table, &pipelines.global).await?;
    let global_ready = completed_global as usize == pipelines.global.len();

    Ok(SdlcReindexProgress {
        completed_namespaces,
        ready: namespaced_ready && global_ready,
    })
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

        // Only promote a version this binary embeds; promoting one we don't run would flip us Outdated.
        if migrating_version != *SCHEMA_VERSION {
            return Ok(());
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

        crate::schema::migration::create_unversioned_tables(&self.graph, &self.ontology)
            .await
            .map_err(|error| {
                TaskError::new(format!(
                    "create unversioned ontology tables before promotion: {error}"
                ))
            })?;
        crate::schema::migration::replace_refreshable_views_for_version(
            &self.graph,
            &self.ontology,
            migrating_version,
        )
        .await
        .map_err(|error| {
            TaskError::new(format!(
                "replace refreshable ontology views for v{migrating_version} before promotion: {error}"
            ))
        })?;

        let versions = read_all_versions(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read all versions: {e}")))?;

        let mut retired_versions = Vec::new();
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
                retired_versions.push(entry.version);
            }
        }

        info!(
            version = migrating_version,
            "marking migrating version as active — schema migration complete"
        );
        mark_version_active(&self.graph, migrating_version)
            .await
            .map_err(|e| TaskError::new(format!("mark v{migrating_version} active: {e}")))?;

        for version in retired_versions {
            if let Err(error) = crate::schema::migration::drop_refreshable_views_for_version(
                &self.graph,
                &self.ontology,
                version,
            )
            .await
            {
                warn!(version, %error, "failed to drop refreshable views for retired schema");
            }
        }
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
    /// `v{N}_code_indexing_checkpoint` continuously via the `Migration`
    /// trigger's active backfill sweep regardless of migration state, so
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

        // Enabled top-level namespaces (the reference set). Subgroup paths are
        // dropped and logged in fetch_enabled_top_level_namespaces; they are never
        // dispatched, so counting them would wedge the gate forever.
        let enabled_namespaces = self
            .fetch_enabled_top_level_namespaces()
            .await
            .map_err(|e| format!("fetch enabled namespaces: {e}"))?;
        let enabled_count = enabled_namespaces.ids.len() as u64;

        if enabled_count == 0 {
            warn!(
                version,
                "enabled namespace count is 0 — skipping promotion to avoid \
                 premature completion during a datalake outage"
            );
            return Ok(false);
        }

        // Code-indexing telemetry. Computed for visibility and emitted as a
        // structured log field below; explicitly NOT part of the promotion
        // predicate. The backfill dispatcher fills
        // `v{N}_code_indexing_checkpoint` after promotion until coverage
        // approaches 100%, and operators watch the `code_coverage` field on
        // the "migration completion status" log line to track progress.
        let code_table = format!("{prefix}{CODE_INDEXING_CHECKPOINT_TABLE}");
        let (eligible_projects, indexed_projects, coverage) = self
            .compute_code_coverage(&code_table, &enabled_namespaces.paths)
            .await
            .map_err(|e| format!("compute code coverage: {e}"))?;

        let scope = self.resolve_migration_scope(version).await?;
        let checkpoint_table = format!("{prefix}{CHECKPOINT_TABLE}");
        let sdlc_progress = get_sdlc_reindex_progress_for_enabled_namespaces(
            &self.graph,
            &self.ontology,
            &scope,
            &checkpoint_table,
            &enabled_namespaces.ids,
        )
        .await?;

        info!(
            version,
            sdlc_indexed_namespaces = sdlc_progress.completed_namespaces,
            enabled_namespaces = enabled_count,
            code_indexed_projects = indexed_projects,
            code_eligible_projects = eligible_projects,
            code_coverage = coverage,
            migration_scope = %scope,
            "migration completion status"
        );

        // Story-telling gauges: indexed/eligible per scope, labeled by
        // version_band. Dashboards compute the ratio; alerts fire on
        // per-scope thresholds (sdlc < 100% during migration window, code
        // < 95% for >24h post-promotion, etc.).
        let current = *SCHEMA_VERSION;
        self.metrics.record_units(
            "sdlc",
            version,
            current,
            sdlc_progress.completed_namespaces,
            enabled_count,
        );
        self.metrics.record_units(
            "code",
            version,
            current,
            indexed_projects,
            eligible_projects,
        );

        Ok(sdlc_progress.ready)
    }

    async fn resolve_migration_scope(
        &self,
        migrating_version: u32,
    ) -> Result<MigrationScope, String> {
        let active = read_active_version(&self.graph)
            .await
            .map_err(|e| format!("read active version: {e}"))?
            .unwrap_or(0);
        let ledger = MigrationLedger::load_embedded()?;
        let requested_scope = ledger.resolve_migration_scope_between(active, migrating_version);
        Ok(get_migration_scope_for_table_writers(
            &self.ontology,
            &requested_scope,
        ))
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
    async fn compute_code_coverage(
        &self,
        code_table: &str,
        enabled_paths: &[String],
    ) -> Result<(u64, u64, f64), String> {
        let eligible_projects = self
            .count_eligible_projects()
            .await
            .map_err(|e| format!("count code-eligible projects: {e}"))?;

        let indexed_projects = self
            .count_scoped_checkpoint_projects(code_table, enabled_paths)
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
    async fn fetch_enabled_top_level_namespaces(&self) -> Result<TopLevelSplit, String> {
        let batches = self
            .datalake
            .query(&FETCH_ENABLED_NAMESPACES)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        let ids = i64::extract_column(&batches, 0).map_err(|e| e.to_string())?;
        let paths = String::extract_column(&batches, 1).map_err(|e| e.to_string())?;

        let split = gkg_utils::traversal_path::split_top_level(ids, paths);
        if !split.skipped.is_empty() {
            warn!(
                skipped = ?split.skipped,
                reason = "traversal_path is not a top-level org/namespace path",
                "excluding enabled namespaces from migration completion gate"
            );
        }
        Ok(split)
    }

    async fn count_eligible_projects(&self) -> Result<u64, String> {
        let batches = self
            .datalake
            .query(&COUNT_CODE_ELIGIBLE_PROJECTS)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;

        batches
            .first()
            .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "ns_count", 0))
            .ok_or_else(|| "no ns_count in result".to_string())
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
            .query(&COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED)
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
    async fn stop_merges_for_version(&self, version: u32) {
        let prefix = table_prefix(version);
        let db = self.graph.database();

        for t in &generate_graph_tables(&self.ontology) {
            let qualified = format!("{db}.{prefix}{}", t.name);
            let _ = self
                .graph
                .execute(&format!(
                    "SYSTEM STOP MERGES ON CLUSTER '{CLICKHOUSE_CLUSTER}' {qualified}"
                ))
                .await
                .inspect_err(
                    |e| warn!(version, table = %qualified, error = %e, "failed to stop merges"),
                );
        }
    }

    /// Enumerates dead-version objects via `system.tables` (keep-set computed
    /// in SQL). Ontology-known objects are always dropped. Objects not in the
    /// ontology are also dropped unless they match a `gc_preserve_patterns`
    /// regex.
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
        let preserve = compile_preserve_patterns(self.ontology.gc_preserve_patterns());
        let current = *SCHEMA_VERSION;

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
                if !known_names.contains(base_name)
                    && matches_preserve_pattern(base_name, &preserve)
                {
                    info!(version, object = %name, "GC: preserving (matches gc_preserve_patterns)");
                    continue;
                }

                let kind = drop_kind_for_engine(&engine);
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
            if let Err(e) =
                crate::nats::versioning::cleanup_schema_state(&self.nats_client, *version).await
            {
                warn!(version, error = %e, "GC: NATS cleanup failed, skipping mark_version_dropped");
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

async fn get_namespace_ids_with_completed_plans(
    graph: &ArrowClickHouseClient,
    checkpoint_table: &str,
    required_plan_names: &[String],
) -> Result<HashSet<i64>, String> {
    if required_plan_names.is_empty() {
        return Ok(HashSet::new());
    }
    let batches = graph
        .query(GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS)
        .param("table", checkpoint_table)
        .param("plans", required_plan_names)
        .param("plan_count", required_plan_names.len() as u64)
        .fetch_arrow()
        .await
        .map_err(|e| e.to_string())?;
    Ok(i64::extract_column(&batches, 0)
        .map_err(|e| e.to_string())?
        .into_iter()
        .collect())
}

async fn count_completed_global_plans(
    graph: &ArrowClickHouseClient,
    checkpoint_table: &str,
    global_plans: &[String],
) -> Result<u64, String> {
    if global_plans.is_empty() {
        return Ok(0);
    }
    let batches = graph
        .query(COUNT_COMPLETE_GLOBAL_PLANS)
        .param("table", checkpoint_table)
        .param("plans", global_plans)
        .fetch_arrow()
        .await
        .map_err(|e| e.to_string())?;
    batches
        .first()
        .and_then(|b| ArrowUtils::get_column::<UInt64Type>(b, "plan_count", 0))
        .ok_or_else(|| "no plan_count in result".to_string())
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

/// Returns `true` if `name` matches any of the preserve `patterns` (regexes).
fn matches_preserve_pattern(name: &str, patterns: &[regex::Regex]) -> bool {
    patterns.iter().any(|re| re.is_match(name))
}

/// Compiles `gc_preserve_patterns` strings into regexes. Invalid patterns
/// are logged and skipped so a typo cannot disable the entire GC sweep.
fn compile_preserve_patterns(raw: &[String]) -> Vec<regex::Regex> {
    raw.iter()
        .filter_map(|p| {
            regex::Regex::new(p)
                .inspect_err(|e| {
                    warn!(pattern = %p, error = %e, "gc_preserve_patterns: invalid regex, skipping")
                })
                .ok()
        })
        .collect()
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
    fn completed_namespace_id_query_uses_identifier_param() {
        assert!(
            GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS.contains("{table:Identifier}"),
            "SDLC checkpoint query must use Identifier param for table name"
        );
    }

    #[test]
    fn completed_namespace_id_query_filters_deleted_and_malformed_keys() {
        assert!(GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS.contains("_deleted = false"));
        assert!(GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS.contains("match(splitByChar"));
        assert!(!GET_NAMESPACE_IDS_WITH_COMPLETED_PLANS.contains("enabled_namespace_ids"));
    }

    #[test]
    fn fetch_enabled_namespaces_query_filters_deleted() {
        assert!(FETCH_ENABLED_NAMESPACES.contains("_siphon_deleted = false"));
    }

    #[test]
    fn fetch_enabled_namespaces_query_selects_id_and_path() {
        assert!(FETCH_ENABLED_NAMESPACES.contains("root_namespace_id"));
        assert!(FETCH_ENABLED_NAMESPACES.contains("traversal_path"));
    }

    #[test]
    fn migration_lock_key_matches_schema_migration() {
        assert_eq!(MIGRATION_LOCK_KEY, "schema_migration");
    }

    #[test]
    fn count_code_eligible_projects_query_filters_deleted() {
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("p.deleted = false"));
    }

    #[test]
    fn count_code_eligible_projects_query_counts_distinct_project_ids() {
        assert!(COUNT_CODE_ELIGIBLE_PROJECTS.contains("count(DISTINCT p.id)"));
    }

    #[test]
    fn count_code_eligible_projects_scoped_to_top_level() {
        assert!(
            COUNT_CODE_ELIGIBLE_PROJECTS.contains("match(traversal_path, '^[0-9]+/[0-9]+/$')"),
            "eligible-projects must scope to top-level enabled namespaces so a moved subgroup \
             cannot deflate coverage"
        );
        assert!(
            COUNT_CODE_ELIGIBLE_PROJECTS.contains("extract(p.traversal_path, '^[0-9]+/[0-9]+/')")
        );
        assert!(!COUNT_CODE_ELIGIBLE_PROJECTS.contains("arrayExists"));
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
        assert!(
            COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED
                .contains("extract(traversal_path, '^[0-9]+/[0-9]+/') IN {paths:Array(String)}"),
            "scoped checkpoint count must match the enabled top-level path set as a hash-set probe; \
             the `arrayExists(startsWith(...))` form it replaced peaked at 93 GiB and OOM-wedged promotion"
        );
        assert!(!COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED.contains("arrayExists"));
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
    fn gc_query_excludes_active_retired_and_migrating() {
        assert!(LIST_DEAD_VERSION_OBJECTS.contains("status = 'active'"));
        assert!(LIST_DEAD_VERSION_OBJECTS.contains("status = 'retired'"));
        assert!(
            LIST_DEAD_VERSION_OBJECTS.contains("status = 'migrating'")
                && !LIST_DEAD_VERSION_OBJECTS.contains("coalesce(max(version), 0)"),
            "every migrating version must be kept regardless of its relationship to active — \
             a rebuild-rollback version sits below active and must not be GC'd"
        );
    }

    #[test]
    fn preserve_pattern_empty_never_matches() {
        assert!(!matches_preserve_pattern("gl_edge_v2", &[]));
    }

    #[test]
    fn preserve_pattern_regex_match() {
        let patterns =
            compile_preserve_patterns(&["^keep_.*".to_string(), "^special_table$".to_string()]);
        assert!(matches_preserve_pattern("keep_this", &patterns));
        assert!(matches_preserve_pattern("special_table", &patterns));
        assert!(!matches_preserve_pattern("gl_edge", &patterns));
    }

    #[test]
    fn compile_preserve_patterns_skips_invalid() {
        let patterns = compile_preserve_patterns(&["^valid$".to_string(), "[invalid".to_string()]);
        assert_eq!(patterns.len(), 1);
    }
}

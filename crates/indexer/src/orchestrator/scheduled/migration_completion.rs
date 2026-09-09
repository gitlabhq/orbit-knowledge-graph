use std::sync::{Arc, LazyLock};

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::scope::{CODE_INDEXING_CHECKPOINT_TABLE, MigrationScope};
use orbit_migrations::version::{
    SCHEMA_VERSION, promote_version, read_migrating_version, table_prefix,
};
use orbit_server_config::{MigrationCompletionConfig, ScheduleConfiguration, SchemaConfig};
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::{TopLevelSplit, TraversalPath};
use tracing::{info, warn};

use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::locking::LockService;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::metrics::CompletionMetrics;

const MIGRATION_LOCK_KEY: &str = "schema_migration";
const LOCK_TTL: std::time::Duration = std::time::Duration::from_secs(120);

static COUNT_CODE_ELIGIBLE_PROJECTS: LazyLock<String> = LazyLock::new(|| {
    let del = ontology::siphon_deleted_column();
    let top = orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
    format!(
        "SELECT count(DISTINCT p.id) AS ns_count \
         FROM project_namespace_traversal_paths AS p \
         WHERE p.deleted = false \
           AND extract(p.traversal_path, '{top}') IN (\
               SELECT traversal_path FROM siphon_knowledge_graph_enabled_namespaces \
               WHERE {del} = false AND match(traversal_path, '{top}$'))"
    )
});

static COUNT_CODE_CHECKPOINT_PROJECTS_SCOPED: LazyLock<String> = LazyLock::new(|| {
    let top = orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;
    format!(
        "SELECT count(DISTINCT project_id) AS ns_count \
         FROM {{table:Identifier}} FINAL \
         WHERE _deleted = false \
           AND extract(traversal_path, '{top}') IN {{paths:Array(String)}}"
    )
});

const READ_MIGRATING_AGE: &str = "\
SELECT toUInt64(dateDiff('second', created_at, now())) AS age_seconds \
FROM gkg_schema_version FINAL \
WHERE status = 'migrating' AND version = {version:UInt32}";

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
    catalog: OntologyCatalog,
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
        catalog: OntologyCatalog,
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
            catalog,
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

pub use orbit_migrations::completion::SdlcReindexProgress;

impl MigrationCompletionChecker {
    async fn run_inner(&self) -> Result<(), TaskError> {
        self.check_completion().await?;
        self.reconcile_dead_versions().await?;
        Ok(())
    }

    async fn check_completion(&self) -> Result<(), TaskError> {
        let migrating = read_migrating_version(&self.graph)
            .await
            .map_err(|e| TaskError::new(format!("read migrating version: {e}")))?;

        let Some(migrating_version) = migrating else {
            self.metrics.record_migrating_age(0);
            return Ok(());
        };

        if let Ok(age) = self.fetch_migrating_age(migrating_version).await {
            self.metrics.record_migrating_age(age);
        }

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

        self.catalog
            .verify_archive(migrating_version)
            .await
            .map_err(|error| {
                TaskError::new(format!("verify ontology archive before promotion: {error}"))
            })?;

        {
            let schema = orbit_migrations::schema::GraphSchema::from_ontology(&self.ontology);
            orbit_migrations::execute::create_unversioned_definitions(&self.graph, &schema)
                .await
                .map_err(|error| {
                    TaskError::new(format!(
                        "create unversioned tables before promotion: {error}"
                    ))
                })?;
        }
        orbit_migrations::execute::replace_refreshable_views(
            &self.graph,
            &self.ontology,
            migrating_version,
        )
        .await
        .map_err(|error| {
            TaskError::new(format!(
                "replace refreshable views for v{migrating_version}: {error}"
            ))
        })?;

        info!(
            version = migrating_version,
            "marking migrating version as active — schema migration complete"
        );
        let retired_versions = promote_version(&self.graph, migrating_version)
            .await
            .map_err(|e| TaskError::new(format!("mark v{migrating_version} active: {e}")))?;

        for version in retired_versions {
            if let Err(error) = orbit_migrations::execute::drop_versioned_refreshable_views(
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

    async fn is_migration_complete(&self, version: u32) -> Result<bool, String> {
        let prefix = table_prefix(version);

        let enabled_namespaces = self
            .fetch_enabled_top_level_namespaces()
            .await
            .map_err(|e| format!("fetch enabled namespaces: {e}"))?;
        let enabled_count = enabled_namespaces.ids.len() as u64;

        let code_table = format!("{prefix}{CODE_INDEXING_CHECKPOINT_TABLE}");
        let code_coverage = match self
            .compute_code_coverage(&code_table, &enabled_namespaces.paths)
            .await
        {
            Ok(coverage) => Some(coverage),
            Err(error) => {
                warn!(version, %error, "code coverage telemetry unavailable this tick");
                None
            }
        };

        let scope = self.resolve_migration_scope(version).await?;
        let sdlc_progress = orbit_migrations::completion::check_sdlc_reindex_progress(
            &self.graph,
            &self.ontology,
            &scope,
            version,
            &enabled_namespaces.ids,
        )
        .await
        .map_err(|error| format!("check SDLC reindex progress: {error}"))?;

        info!(
            version,
            sdlc_indexed_namespaces = sdlc_progress.completed_namespaces,
            enabled_namespaces = enabled_count,
            code_indexed_projects = code_coverage.map(|(_, indexed, _)| indexed),
            code_eligible_projects = code_coverage.map(|(eligible, _, _)| eligible),
            code_coverage = code_coverage.map(|(_, _, ratio)| ratio),
            migration_scope = %scope,
            "migration completion status"
        );

        let current = *SCHEMA_VERSION;
        self.metrics.record_units(
            "sdlc",
            version,
            current,
            sdlc_progress.completed_namespaces,
            enabled_count,
        );
        if let Some((eligible_projects, indexed_projects, _)) = code_coverage {
            self.metrics.record_units(
                "code",
                version,
                current,
                indexed_projects,
                eligible_projects,
            );
        }

        Ok(sdlc_progress.ready)
    }

    async fn resolve_migration_scope(
        &self,
        migrating_version: u32,
    ) -> Result<MigrationScope, String> {
        orbit_migrations::completion::resolve_migration_scope(
            &self.graph,
            &self.ontology,
            migrating_version,
        )
        .await
        .map_err(|error| format!("resolve migration scope: {error}"))
    }

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

    async fn compute_code_coverage(
        &self,
        code_table: &str,
        enabled_paths: &[TraversalPath],
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

    async fn fetch_enabled_top_level_namespaces(&self) -> Result<TopLevelSplit, String> {
        orbit_migrations::completion::fetch_enabled_top_level_namespaces(&self.datalake)
            .await
            .map_err(|e| format!("fetch enabled namespaces: {e}"))
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

    async fn count_scoped_checkpoint_projects(
        &self,
        code_table: &str,
        enabled_paths: &[TraversalPath],
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

    async fn reconcile_dead_versions(&self) -> Result<(), TaskError> {
        let schema = orbit_migrations::schema::GraphSchema::from_ontology(&self.ontology);

        let entities = orbit_migrations::garbage_collection::find_droppable_entities(
            &self.graph,
            &schema,
            self.schema_config.max_retained_versions,
            self.ontology.gc_preserve_patterns(),
        )
        .await
        .map_err(|e| TaskError::new(format!("find droppable entities: {e}")))?;

        let result =
            orbit_migrations::garbage_collection::drop_entities(&self.graph, &entities).await;

        let current = *SCHEMA_VERSION;
        for version in result.fully_dropped_versions() {
            if let Err(e) = orbit_migrations::nats::cleanup_schema_version_buckets(
                &self.nats_client,
                version,
                crate::nats::versioning::MANAGED_BUCKETS,
            )
            .await
            {
                let e = e.to_string();
                warn!(version, error = %e, "GC: NATS cleanup failed, skipping mark_version_dropped");
                self.metrics.record_cleanup(version, current, "failure");
                continue;
            }
            if let Err(e) =
                orbit_migrations::version::mark_version_dropped(&self.graph, version).await
            {
                warn!(version, error = %e, "GC: failed to mark dropped");
            }
            self.metrics.record_cleanup(version, current, "success");
        }

        for version in &result.failed_versions {
            self.metrics.record_cleanup(*version, current, "failure");
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
        let config = orbit_server_config::AppConfig::embedded_defaults()
            .schedule
            .tasks
            .migration_completion;
        assert!(!config.schedule.cron.expression().is_empty());
    }
}

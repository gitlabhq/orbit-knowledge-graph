//! `analytics.graph_stats`: the daily `orbit_graph_stats` snapshot of the served graph.
//!
//! The cadence lock lets one dispatcher replica run it per day, so each
//! instance sends one snapshot. Counts drop soft-deleted rows: node counts read
//! `FINAL` and skip `_deleted`, and projects count only when the datalake still
//! has them under an enabled namespace.

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use clickhouse_client::FromArrowColumn;
use orbit_log_fields::{info, warn};
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::scope::CODE_INDEXING_CHECKPOINT_TABLE;
use orbit_migrations::version::{prefixed_table_name, read_active_version};
use orbit_server_config::{GraphStatsConfig, ScheduleConfiguration};
use orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;

use crate::analytics::{GraphStats, GraphStatsReporter};
use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::dispatch::enabled_namespaces::resolved_enabled_namespaces_sql;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};

const TASK_NAME: &str = "analytics.graph_stats";
/// A failed attempt is retried inside the run, because the cadence lock holds the day's slot.
const ATTEMPTS: u32 = 3;
const RETRY_DELAY: Duration = Duration::from_secs(60);

const GRAPH_BYTES_SQL: &str = "\
SELECT toInt64(sum(bytes_on_disk)) AS bytes \
FROM system.parts \
WHERE active AND database = currentDatabase() AND table IN {tables:Array(String)}";

const LIVE_ROWS_SQL: &str =
    "SELECT toInt64(count()) AS live_rows FROM {table:Identifier} FINAL WHERE _deleted = false";

static ELIGIBLE_PROJECTS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT DISTINCT id \
FROM project_namespace_traversal_paths FINAL \
WHERE deleted = false \
AND extract(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}') IN {{paths:Array(String)}}"
    )
});

static INDEXED_PROJECTS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT DISTINCT project_id \
FROM {{table:Identifier}} FINAL \
WHERE _deleted = false AND is_default_branch AND indexed_at IS NOT NULL \
AND extract(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}') IN {{paths:Array(String)}}"
    )
});

pub struct GraphStatsSnapshot {
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    catalog: OntologyCatalog,
    reporter: GraphStatsReporter,
    metrics: ScheduledTaskMetrics,
    config: GraphStatsConfig,
}

impl GraphStatsSnapshot {
    pub fn new(
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        catalog: OntologyCatalog,
        reporter: GraphStatsReporter,
        metrics: ScheduledTaskMetrics,
        config: GraphStatsConfig,
    ) -> Self {
        Self {
            graph,
            datalake,
            catalog,
            reporter,
            metrics,
            config,
        }
    }

    async fn snapshot(&self) -> Result<Option<GraphStats>, TaskError> {
        let Some((version, ontology)) = self.served_ontology().await? else {
            return Ok(None);
        };
        let paths = self.enabled_paths().await?;
        let (graph_bytes, entity_counts, projects_indexed) = tokio::try_join!(
            self.graph_bytes(&ontology, version),
            self.entity_counts(&ontology, version),
            self.projects_indexed(version, &paths),
        )?;

        Ok(Some(GraphStats {
            graph_bytes,
            entity_counts,
            projects_indexed,
            top_level_groups_indexed: paths.len() as u64,
            snapshot_at: Utc::now(),
        }))
    }

    /// The active schema version and the ontology it was built from.
    async fn served_ontology(&self) -> Result<Option<(u32, ontology::Ontology)>, TaskError> {
        let Some(version) = read_active_version(&self.graph)
            .await
            .map_err(TaskError::new)?
        else {
            return Ok(None);
        };
        let ontology = self
            .catalog
            .load(version)
            .await
            .map_err(TaskError::new)?
            .load_ontology()
            .map_err(TaskError::new)?;
        Ok(Some((version, ontology)))
    }

    async fn graph_bytes(
        &self,
        ontology: &ontology::Ontology,
        version: u32,
    ) -> Result<u64, TaskError> {
        let storage = query_data_model::Relational::<
            query_data_model::implementations::clickhouse::layout::ClickHouse,
        >::derive(ontology)
        .map_err(TaskError::new)?;
        let tables: Vec<String> = storage
            .deployed_tables()
            .map(|table| prefixed_table_name(&table.name, version))
            .collect();
        let batches = self
            .graph
            .query(GRAPH_BYTES_SQL)
            .param("tables", &tables)
            .fetch_arrow()
            .await
            .map_err(|error| self.query_error(error))?;
        first_count(&batches)
    }

    /// One table at a time, so the daily `FINAL` scans never stack up on the graph.
    async fn entity_counts(
        &self,
        ontology: &ontology::Ontology,
        version: u32,
    ) -> Result<BTreeMap<String, u64>, TaskError> {
        let mut counts = BTreeMap::new();
        for node in ontology.nodes() {
            let table = prefixed_table_name(&node.destination_table, version);
            let started = Instant::now();
            let batches = self
                .graph
                .query(LIVE_ROWS_SQL)
                .param("table", table.as_str())
                .fetch_arrow()
                .await
                .map_err(|error| self.query_error(error))?;
            self.metrics
                .record_query_duration(&table, started.elapsed().as_secs_f64());
            counts.insert(node.name.clone(), first_count(&batches)?);
        }
        Ok(counts)
    }

    /// A project counts as indexed only while the datalake still lists it.
    async fn projects_indexed(&self, version: u32, paths: &[String]) -> Result<u64, TaskError> {
        if paths.is_empty() {
            return Ok(0);
        }
        let code_checkpoint = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, version);
        let (eligible, indexed) = tokio::try_join!(
            self.project_ids(&self.datalake, &ELIGIBLE_PROJECTS_SQL, None, paths),
            self.project_ids(
                &self.graph,
                &INDEXED_PROJECTS_SQL,
                Some(&code_checkpoint),
                paths
            ),
        )?;
        let eligible: HashSet<i64> = eligible.into_iter().collect();
        let indexed = indexed
            .into_iter()
            .filter(|id| eligible.contains(id))
            .collect::<HashSet<_>>()
            .len();
        Ok(indexed as u64)
    }

    async fn project_ids(
        &self,
        client: &ArrowClickHouseClient,
        sql: &str,
        table: Option<&str>,
        paths: &[String],
    ) -> Result<Vec<i64>, TaskError> {
        let mut query = client.query(sql).param("paths", paths);
        if let Some(table) = table {
            query = query.param("table", table);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|error| self.query_error(error))?;
        i64::extract_column(&batches, 0).map_err(TaskError::new)
    }

    async fn enabled_paths(&self) -> Result<Vec<String>, TaskError> {
        let batches = self
            .datalake
            .query(resolved_enabled_namespaces_sql())
            .fetch_arrow()
            .await
            .map_err(|error| self.query_error(error))?;
        String::extract_column(&batches, 1).map_err(TaskError::new)
    }

    fn query_error(&self, error: impl std::fmt::Display) -> TaskError {
        self.metrics.record_error(TASK_NAME, "query");
        TaskError::new(error)
    }
}

#[async_trait]
impl ScheduledTask for GraphStatsSnapshot {
    fn name(&self) -> &str {
        TASK_NAME
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let result = self.snapshot_and_report().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(TASK_NAME, outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl GraphStatsSnapshot {
    async fn snapshot_and_report(&self) -> Result<(), TaskError> {
        let mut attempt = 1;
        let stats = loop {
            match self.snapshot().await {
                Ok(stats) => break stats,
                Err(error) if attempt < ATTEMPTS => {
                    warn!(
                        safe(ATTEMPT) = attempt,
                        safe(ERROR_TYPE) = "graph_stats_snapshot",
                        expanded(ERROR_MESSAGE) = %error,
                        "graph stats snapshot failed, retrying"
                    );
                    attempt += 1;
                    tokio::time::sleep(RETRY_DELAY).await;
                }
                Err(error) => return Err(error),
            }
        };
        let Some(stats) = stats else {
            info!("no active schema version, skipping the graph stats snapshot");
            return Ok(());
        };
        self.reporter.report(&stats).map_err(TaskError::new)?;
        info!(
            safe(COUNT) = stats.entity_counts.len(),
            "sent the graph stats snapshot"
        );
        Ok(())
    }
}

fn first_count(batches: &[arrow::record_batch::RecordBatch]) -> Result<u64, TaskError> {
    let value = i64::extract_column(batches, 0)
        .map_err(TaskError::new)?
        .into_iter()
        .next()
        .unwrap_or(0);
    Ok(u64::try_from(value).unwrap_or(0))
}

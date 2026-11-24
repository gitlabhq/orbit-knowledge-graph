use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use clickhouse_client::{ArrowQuery, FromArrowColumn};
use orbit_log_fields::{info, warn};
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::scope::CODE_INDEXING_CHECKPOINT_TABLE;
use orbit_migrations::version::{prefixed_table_name, read_active_version};
use orbit_server_config::{GraphStatsConfig, ScheduleConfiguration};
use orbit_utils::traversal_path::TOP_LEVEL_PREFIX_REGEX;

use crate::analytics::{GraphStats, GraphStatsReporter, NamespaceGraphStats};
use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::dispatch::enabled_namespaces::resolved_enabled_namespaces_sql;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::retry::{Backoff, LocalRetry, Step, drive};

const TASK_NAME: &str = "analytics.graph_stats";
const SNAPSHOT_RETRY: LocalRetry = LocalRetry {
    backoff: Backoff::Fixed(&[Duration::from_secs(60)]),
    max_attempts: 3,
};

const GRAPH_BYTES_SQL: &str = r#"SELECT toInt64(sum(bytes_on_disk)) AS bytes
FROM system.parts
WHERE active
  AND database = currentDatabase()
  AND table IN {tables:Array(String)}"#;

const LIVE_ROWS_BY_ROOT_SQL: &str = r#"SELECT toInt64OrZero(splitByChar('/', traversal_path)[2]) AS root_id,
       toInt64(count()) AS live_rows
FROM {table:Identifier} FINAL
WHERE _deleted = false
GROUP BY root_id"#;

const GLOBAL_LIVE_ROWS_SQL: &str = r#"SELECT toInt64(0) AS root_id, toInt64(count()) AS live_rows
FROM {table:Identifier} FINAL
WHERE _deleted = false"#;

const GRAPH_BYTES_BY_ROOT_SQL: &str = r#"SELECT toInt64OrZero(splitByChar('/', top_level_namespace)[2]) AS root_id,
       toInt64(sum(compressed_bytes)) AS bytes
FROM namespace_storage_snapshot FINAL
WHERE snapshot_date = (SELECT max(snapshot_date) FROM namespace_storage_snapshot)
GROUP BY root_id"#;

static ENABLED_ROOTS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT root_namespace_id FROM ({})",
        resolved_enabled_namespaces_sql()
    )
});

static ELIGIBLE_PROJECTS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"SELECT DISTINCT id, toInt64OrZero(splitByChar('/', traversal_path)[2]) AS root_id
FROM project_namespace_traversal_paths FINAL
WHERE deleted = false
  AND extract(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}') IN (
    SELECT traversal_path FROM ({}))"#,
        resolved_enabled_namespaces_sql()
    )
});

const INDEXED_PROJECTS_SQL: &str = r#"SELECT DISTINCT project_id
FROM {table:Identifier} FINAL
WHERE _deleted = false
  AND is_default_branch
  AND indexed_at IS NOT NULL"#;

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
        let (graph_bytes, mut entity_counts_by_root, indexed_by_root, roots, bytes_by_root) = tokio::try_join!(
            self.graph_bytes(&ontology, version),
            self.entity_counts_by_root(&ontology, version),
            self.projects_indexed_by_root(version),
            self.enabled_roots(),
            self.graph_bytes_by_root(),
        )?;

        let entity_counts = ontology
            .nodes()
            .map(|node| {
                let total = entity_counts_by_root
                    .values()
                    .filter_map(|counts| counts.get(&node.name))
                    .sum();
                (node.name.clone(), total)
            })
            .collect();
        let namespaces = roots
            .into_iter()
            .map(|root| NamespaceGraphStats {
                root_namespace_id: root,
                graph_bytes: bytes_by_root.get(&root).copied().unwrap_or_default(),
                entity_counts: entity_counts_by_root.remove(&root).unwrap_or_default(),
                projects_indexed: indexed_by_root.get(&root).copied().unwrap_or_default(),
            })
            .collect();

        Ok(Some(GraphStats {
            graph_bytes,
            entity_counts,
            projects_indexed: indexed_by_root.values().sum(),
            namespaces,
        }))
    }

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
            .fetch(self.graph.query(GRAPH_BYTES_SQL).param("tables", &tables))
            .await?;
        first_count(&batches)
    }

    async fn entity_counts_by_root(
        &self,
        ontology: &ontology::Ontology,
        version: u32,
    ) -> Result<HashMap<i64, BTreeMap<String, u64>>, TaskError> {
        let mut by_root: HashMap<i64, BTreeMap<String, u64>> = HashMap::new();
        for node in ontology.nodes() {
            let table = prefixed_table_name(&node.destination_table, version);
            let sql = if node.has_traversal_path {
                LIVE_ROWS_BY_ROOT_SQL
            } else {
                GLOBAL_LIVE_ROWS_SQL
            };
            let started = Instant::now();
            let batches = self
                .fetch(self.graph.query(sql).param("table", table.as_str()))
                .await?;
            self.metrics
                .record_query_duration(&table, started.elapsed().as_secs_f64());
            for (root, count) in counts_by_root(&batches)? {
                by_root
                    .entry(root)
                    .or_default()
                    .insert(node.name.clone(), count);
            }
        }
        Ok(by_root)
    }

    async fn projects_indexed_by_root(&self, version: u32) -> Result<HashMap<i64, u64>, TaskError> {
        let code_checkpoint = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, version);
        let (eligible, indexed) = tokio::try_join!(
            self.fetch(self.datalake.query(&ELIGIBLE_PROJECTS_SQL)),
            self.fetch(
                self.graph
                    .query(INDEXED_PROJECTS_SQL)
                    .param("table", code_checkpoint.as_str())
            ),
        )?;
        let indexed: HashSet<i64> = i64::extract_column(&indexed, 0)
            .map_err(TaskError::new)?
            .into_iter()
            .collect();
        let projects = i64::extract_column(&eligible, 0).map_err(TaskError::new)?;
        let roots = i64::extract_column(&eligible, 1).map_err(TaskError::new)?;
        let mut by_root = HashMap::new();
        for (_, root) in projects
            .into_iter()
            .zip(roots)
            .filter(|(project, _)| indexed.contains(project))
        {
            *by_root.entry(root).or_default() += 1;
        }
        Ok(by_root)
    }

    async fn enabled_roots(&self) -> Result<Vec<i64>, TaskError> {
        let batches = self.fetch(self.datalake.query(&ENABLED_ROOTS_SQL)).await?;
        i64::extract_column(&batches, 0).map_err(TaskError::new)
    }

    async fn graph_bytes_by_root(&self) -> Result<HashMap<i64, u64>, TaskError> {
        let batches = self
            .fetch(self.graph.query(GRAPH_BYTES_BY_ROOT_SQL))
            .await?;
        counts_by_root(&batches)
    }

    async fn fetch(&self, query: ArrowQuery) -> Result<Vec<RecordBatch>, TaskError> {
        query
            .fetch_arrow()
            .await
            .map_err(|error| self.query_error(error))
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
        let stats = drive(&SNAPSHOT_RETRY, |attempt| async move {
            match self.snapshot().await {
                Ok(stats) => Step::Done(stats),
                Err(error) => {
                    warn!(
                        safe(ATTEMPT) = attempt + 1,
                        safe(ERROR_TYPE) = "graph_stats_snapshot",
                        expanded(ERROR_MESSAGE) = %error,
                        "graph stats snapshot failed"
                    );
                    SNAPSHOT_RETRY.retry_or_give_up(attempt, (), error)
                }
            }
        })
        .await?;
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

fn counts_by_root(batches: &[RecordBatch]) -> Result<HashMap<i64, u64>, TaskError> {
    let roots = i64::extract_column(batches, 0).map_err(TaskError::new)?;
    let counts = i64::extract_column(batches, 1).map_err(TaskError::new)?;
    Ok(roots
        .into_iter()
        .zip(counts)
        .map(|(root, count)| (root, u64::try_from(count).unwrap_or(0)))
        .collect())
}

fn first_count(batches: &[RecordBatch]) -> Result<u64, TaskError> {
    let value = i64::extract_column(batches, 0)
        .map_err(TaskError::new)?
        .into_iter()
        .next()
        .unwrap_or(0);
    Ok(u64::try_from(value).unwrap_or(0))
}

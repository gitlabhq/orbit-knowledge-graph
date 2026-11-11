//! `analytics.graph_stats`: the daily `gkg_graph_stats` snapshot of the served graph.
//!
//! The cadence lock lets one dispatcher replica run it per day, so each
//! instance sends one snapshot. Counts drop soft-deleted rows: node counts read
//! `FINAL` and skip `_deleted`, and projects count only when the datalake still
//! has them under an enabled namespace.

use std::collections::{BTreeMap, HashSet};
use std::sync::LazyLock;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use clickhouse_client::FromArrowColumn;
use orbit_log_fields::info;
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
const METRIC_NAME: &str = "analytics_graph_stats";

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
        // Snapshot what users query: the active version, read with its own ontology.
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
        let tables = StatsTables::from_ontology(&ontology, version)?;

        let paths = self.enabled_paths().await?;
        let (graph_bytes, entity_counts, (projects_indexed, projects_eligible)) = tokio::try_join!(
            self.graph_bytes(&tables.graph),
            self.entity_counts(&tables.nodes),
            self.project_coverage(&tables.code_checkpoint, &paths),
        )?;

        Ok(Some(GraphStats {
            graph_bytes,
            entity_counts,
            projects_indexed,
            projects_eligible,
            snapshot_at: Utc::now(),
        }))
    }

    async fn graph_bytes(&self, tables: &[String]) -> Result<u64, TaskError> {
        let batches = self
            .graph
            .query(GRAPH_BYTES_SQL)
            .param("tables", tables)
            .fetch_arrow()
            .await
            .map_err(|error| self.query_error(error))?;
        first_count(&batches)
    }

    /// One table at a time, so the daily `FINAL` scans never stack up on the graph.
    async fn entity_counts(
        &self,
        nodes: &[(String, String)],
    ) -> Result<BTreeMap<String, u64>, TaskError> {
        let mut counts = BTreeMap::new();
        for (node, table) in nodes {
            let started = Instant::now();
            let batches = self
                .graph
                .query(LIVE_ROWS_SQL)
                .param("table", table.as_str())
                .fetch_arrow()
                .await
                .map_err(|error| self.query_error(error))?;
            self.metrics
                .record_query_duration(table, started.elapsed().as_secs_f64());
            counts.insert(node.clone(), first_count(&batches)?);
        }
        Ok(counts)
    }

    /// `(indexed, eligible)`. A project counts as indexed only while the datalake still lists it.
    async fn project_coverage(
        &self,
        code_checkpoint: &str,
        paths: &[String],
    ) -> Result<(u64, u64), TaskError> {
        if paths.is_empty() {
            return Ok((0, 0));
        }
        let (eligible, indexed) = tokio::try_join!(
            self.project_ids(&self.datalake, &ELIGIBLE_PROJECTS_SQL, None, paths),
            self.project_ids(
                &self.graph,
                &INDEXED_PROJECTS_SQL,
                Some(code_checkpoint),
                paths
            ),
        )?;
        let eligible: HashSet<i64> = eligible.into_iter().collect();
        let indexed = indexed
            .into_iter()
            .filter(|id| eligible.contains(id))
            .collect::<HashSet<_>>()
            .len();
        Ok((indexed as u64, eligible.len() as u64))
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
        self.metrics.record_error(METRIC_NAME, "query");
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
            .record_run(METRIC_NAME, outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl GraphStatsSnapshot {
    async fn snapshot_and_report(&self) -> Result<(), TaskError> {
        let Some(stats) = self.snapshot().await? else {
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

/// The versioned tables one snapshot reads, all derived from the served ontology.
#[derive(Debug)]
struct StatsTables {
    /// Every deployed graph table, for `graph_bytes`.
    graph: Vec<String>,
    /// `(node type name, table)`. The keys of `entity_counts` come only from here.
    nodes: Vec<(String, String)>,
    code_checkpoint: String,
}

impl StatsTables {
    fn from_ontology(ontology: &ontology::Ontology, version: u32) -> Result<Self, TaskError> {
        let storage = query_data_model::Relational::<
            query_data_model::implementations::clickhouse::layout::ClickHouse,
        >::derive(ontology)
        .map_err(TaskError::new)?;
        let graph = storage
            .deployed_tables()
            .map(|table| prefixed_table_name(&table.name, version))
            .collect();
        let nodes = ontology
            .nodes()
            .map(|node| {
                (
                    node.name.clone(),
                    prefixed_table_name(&node.destination_table, version),
                )
            })
            .collect();
        Ok(Self {
            graph,
            nodes,
            code_checkpoint: prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, version),
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tables() -> StatsTables {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        StatsTables::from_ontology(&ontology, 7).unwrap()
    }

    #[test]
    fn entity_count_keys_are_exactly_the_ontology_node_types() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let mut keys: Vec<String> = tables().nodes.into_iter().map(|(node, _)| node).collect();
        let mut expected: Vec<&str> = ontology.node_names().collect();
        keys.sort_unstable();
        expected.sort_unstable();
        assert_eq!(keys, expected);
        // The Iglu key pattern is ^[A-Z][A-Za-z0-9]{0,63}$.
        for key in &keys {
            let valid = key.len() <= 64
                && key.starts_with(|c: char| c.is_ascii_uppercase())
                && key.chars().all(|c| c.is_ascii_alphanumeric());
            assert!(valid, "{key} breaks the Iglu key pattern");
        }
    }

    #[test]
    fn every_node_table_is_a_deployed_graph_table_of_the_version() {
        let tables = tables();
        for (node, table) in &tables.nodes {
            assert!(table.starts_with("v7_"), "{node}: {table}");
            assert!(
                tables.graph.contains(table),
                "{node}: {table} is not deployed"
            );
        }
        assert_eq!(tables.code_checkpoint, "v7_code_indexing_checkpoint");
    }

    #[test]
    fn project_queries_match_only_enabled_top_level_paths() {
        for sql in [&*ELIGIBLE_PROJECTS_SQL, &*INDEXED_PROJECTS_SQL] {
            assert!(sql.contains("FINAL"), "{sql}");
            assert!(
                sql.contains(&format!(
                    "extract(traversal_path, '{TOP_LEVEL_PREFIX_REGEX}') IN {{paths:Array(String)}}"
                )),
                "{sql}"
            );
        }
        assert!(INDEXED_PROJECTS_SQL.contains("is_default_branch AND indexed_at IS NOT NULL"));
    }
}

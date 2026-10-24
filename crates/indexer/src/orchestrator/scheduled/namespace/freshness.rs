use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::EtlScope;
use opentelemetry::KeyValue;
use opentelemetry::metrics::Histogram;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::version::read_active_version;
use orbit_observability::indexer::sdlc;
use orbit_server_config::{NamespaceFreshnessConfig, ScheduleConfiguration};

use super::datalake::render_change_query;
use crate::checkpoint::{
    Checkpoint, CheckpointStore, ClickHouseCheckpointStore, NAMESPACE_KEY_PREFIX,
    namespace_position_key,
};
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::dispatch::enabled_namespaces::{
    ENABLED_NAMESPACE_TABLE, resolved_enabled_namespaces_sql,
};
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};

// The last sweep syncs idle entities; the margin keeps a late or slow sweep inside the scan.
const SWEEP_MARGIN: TimeDelta = TimeDelta::minutes(15);

const FRESHNESS_QUERY_SQL: &str = r#"SELECT root_path, target, max(watermark) AS latest
FROM changed
WHERE root_path IN (SELECT traversal_path FROM enabled)
GROUP BY root_path, target"#;

static ENABLED_ROOTS_SQL: LazyLock<String> = LazyLock::new(|| {
    format!(
        "SELECT enabled.root_namespace_id, enabled.traversal_path, enrolled.enabled_at \
FROM ({}) AS enabled \
INNER JOIN (SELECT root_namespace_id, max(created_at) AS enabled_at \
FROM {ENABLED_NAMESPACE_TABLE} GROUP BY root_namespace_id) AS enrolled \
ON enabled.root_namespace_id = enrolled.root_namespace_id",
        resolved_enabled_namespaces_sql()
    )
});

pub struct NamespaceFreshness {
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    catalog: OntologyCatalog,
    scan_window: TimeDelta,
    lag: Histogram<f64>,
    metrics: ScheduledTaskMetrics,
    config: NamespaceFreshnessConfig,
}

struct Root {
    checkpoint_prefix: String,
    path: String,
    enabled_at: DateTime<Utc>,
}

type LatestChanges = HashMap<String, HashMap<String, DateTime<Utc>>>;

impl NamespaceFreshness {
    pub fn new(
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        catalog: OntologyCatalog,
        config: NamespaceFreshnessConfig,
        sweep_interval_secs: u64,
    ) -> Self {
        let sweep_interval = TimeDelta::seconds(sweep_interval_secs.try_into().unwrap_or(i64::MAX));
        Self {
            graph,
            datalake,
            catalog,
            scan_window: sweep_interval + SWEEP_MARGIN,
            lag: sdlc::FRESHNESS_LAG.build_histogram_f64(&orbit_observability::meter()),
            metrics: ScheduledTaskMetrics::new(),
            config,
        }
    }

    async fn enabled_roots(&self) -> Result<Vec<Root>, TaskError> {
        let batches = self
            .datalake
            .query(&ENABLED_ROOTS_SQL)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        let enabled_ats = DateTime::<Utc>::extract_column(&batches, 2).map_err(TaskError::new)?;

        Ok(ids
            .into_iter()
            .zip(paths)
            .zip(enabled_ats)
            .map(|((id, path), enabled_at)| Root {
                checkpoint_prefix: namespace_position_key(id),
                path,
                enabled_at,
            })
            .collect())
    }

    async fn latest_changes(
        &self,
        ontology: &ontology::Ontology,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<LatestChanges, TaskError> {
        let batches = self
            .datalake
            .query(&render_change_query(
                FRESHNESS_QUERY_SQL,
                &ontology.reindex_sources(),
            ))
            .param("lower", from.format(TIMESTAMP_FORMAT).to_string())
            .param("upper", to.format(TIMESTAMP_FORMAT).to_string())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 0).map_err(TaskError::new)?;
        let targets = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        let latest = DateTime::<Utc>::extract_column(&batches, 2).map_err(TaskError::new)?;

        let mut changes = LatestChanges::new();
        for ((path, target), latest) in paths.into_iter().zip(targets).zip(latest) {
            changes.entry(path).or_default().insert(target, latest);
        }
        Ok(changes)
    }
}

#[async_trait]
impl ScheduledTask for NamespaceFreshness {
    fn name(&self) -> &str {
        "namespace_freshness"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();
        let result = self.record_lag().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, start.elapsed().as_secs_f64());
        result
    }
}

impl NamespaceFreshness {
    async fn record_lag(&self) -> Result<(), TaskError> {
        let Some(version) = read_active_version(&self.graph)
            .await
            .map_err(TaskError::new)?
        else {
            return Ok(());
        };
        let ontology = self
            .catalog
            .load(version)
            .await
            .map_err(TaskError::new)?
            .load_ontology()
            .map_err(TaskError::new)?;
        let checkpoint_store =
            ClickHouseCheckpointStore::for_version(Arc::new(self.graph.clone()), version);
        let now = Utc::now();
        let scanned_from = now - self.scan_window;
        let (checkpoints, roots, changes) = tokio::try_join!(
            async {
                checkpoint_store
                    .load_by_prefix(NAMESPACE_KEY_PREFIX)
                    .await
                    .map_err(TaskError::new)
            },
            self.enabled_roots(),
            self.latest_changes(&ontology, scanned_from, now),
        )?;
        let checkpoints: HashMap<String, Checkpoint> = checkpoints.into_iter().collect();

        let plans = ontology
            .pipeline_descriptors()
            .into_iter()
            .filter(|plan| plan.scope == EtlScope::Namespaced);
        for plan in plans {
            let labels = [KeyValue::new(sdlc::labels::ENTITY, plan.name.clone())];
            for root in &roots {
                let key = format!("{}.{}", root.checkpoint_prefix, plan.name);
                let synced = checkpoints.get(&key).and_then(Checkpoint::synced_until);
                let latest_change = changes
                    .get(&root.path)
                    .and_then(|targets| targets.get(&plan.entity))
                    .copied();
                let behind = lag(now, scanned_from, root.enabled_at, synced, latest_change);
                self.lag.record(behind.as_seconds_f64(), &labels);
            }
        }
        Ok(())
    }
}

/// An upper bound on how far indexed data trails the datalake: merges keep only the newest version
/// of a row, so the first unindexed change is lost and a pending entity counts from its last sync.
fn lag(
    now: DateTime<Utc>,
    scanned_from: DateTime<Utc>,
    enabled_at: DateTime<Utc>,
    synced: Option<DateTime<Utc>>,
    latest_change: Option<DateTime<Utc>>,
) -> TimeDelta {
    let Some(synced) = synced else {
        return now - enabled_at;
    };
    let pending = latest_change.is_some_and(|latest| latest > synced);
    if synced < scanned_from || pending {
        return now - synced;
    }
    TimeDelta::zero()
}

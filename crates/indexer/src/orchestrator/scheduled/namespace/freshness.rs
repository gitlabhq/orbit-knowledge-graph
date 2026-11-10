use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::EtlScope;
use ontology::pipelines::PipelineDescriptor;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Histogram, ObservableGauge};
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::version::read_active_version;
use orbit_observability::indexer::sdlc;
use orbit_server_config::{NamespaceFreshnessConfig, ScheduleConfiguration};
use parking_lot::Mutex;

use super::datalake::render_change_query;
use crate::checkpoint::{
    Checkpoint, CheckpointStore, ClickHouseCheckpointStore, GLOBAL_SCOPE_KEY, NAMESPACE_KEY_PREFIX,
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
    oldest_sync: Arc<Mutex<Vec<(String, i64)>>>,
    // Dropping the handle deregisters the gauge callback.
    _oldest_sync_gauge: ObservableGauge<i64>,
    metrics: ScheduledTaskMetrics,
    config: NamespaceFreshnessConfig,
}

struct Root {
    checkpoint_prefix: String,
    path: String,
    enabled_at: DateTime<Utc>,
}

type LatestChanges = HashMap<String, HashMap<String, DateTime<Utc>>>;

struct SyncState {
    now: DateTime<Utc>,
    scanned_from: DateTime<Utc>,
    roots: Vec<Root>,
    checkpoints: HashMap<String, Checkpoint>,
    changes: LatestChanges,
}

impl NamespaceFreshness {
    pub fn new(
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        catalog: OntologyCatalog,
        config: NamespaceFreshnessConfig,
        sweep_interval_secs: u64,
    ) -> Self {
        let sweep_interval = TimeDelta::seconds(sweep_interval_secs.try_into().unwrap_or(i64::MAX));
        let meter = orbit_observability::meter();
        let oldest_sync: Arc<Mutex<Vec<(String, i64)>>> = Arc::default();
        let reported = Arc::clone(&oldest_sync);
        // Observing only the last run's plans drops a plan the served ontology no longer has.
        let oldest_sync_gauge =
            sdlc::FRESHNESS_OLDEST_SYNC.build_observable_gauge_i64(&meter, move |observer| {
                for (plan, timestamp) in reported.lock().iter() {
                    observer.observe(
                        *timestamp,
                        &[KeyValue::new(sdlc::labels::ENTITY, plan.clone())],
                    );
                }
            });
        Self {
            graph,
            datalake,
            catalog,
            scan_window: sweep_interval + SWEEP_MARGIN,
            lag: sdlc::FRESHNESS_LAG.build_histogram_f64(&meter),
            oldest_sync,
            _oldest_sync_gauge: oldest_sync_gauge,
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
        let global_prefix = format!("{GLOBAL_SCOPE_KEY}.");
        let now = Utc::now();
        let scanned_from = now - self.scan_window;
        let (namespace_checkpoints, global_checkpoints, roots, changes) = tokio::try_join!(
            load_checkpoints(&checkpoint_store, NAMESPACE_KEY_PREFIX),
            load_checkpoints(&checkpoint_store, &global_prefix),
            self.enabled_roots(),
            self.latest_changes(&ontology, scanned_from, now),
        )?;
        let state = SyncState {
            now,
            scanned_from,
            roots,
            checkpoints: namespace_checkpoints
                .into_iter()
                .chain(global_checkpoints)
                .collect(),
            changes,
        };

        let mut oldest_sync = Vec::new();
        for plan in ontology.pipeline_descriptors() {
            let synced_until = match plan.scope {
                EtlScope::Namespaced => self.record_plan_lag(&plan, &state),
                EtlScope::Global => global_synced_until(&plan, &state),
            };
            if let Some(synced_until) = synced_until {
                oldest_sync.push((plan.name, synced_until.timestamp()));
            }
        }
        *self.oldest_sync.lock() = oldest_sync;
        Ok(())
    }

    /// Records one lag sample per enabled root and returns the time up to which every root is in sync.
    /// Without enabled roots there is no data to measure, so the plan reports nothing.
    fn record_plan_lag(
        &self,
        plan: &PipelineDescriptor,
        state: &SyncState,
    ) -> Option<DateTime<Utc>> {
        if state.roots.is_empty() {
            return None;
        }
        let labels = [KeyValue::new(sdlc::labels::ENTITY, plan.name.clone())];
        let mut most_behind = TimeDelta::zero();
        for root in &state.roots {
            let key = format!("{}.{}", root.checkpoint_prefix, plan.name);
            let synced = state
                .checkpoints
                .get(&key)
                .and_then(Checkpoint::synced_until);
            let latest_change = state
                .changes
                .get(&root.path)
                .and_then(|targets| targets.get(&plan.entity))
                .copied();
            let behind = lag(
                state.now,
                state.scanned_from,
                root.enabled_at,
                synced,
                latest_change,
            );
            self.lag.record(behind.as_seconds_f64(), &labels);
            most_behind = most_behind.max(behind);
        }
        Some(state.now - most_behind)
    }
}

async fn load_checkpoints(
    store: &ClickHouseCheckpointStore,
    prefix: &str,
) -> Result<Vec<(String, Checkpoint)>, TaskError> {
    store.load_by_prefix(prefix).await.map_err(TaskError::new)
}

/// The global dispatcher targets the current time every minute, so a global checkpoint is its own sync point.
/// A plan that never synced reports nothing; promotion requires every global plan, so only a fresh install has one.
fn global_synced_until(plan: &PipelineDescriptor, state: &SyncState) -> Option<DateTime<Utc>> {
    let key = format!("{GLOBAL_SCOPE_KEY}.{}", plan.name);
    state
        .checkpoints
        .get(&key)
        .and_then(Checkpoint::synced_until)
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

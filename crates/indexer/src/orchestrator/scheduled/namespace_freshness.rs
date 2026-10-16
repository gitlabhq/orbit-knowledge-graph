use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use ontology::pipelines::PipelineDescriptor;
use opentelemetry::KeyValue;
use opentelemetry::metrics::AsyncInstrument;
use orbit_migrations::catalog::OntologyCatalog;
use orbit_migrations::completion::fetch_enabled_top_level_namespaces;
use orbit_migrations::version::read_active_version;
use orbit_observability::indexer::freshness as spec;
use orbit_server_config::{NamespaceFreshnessConfig, ScheduleConfiguration};

use crate::clickhouse::ArrowClickHouseClient;
use crate::indexing_status::{
    PLAN_CHECKPOINTS_SQL, Phase, PlanCheckpoints, combine_phases, fetch_plan_checkpoints,
    namespaced_plans,
};
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};

const ROOTS_PER_QUERY: usize = 1_000;
const PHASES: [Phase; 4] = [
    Phase::NotStarted,
    Phase::Syncing,
    Phase::Ready,
    Phase::Error,
];
const READY_AGE_BANDS: [(&str, TimeDelta); 5] = [
    ("1h", TimeDelta::hours(1)),
    ("2h", TimeDelta::hours(2)),
    ("4h", TimeDelta::hours(4)),
    ("8h", TimeDelta::hours(8)),
    ("16h", TimeDelta::hours(16)),
];
// A replica that stops winning the cadence lock must stop exporting instead of repeating an old snapshot.
const SNAPSHOT_TTL_INTERVALS: u32 = 3;

type Snapshot = Arc<Mutex<Option<Freshness>>>;

pub struct NamespaceFreshness {
    graph: ArrowClickHouseClient,
    datalake: ArrowClickHouseClient,
    catalog: OntologyCatalog,
    config: NamespaceFreshnessConfig,
    snapshot: Snapshot,
}

impl NamespaceFreshness {
    pub fn new(
        graph: ArrowClickHouseClient,
        datalake: ArrowClickHouseClient,
        catalog: OntologyCatalog,
        config: NamespaceFreshnessConfig,
    ) -> Self {
        let snapshot = Snapshot::default();
        let ttl = config
            .schedule
            .interval_hint()
            .saturating_mul(SNAPSHOT_TTL_INTERVALS);
        register_gauges(&snapshot, ttl);
        Self {
            graph,
            datalake,
            catalog,
            config,
            snapshot,
        }
    }

    async fn read_freshness(&self) -> Result<Option<Freshness>, TaskError> {
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
        let plans = namespaced_plans(&ontology);
        if plans.is_empty() {
            return Err(TaskError::new(format!(
                "schema version {version} has no namespaced plans"
            )));
        }
        let roots = fetch_enabled_top_level_namespaces(&self.datalake)
            .await
            .map_err(TaskError::new)?
            .ids;

        let mut freshness = Freshness::new(Utc::now());
        for chunk in roots.chunks(ROOTS_PER_QUERY) {
            let checkpoints =
                fetch_plan_checkpoints(self.graph.query(PLAN_CHECKPOINTS_SQL), version, chunk)
                    .await
                    .map_err(TaskError::new)?;
            for namespace in checkpoints.values() {
                freshness.add(namespace, &plans);
            }
        }
        Ok(Some(freshness))
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
        let freshness = self.read_freshness().await?;
        *self.snapshot.lock().unwrap_or_else(PoisonError::into_inner) = freshness;
        Ok(())
    }
}

struct Freshness {
    computed_at: DateTime<Utc>,
    namespaces: HashMap<Phase, i64>,
    ready_within: [i64; READY_AGE_BANDS.len()],
    oldest_completion: HashMap<String, DateTime<Utc>>,
}

impl Freshness {
    fn new(computed_at: DateTime<Utc>) -> Self {
        Self {
            computed_at,
            namespaces: HashMap::new(),
            ready_within: [0; READY_AGE_BANDS.len()],
            oldest_completion: HashMap::new(),
        }
    }

    fn add(&mut self, checkpoints: &PlanCheckpoints, plans: &[PipelineDescriptor]) {
        let phase = combine_phases(
            plans
                .iter()
                .map(|plan| checkpoints.get_plan_phase(&plan.name, self.computed_at)),
        )
        .unwrap_or(Phase::Unknown);
        *self.namespaces.entry(phase).or_default() += 1;

        if phase == Phase::Ready
            && let Some(completed_at) = checkpoints.get_last_completed_at(plans)
        {
            let age = self.computed_at - completed_at;
            for ((_, max_age), count) in READY_AGE_BANDS.iter().zip(&mut self.ready_within) {
                if age <= *max_age {
                    *count += 1;
                }
            }
        }

        for plan in plans {
            if let Some(completed_at) = checkpoints.get_completed_at(&plan.name) {
                self.oldest_completion
                    .entry(plan.name.clone())
                    .and_modify(|oldest| *oldest = (*oldest).min(completed_at))
                    .or_insert(completed_at);
            }
        }
    }

    fn observe_namespaces(&self, observer: &dyn AsyncInstrument<i64>) {
        for phase in PHASES {
            let count = self.namespaces.get(&phase).copied().unwrap_or(0);
            observer.observe(count, &[KeyValue::new(spec::labels::PHASE, phase.as_str())]);
        }
    }

    fn observe_ready_namespaces(&self, observer: &dyn AsyncInstrument<i64>) {
        for ((max_age, _), count) in READY_AGE_BANDS.iter().zip(self.ready_within) {
            observer.observe(count, &[KeyValue::new(spec::labels::MAX_AGE, *max_age)]);
        }
    }

    fn observe_plan_oldest_age(&self, observer: &dyn AsyncInstrument<i64>) {
        for (plan, oldest) in &self.oldest_completion {
            let age = (self.computed_at - *oldest).num_seconds();
            observer.observe(age, &[KeyValue::new(spec::labels::PLAN, plan.clone())]);
        }
    }
}

fn register_gauges(snapshot: &Snapshot, ttl: Duration) {
    let meter = orbit_observability::meter();
    spec::NAMESPACES.build_observable_gauge_i64(
        &meter,
        observe_recent(snapshot, ttl, Freshness::observe_namespaces),
    );
    spec::READY_NAMESPACES.build_observable_gauge_i64(
        &meter,
        observe_recent(snapshot, ttl, Freshness::observe_ready_namespaces),
    );
    spec::PLAN_OLDEST_AGE.build_observable_gauge_i64(
        &meter,
        observe_recent(snapshot, ttl, Freshness::observe_plan_oldest_age),
    );
}

fn observe_recent(
    snapshot: &Snapshot,
    ttl: Duration,
    observe: fn(&Freshness, &dyn AsyncInstrument<i64>),
) -> impl Fn(&dyn AsyncInstrument<i64>) + Send + Sync + 'static {
    let snapshot = Arc::downgrade(snapshot);
    move |observer| {
        let Some(snapshot) = Weak::upgrade(&snapshot) else {
            return;
        };
        let snapshot = snapshot.lock().unwrap_or_else(PoisonError::into_inner);
        let recent = |freshness: &&Freshness| {
            (Utc::now() - freshness.computed_at)
                .to_std()
                .is_ok_and(|age| age <= ttl)
        };
        if let Some(freshness) = snapshot.as_ref().filter(recent) {
            observe(freshness, observer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Recorder(Mutex<Vec<i64>>);

    impl AsyncInstrument<i64> for Recorder {
        fn observe(&self, measurement: i64, _attributes: &[KeyValue]) {
            self.0.lock().unwrap().push(measurement);
        }
    }

    fn observations_of_snapshot_aged(age: TimeDelta) -> usize {
        let snapshot = Snapshot::default();
        *snapshot.lock().unwrap() = Some(Freshness::new(Utc::now() - age));
        let recorder = Recorder::default();
        let observe = observe_recent(
            &snapshot,
            Duration::from_secs(900),
            Freshness::observe_namespaces,
        );
        observe(&recorder);
        recorder.0.lock().unwrap().len()
    }

    #[test]
    fn recent_snapshot_is_exported() {
        assert_eq!(
            observations_of_snapshot_aged(TimeDelta::minutes(5)),
            PHASES.len()
        );
    }

    #[test]
    fn expired_snapshot_is_not_exported() {
        assert_eq!(observations_of_snapshot_aged(TimeDelta::minutes(20)), 0);
    }
}

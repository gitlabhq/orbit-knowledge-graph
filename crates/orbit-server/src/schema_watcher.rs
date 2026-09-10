use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use opentelemetry::KeyValue;
use orbit_migrations::version::{
    STATUS_ACTIVE, STATUS_MIGRATING, STATUS_RETIRED, SchemaVersionError, read_all_versions,
    version_tables_complete,
};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaState {
    Pending = 0,
    Ready = 1,
    Outdated = 2,
    Migrating = 3,
}

impl SchemaState {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Outdated => "outdated",
            Self::Migrating => "migrating",
        }
    }

    fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Ready,
            2 => Self::Outdated,
            3 => Self::Migrating,
            _ => Self::Pending,
        }
    }
}

pub struct SchemaWatcher {
    state: Arc<AtomicU8>,
}

impl SchemaWatcher {
    pub fn spawn(
        graph: ArrowClickHouseClient,
        embedded_version: u32,
        expected_table_names: Vec<String>,
        poll_interval: Duration,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let state = Arc::new(AtomicU8::new(SchemaState::Pending as u8));
        register_state_gauge(state.clone());

        tokio::spawn(watch_loop(
            graph,
            embedded_version,
            expected_table_names,
            poll_interval,
            shutdown,
            state.clone(),
        ));

        Arc::new(Self { state })
    }

    pub fn current(&self) -> SchemaState {
        SchemaState::from_raw(self.state.load(Ordering::Relaxed))
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn for_state(state: SchemaState) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(AtomicU8::new(state as u8)),
        })
    }
}

async fn watch_loop(
    graph: ArrowClickHouseClient,
    embedded_version: u32,
    expected_table_names: Vec<String>,
    poll_interval: Duration,
    shutdown: CancellationToken,
    state: Arc<AtomicU8>,
) {
    info!(
        embedded_version,
        poll_interval_secs = poll_interval.as_secs(),
        "schema version watcher started"
    );

    loop {
        match read_schema_state(&graph, embedded_version, &expected_table_names).await {
            Ok(next_state) => transition(&state, next_state),
            Err(error) => {
                warn!(%error, "failed to check serving schema — keeping previous state");
            }
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = sleep(poll_interval) => {}
        }
    }
}

async fn read_schema_state(
    graph: &ArrowClickHouseClient,
    reader_version: u32,
    expected_table_names: &[String],
) -> Result<SchemaState, SchemaVersionError> {
    let schemas = read_all_versions(graph).await?;

    let reader_status = schemas
        .iter()
        .find(|schema| schema.version == reader_version)
        .map(|schema| schema.status.as_str());

    let newer_schema_is_active = schemas
        .iter()
        .any(|schema| schema.version > reader_version && schema.status == STATUS_ACTIVE);

    let serving_state = match reader_status {
        Some(STATUS_ACTIVE) => SchemaState::Ready,
        Some(STATUS_RETIRED) if newer_schema_is_active => SchemaState::Outdated,
        Some(STATUS_MIGRATING) => return Ok(SchemaState::Migrating),
        _ => return Ok(SchemaState::Pending),
    };

    let tables_exist = version_tables_complete(graph, reader_version, expected_table_names).await?;

    if !tables_exist {
        return Ok(SchemaState::Pending);
    }

    Ok(serving_state)
}

fn transition(state: &Arc<AtomicU8>, next: SchemaState) {
    let prior = state.swap(next as u8, Ordering::Relaxed);
    if prior != next as u8 {
        info!(
            from = SchemaState::from_raw(prior).as_label(),
            to = next.as_label(),
            "schema watcher state transition"
        );
    }
}

fn register_state_gauge(state: Arc<AtomicU8>) {
    use orbit_observability::server::schema_watcher as spec;
    let meter = orbit_observability::meter();
    spec::STATE.build_observable_gauge_i64(&meter, move |observer| {
        let raw = state.load(Ordering::Relaxed);
        for schema_state in [
            SchemaState::Pending,
            SchemaState::Ready,
            SchemaState::Outdated,
            SchemaState::Migrating,
        ] {
            let value = i64::from(schema_state as u8 == raw);
            observer.observe(
                value,
                &[KeyValue::new(spec::labels::STATE, schema_state.as_label())],
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_raw_round_trip() {
        for schema_state in [
            SchemaState::Pending,
            SchemaState::Ready,
            SchemaState::Outdated,
            SchemaState::Migrating,
        ] {
            assert_eq!(SchemaState::from_raw(schema_state as u8), schema_state);
        }
    }

    #[test]
    fn from_raw_invalid_falls_back_to_pending() {
        assert_eq!(SchemaState::from_raw(99), SchemaState::Pending);
    }

    #[test]
    fn transition_updates_state() {
        let state = Arc::new(AtomicU8::new(SchemaState::Pending as u8));
        transition(&state, SchemaState::Ready);
        assert_eq!(
            SchemaState::from_raw(state.load(Ordering::Relaxed)),
            SchemaState::Ready
        );
    }
}

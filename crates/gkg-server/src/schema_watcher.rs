use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use indexer::schema::version::{read_active_version, read_migrating_version};
use opentelemetry::KeyValue;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

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
        poll_interval: Duration,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let state = Arc::new(AtomicU8::new(SchemaState::Pending as u8));
        register_state_gauge(state.clone());

        tokio::spawn(watch_loop(
            graph,
            embedded_version,
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
        let (next, active) = poll_once(&graph, embedded_version, &state).await;
        transition(&state, next);

        if next == SchemaState::Outdated {
            error!(
                embedded_version,
                active_version = active,
                "active schema version exceeds binary version — \
                 binary too old, requesting shutdown"
            );
            shutdown.cancel();
            return;
        }

        tokio::select! {
            _ = shutdown.cancelled() => return,
            _ = sleep(poll_interval) => {}
        }
    }
}

async fn poll_once(
    graph: &ArrowClickHouseClient,
    embedded_version: u32,
    state: &Arc<AtomicU8>,
) -> (SchemaState, Option<u32>) {
    let active = match read_active_version(graph).await {
        Ok(active) => active,
        Err(e) => {
            warn!(error = %e, "failed to read active schema version — keeping previous state");
            return (SchemaState::from_raw(state.load(Ordering::Relaxed)), None);
        }
    };

    if active == Some(embedded_version) {
        return (SchemaState::Ready, active);
    }

    let migrating = match read_migrating_version(graph).await {
        Ok(migrating) => migrating,
        Err(e) => {
            warn!(error = %e, "failed to read migrating schema version — keeping previous state");
            return (SchemaState::from_raw(state.load(Ordering::Relaxed)), active);
        }
    };

    (classify(active, migrating, embedded_version), active)
}

fn classify(active: Option<u32>, migrating: Option<u32>, embedded: u32) -> SchemaState {
    if active == Some(embedded) {
        return SchemaState::Ready;
    }

    // Outdated must beat Migrating: a below-active migrating row is anomalous data
    // and must not suppress the safety shutdown (consistent with the #957 downgrade guard).
    if active.is_some_and(|active| active > embedded) {
        return SchemaState::Outdated;
    }

    if migrating == Some(embedded) {
        return SchemaState::Migrating;
    }

    SchemaState::Pending
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
    use gkg_observability::server::schema_watcher as spec;
    let meter = gkg_observability::meter();
    spec::STATE.build_observable_gauge_i64(&meter, move |observer| {
        let raw = state.load(Ordering::Relaxed);
        for s in [
            SchemaState::Pending,
            SchemaState::Ready,
            SchemaState::Outdated,
            SchemaState::Migrating,
        ] {
            let value = i64::from(s as u8 == raw);
            observer.observe(value, &[KeyValue::new(spec::labels::STATE, s.as_label())]);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_active_equal_is_ready() {
        assert_eq!(classify(Some(2), None, 2), SchemaState::Ready);
        assert_eq!(classify(Some(2), Some(3), 2), SchemaState::Ready);
    }

    #[test]
    fn classify_migrating_equal_is_migrating() {
        assert_eq!(classify(Some(1), Some(2), 2), SchemaState::Migrating);
        assert_eq!(classify(None, Some(2), 2), SchemaState::Migrating);
    }

    #[test]
    fn classify_outdated_beats_migrating() {
        assert_eq!(classify(Some(5), Some(1), 1), SchemaState::Outdated);
    }

    #[test]
    fn classify_active_higher_is_outdated() {
        assert_eq!(classify(Some(3), None, 2), SchemaState::Outdated);
    }

    #[test]
    fn classify_none_is_pending() {
        assert_eq!(classify(None, None, 2), SchemaState::Pending);
    }

    #[test]
    fn classify_active_lower_is_pending() {
        assert_eq!(classify(Some(1), None, 2), SchemaState::Pending);
    }

    #[test]
    fn from_raw_round_trip() {
        for s in [
            SchemaState::Pending,
            SchemaState::Ready,
            SchemaState::Outdated,
            SchemaState::Migrating,
        ] {
            assert_eq!(SchemaState::from_raw(s as u8), s);
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

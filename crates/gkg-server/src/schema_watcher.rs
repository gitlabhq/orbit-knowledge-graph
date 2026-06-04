use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, Ordering};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use indexer::schema::version::read_active_version;
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
}

impl SchemaState {
    pub fn as_label(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Ready => "ready",
            Self::Outdated => "outdated",
        }
    }

    fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Ready,
            2 => Self::Outdated,
            _ => Self::Pending,
        }
    }
}

pub struct SchemaWatcher {
    state: Arc<AtomicU8>,
    active: Arc<AtomicU32>,
}

impl SchemaWatcher {
    pub fn spawn(
        graph: ArrowClickHouseClient,
        embedded_version: u32,
        poll_interval: Duration,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        let state = Arc::new(AtomicU8::new(SchemaState::Pending as u8));
        let active = Arc::new(AtomicU32::new(0));
        register_state_gauge(state.clone());

        tokio::spawn(watch_loop(
            graph,
            embedded_version,
            poll_interval,
            shutdown,
            state.clone(),
            active.clone(),
        ));

        Arc::new(Self { state, active })
    }

    pub fn current(&self) -> SchemaState {
        SchemaState::from_raw(self.state.load(Ordering::Relaxed))
    }

    pub fn active_version(&self) -> Option<u32> {
        match self.active.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn for_state(state: SchemaState) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(AtomicU8::new(state as u8)),
            active: Arc::new(AtomicU32::new(0)),
        })
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn for_active_version(state: SchemaState, active: u32) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(AtomicU8::new(state as u8)),
            active: Arc::new(AtomicU32::new(active)),
        })
    }
}

async fn watch_loop(
    graph: ArrowClickHouseClient,
    embedded_version: u32,
    poll_interval: Duration,
    shutdown: CancellationToken,
    state: Arc<AtomicU8>,
    active: Arc<AtomicU32>,
) {
    info!(
        embedded_version,
        poll_interval_secs = poll_interval.as_secs(),
        "schema version watcher started"
    );

    loop {
        let (next, active_version) = poll_once(&graph, embedded_version, &state, &active).await;
        transition(&state, next);
        active.store(active_version.unwrap_or(0), Ordering::Relaxed);

        if next == SchemaState::Outdated {
            error!(
                embedded_version,
                active_version,
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
    active: &Arc<AtomicU32>,
) -> (SchemaState, Option<u32>) {
    match read_active_version(graph).await {
        Ok(Some(version)) => (classify(version, embedded_version), Some(version)),
        Ok(None) => (SchemaState::Pending, None),
        Err(e) => {
            warn!(error = %e, "failed to read active schema version — keeping previous state");
            let previous = match active.load(Ordering::Relaxed) {
                0 => None,
                v => Some(v),
            };
            (
                SchemaState::from_raw(state.load(Ordering::Relaxed)),
                previous,
            )
        }
    }
}

fn classify(active: u32, embedded: u32) -> SchemaState {
    use std::cmp::Ordering::*;
    match active.cmp(&embedded) {
        Equal => SchemaState::Ready,
        Less => SchemaState::Pending,
        Greater => SchemaState::Outdated,
    }
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
    fn classify_equal_is_ready() {
        assert_eq!(classify(2, 2), SchemaState::Ready);
    }

    #[test]
    fn classify_active_lower_is_pending() {
        assert_eq!(classify(1, 2), SchemaState::Pending);
    }

    #[test]
    fn classify_active_higher_is_outdated() {
        assert_eq!(classify(3, 2), SchemaState::Outdated);
    }

    #[test]
    fn from_raw_round_trip() {
        for s in [
            SchemaState::Pending,
            SchemaState::Ready,
            SchemaState::Outdated,
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

    #[test]
    fn active_version_unknown_is_none() {
        let watcher = SchemaWatcher::for_state(SchemaState::Pending);
        assert_eq!(watcher.active_version(), None);
    }

    #[test]
    fn active_version_reports_set_value() {
        let watcher = SchemaWatcher::for_active_version(SchemaState::Ready, 7);
        assert_eq!(watcher.active_version(), Some(7));
    }
}

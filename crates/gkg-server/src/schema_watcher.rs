use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use indexer::schema::version::read_active_version;
use opentelemetry::KeyValue;
use opentelemetry::global;
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
    embedded_version: u32,
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

        Arc::new(Self {
            state,
            embedded_version,
        })
    }

    pub fn current(&self) -> SchemaState {
        SchemaState::from_raw(self.state.load(Ordering::Relaxed))
    }

    pub fn embedded_version(&self) -> u32 {
        self.embedded_version
    }

    #[cfg(any(test, feature = "testkit"))]
    pub fn for_state(state: SchemaState, embedded_version: u32) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(AtomicU8::new(state as u8)),
            embedded_version,
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
    match read_active_version(graph).await {
        Ok(Some(active)) => (classify(active, embedded_version), Some(active)),
        Ok(None) => (SchemaState::Pending, None),
        Err(e) => {
            warn!(error = %e, "failed to read active schema version — keeping previous state");
            (SchemaState::from_raw(state.load(Ordering::Relaxed)), None)
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
    let meter = global::meter("gkg_webserver_schema");
    meter
        .i64_observable_gauge("gkg.webserver.schema.state")
        .with_description(
            "Webserver readiness gate state; 1 indicates the active state per `state` label",
        )
        .with_callback(move |observer| {
            let raw = state.load(Ordering::Relaxed);
            for s in [
                SchemaState::Pending,
                SchemaState::Ready,
                SchemaState::Outdated,
            ] {
                let value = i64::from(s as u8 == raw);
                observer.observe(value, &[KeyValue::new("state", s.as_label())]);
            }
        })
        .build();
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
}

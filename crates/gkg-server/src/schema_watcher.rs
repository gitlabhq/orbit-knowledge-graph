use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use indexer::schema::version::{read_active_version, read_all_versions};
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
    let active = match read_active_version(graph).await {
        Ok(v) => v,
        Err(e) => {
            warn!(error = %e, "failed to read active schema version — keeping previous state");
            return (SchemaState::from_raw(state.load(Ordering::Relaxed)), None);
        }
    };

    // If our version is the active one, we're ready (common case).
    if active == Some(embedded_version) {
        return (SchemaState::Ready, active);
    }

    // If active is ahead of us, check whether our tables still exist
    // (retained from a previous migration). This enables emergency
    // rollback: deploy an older binary and it serves from its own tables
    // without requiring manual gkg_schema_version changes.
    if let Some(active_v) = active {
        if active_v > embedded_version {
            match version_exists(graph, embedded_version).await {
                true => {
                    info!(
                        embedded_version,
                        active_version = active_v,
                        "active version is ahead but our tables still exist — serving in rollback mode"
                    );
                    return (SchemaState::Ready, active);
                }
                false => return (SchemaState::Outdated, active),
            }
        }
    }

    // Active is behind us — migration hasn't promoted yet.
    (SchemaState::Pending, active)
}

/// Check if a version's tables exist by looking for it in gkg_schema_version
/// with any status (active, migrating, or retired — all mean tables exist).
async fn version_exists(graph: &ArrowClickHouseClient, version: u32) -> bool {
    match read_all_versions(graph).await {
        Ok(entries) => entries.iter().any(|e| e.version == version),
        Err(_) => false,
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

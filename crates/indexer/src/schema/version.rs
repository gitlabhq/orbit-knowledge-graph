// Re-exports from orbit-migrations for backward compatibility with integration tests.
pub use orbit_migrations::version::*;

use std::time::Duration;

use thiserror::Error;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::retry::{Backoff, LocalRetry, Step, drive_until};

const MAX_BACKOFF_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaReadiness {
    Ready,
    Pending,
    Outdated,
}

fn classify_readiness(
    active: Option<u32>,
    migrating: Option<u32>,
    embedded: u32,
) -> SchemaReadiness {
    if active == Some(embedded) || migrating == Some(embedded) {
        return SchemaReadiness::Ready;
    }
    if let Some(active_version) = active
        && active_version > embedded
    {
        return SchemaReadiness::Outdated;
    }
    SchemaReadiness::Pending
}

#[derive(Debug, Error)]
pub enum SchemaWaitError {
    #[error(
        "timed out after {seconds}s waiting for schema version {target} to become ready \
         (last seen active={active:?}, migrating={migrating:?})"
    )]
    Timeout {
        target: u32,
        seconds: u64,
        active: Option<u32>,
        migrating: Option<u32>,
    },

    #[error(
        "binary schema version {embedded} is older than the active version {active}; \
         binary is outdated and must not process"
    )]
    Outdated { embedded: u32, active: u32 },
}

pub async fn init(
    graph: &clickhouse_client::ArrowClickHouseClient,
) -> Result<(), SchemaVersionError> {
    ensure_version_table(graph).await
}

pub async fn wait_until_ready(
    graph: &clickhouse_client::ArrowClickHouseClient,
    target_version: u32,
    timeout: Duration,
    poll_interval: Duration,
) -> Result<(), SchemaWaitError> {
    info!(
        target_version,
        timeout_secs = timeout.as_secs(),
        "waiting for schema version to become ready"
    );

    let deadline = Instant::now() + timeout;
    let policy = LocalRetry {
        backoff: Backoff::Exponential {
            base: poll_interval,
            cap: MAX_BACKOFF_INTERVAL,
        },
        max_attempts: u32::MAX,
    };

    drive_until(
        &policy,
        deadline,
        (None, None),
        |_carried, _attempt| async move {
            let active = read_active_version(graph).await.unwrap_or_else(|error| {
                warn!(%error, "failed to read active schema version — retrying");
                None
            });
            let migrating = read_migrating_version(graph).await.unwrap_or_else(|error| {
                warn!(%error, "failed to read migrating schema version — retrying");
                None
            });

            match classify_readiness(active, migrating, target_version) {
                SchemaReadiness::Ready => {
                    info!(target_version, "schema version is ready — proceeding");
                    Step::Done(())
                }
                SchemaReadiness::Outdated => Step::GiveUp(SchemaWaitError::Outdated {
                    embedded: target_version,
                    active: active.expect("Outdated requires a known active version"),
                }),
                SchemaReadiness::Pending => {
                    info!(
                        target_version,
                        active_version = ?active,
                        migrating_version = ?migrating,
                        "schema version not ready yet — dispatcher has not prepared it"
                    );
                    Step::Retry((active, migrating))
                }
            }
        },
        |(active, migrating)| SchemaWaitError::Timeout {
            target: target_version,
            seconds: timeout.as_secs(),
            active: *active,
            migrating: *migrating,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_classification() {
        assert_eq!(classify_readiness(Some(2), None, 2), SchemaReadiness::Ready);
        assert_eq!(
            classify_readiness(Some(1), Some(2), 2),
            SchemaReadiness::Ready
        );
        assert_eq!(classify_readiness(None, None, 2), SchemaReadiness::Pending);
        assert_eq!(
            classify_readiness(Some(3), None, 2),
            SchemaReadiness::Outdated
        );
        assert_eq!(
            classify_readiness(Some(3), Some(2), 2),
            SchemaReadiness::Ready
        );
    }
}

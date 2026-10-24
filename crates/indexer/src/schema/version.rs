// Re-exports from orbit-migrations for backward compatibility with integration tests.
pub use orbit_migrations::version::*;

use std::time::Duration;

use orbit_log_fields::{info, warn};
use thiserror::Error;
use tokio::time::Instant;

use crate::retry::{Backoff, LocalRetry, Step, drive_until};

const MAX_BACKOFF_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaReadiness {
    Ready,
    Pending,
    Outdated { active: u32 },
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
        return SchemaReadiness::Outdated {
            active: active_version,
        };
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
        safe(SCHEMA_VERSION) = target_version,
        safe(TIMEOUT_S) = timeout.as_secs_f64(),
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
                warn!(
                    safe(ERROR_TYPE) = <&str>::from(&error),
                    expanded(ERROR_MESSAGE) = %error,
                    "failed to read active schema version — retrying"
                );
                None
            });
            let migrating = read_migrating_version(graph).await.unwrap_or_else(|error| {
                warn!(
                    safe(ERROR_TYPE) = <&str>::from(&error),
                    expanded(ERROR_MESSAGE) = %error,
                    "failed to read migrating schema version — retrying"
                );
                None
            });

            match classify_readiness(active, migrating, target_version) {
                SchemaReadiness::Ready => {
                    info!(
                        safe(SCHEMA_VERSION) = target_version,
                        "schema version is ready — proceeding"
                    );
                    Step::Done(())
                }
                SchemaReadiness::Outdated { active } => Step::GiveUp(SchemaWaitError::Outdated {
                    embedded: target_version,
                    active,
                }),
                SchemaReadiness::Pending => {
                    info!(
                        safe(SCHEMA_VERSION) = target_version,
                        safe(ACTIVE_SCHEMA_VERSION) = active,
                        safe(MIGRATING_SCHEMA_VERSION) = migrating,
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
            SchemaReadiness::Outdated { active: 3 }
        );
        assert_eq!(
            classify_readiness(Some(3), Some(2), 2),
            SchemaReadiness::Ready
        );
    }
}

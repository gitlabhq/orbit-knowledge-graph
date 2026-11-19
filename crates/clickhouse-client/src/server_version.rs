//! The oldest ClickHouse server that the graph schema and the compiled SQL run on.

use std::fmt;
use std::time::Duration;

use orbit_log_fields::warn;
use orbit_server_config::ClickHouseConfiguration;
use thiserror::Error;

/// ClickHouse 26.4 added `WITH name AS MATERIALIZED (...)`, which the query compiler emits.
pub const MINIMUM_SERVER_VERSION: ServerVersion = ServerVersion {
    major: 26,
    minor: 4,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ServerVersion {
    pub major: u32,
    pub minor: u32,
}

impl fmt::Display for ServerVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum ServerVersionError {
    #[error("ClickHouse {found} is not supported: Orbit requires ClickHouse {required} or later")]
    #[strum(serialize = "clickhouse_version_below_minimum")]
    BelowMinimum {
        found: String,
        required: ServerVersion,
    },

    #[error("ClickHouse reported version {0:?}, which does not start with MAJOR.MINOR")]
    #[strum(serialize = "clickhouse_version_unparsable")]
    Unparsable(String),

    #[error("could not read the ClickHouse server version: {0}")]
    #[strum(serialize = "clickhouse_version_unreadable")]
    Unreadable(#[from] clickhouse::error::Error),
}

/// Parses the text of `SELECT version()` and rejects a server below [`MINIMUM_SERVER_VERSION`].
pub fn check_server_version(reported: &str) -> Result<ServerVersion, ServerVersionError> {
    let version = parse_server_version(reported)
        .ok_or_else(|| ServerVersionError::Unparsable(reported.to_string()))?;

    if version < MINIMUM_SERVER_VERSION {
        return Err(ServerVersionError::BelowMinimum {
            found: reported.to_string(),
            required: MINIMUM_SERVER_VERSION,
        });
    }
    Ok(version)
}

fn parse_server_version(reported: &str) -> Option<ServerVersion> {
    let mut parts = reported.trim().split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    Some(ServerVersion { major, minor })
}

/// Reads `SELECT version()` without the session settings, which an older server rejects
/// with `UNKNOWN_SETTING` before it answers.
pub async fn require_minimum_server_version(
    config: &ClickHouseConfiguration,
) -> Result<ServerVersion, ServerVersionError> {
    let mut client = clickhouse::Client::default()
        .with_url(&config.url)
        .with_user(&config.username);
    if let Some(password) = &config.password {
        client = client.with_password(password);
    }
    let reported: String = client.query("SELECT version()").fetch_one().await?;

    check_server_version(&reported)
}

/// Retries a server that does not answer yet, and stops on a version that Orbit cannot run on.
pub async fn wait_for_minimum_server_version(
    config: &ClickHouseConfiguration,
    retry_backoff: Duration,
) -> Result<ServerVersion, ServerVersionError> {
    loop {
        match require_minimum_server_version(config).await {
            Ok(version) => return Ok(version),
            Err(
                error @ (ServerVersionError::BelowMinimum { .. }
                | ServerVersionError::Unparsable(_)),
            ) => {
                return Err(error);
            }
            Err(error @ ServerVersionError::Unreadable(_)) => {
                warn!(
                    safe(ERROR_TYPE) = <&str>::from(&error),
                    expanded(ERROR_MESSAGE) = %error,
                    "ClickHouse version unreadable; retrying"
                );
            }
        }
        tokio::time::sleep(retry_backoff).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_cloud_and_self_managed_builds_at_or_above_the_minimum() {
        for reported in [
            "26.4.1.1",
            "26.6.1.1193",
            "26.6.8.7",
            "26.6.2.5-lts",
            "26.4.5.143.altinitystable",
            "26.10.1.1",
            "27.1.1.1",
        ] {
            assert!(check_server_version(reported).is_ok(), "{reported}");
        }
    }

    #[test]
    fn compares_the_minor_version_as_a_number() {
        assert_eq!(
            check_server_version("26.10.2.3").unwrap(),
            ServerVersion {
                major: 26,
                minor: 10
            }
        );
    }

    #[test]
    fn rejects_a_server_below_the_minimum_and_names_both_versions() {
        for reported in ["26.2.1.788", "26.3.9.1", "25.8.33.6", "25.12.1.1"] {
            let error = check_server_version(reported).unwrap_err();

            assert!(
                matches!(error, ServerVersionError::BelowMinimum { .. }),
                "{reported}: {error}"
            );
            assert_eq!(
                error.to_string(),
                format!(
                    "ClickHouse {reported} is not supported: Orbit requires ClickHouse 26.4 or later"
                )
            );
        }
    }

    #[test]
    fn rejects_text_without_a_major_and_minor_version() {
        for reported in ["", "26", "v26.6.1", "26.x.1", "latest"] {
            assert!(
                matches!(
                    check_server_version(reported),
                    Err(ServerVersionError::Unparsable(_))
                ),
                "{reported}"
            );
        }
    }

    #[test]
    fn below_minimum_has_a_fixed_error_type() {
        let error = check_server_version("26.2.1.788").unwrap_err();

        assert_eq!(<&str>::from(&error), "clickhouse_version_below_minimum");
    }

    #[tokio::test]
    async fn waiting_keeps_retrying_while_the_server_does_not_answer() {
        let mut config = orbit_server_config::AppConfig::embedded_defaults().graph;
        config.url = "http://127.0.0.1:1".to_string();

        let waited = tokio::time::timeout(
            Duration::from_millis(300),
            wait_for_minimum_server_version(&config, Duration::from_millis(10)),
        )
        .await;

        assert!(waited.is_err(), "{waited:?}");
    }
}

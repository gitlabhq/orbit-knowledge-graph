//! The connections a deployment opens next to its ClickHouse graph, by the name packages give them.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ConnectionsConfig {
    /// DuckDB connections that read their tables from S3.
    #[serde(default)]
    pub duckdb: BTreeMap<String, DuckDbConnection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct DuckDbConnection {
    /// Holds the `httpfs` and `ducklake` extension files in DuckDB's `<version>/<platform>/` layout.
    pub extension_directory: PathBuf,
    pub s3: S3Endpoint,
    pub credentials: S3Credentials,
    pub location: DuckDbLocation,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct S3Endpoint {
    /// `host:port`, without a scheme.
    pub endpoint: String,
    pub region: String,
    pub url_style: S3UrlStyle,
    pub use_ssl: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3UrlStyle {
    Path,
    Vhost,
}

impl S3UrlStyle {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Path => "path",
            Self::Vhost => "vhost",
        }
    }
}

/// Static S3 keys, meant for secret files under
/// `/etc/secrets/connections/duckdb/<name>/credentials/`.
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(deny_unknown_fields)]
pub struct S3Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl fmt::Debug for S3Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Credentials")
            .field("access_key_id", &self.access_key_id)
            .finish_non_exhaustive()
    }
}

/// Where the connection's tables live. The package names the tables either way.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum DuckDbLocation {
    /// Each table reads the Parquet files under `s3://<bucket>/<prefix><table>/`.
    Parquet { bucket: String, prefix: String },
    /// Each table reads through the DuckLake catalog, whose data sits under `s3://<bucket>/<prefix>`.
    Ducklake {
        catalog: PathBuf,
        bucket: String,
        prefix: String,
    },
}

impl DuckDbLocation {
    /// The `s3://<bucket>/<prefix>` the tables sit under.
    pub fn root(&self) -> String {
        let (Self::Parquet { bucket, prefix } | Self::Ducklake { bucket, prefix, .. }) = self;
        format!("s3://{bucket}/{prefix}")
    }

    pub fn bucket(&self) -> &str {
        let (Self::Parquet { bucket, .. } | Self::Ducklake { bucket, .. }) = self;
        bucket
    }

    fn prefix(&self) -> &str {
        let (Self::Parquet { prefix, .. } | Self::Ducklake { prefix, .. }) = self;
        prefix
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("connection {connection}: {problem}")]
pub struct ConnectionError {
    pub connection: String,
    pub problem: String,
}

impl DuckDbConnection {
    /// Every location is a key prefix inside one S3 bucket, and the endpoint is a bare host.
    pub fn validate(&self, name: &str) -> Result<(), ConnectionError> {
        let problem = |problem: String| ConnectionError {
            connection: name.to_string(),
            problem,
        };
        let bucket = self.location.bucket();
        let named = bucket.len() >= 3
            && bucket.len() <= 63
            && bucket
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            && !bucket.starts_with('-')
            && !bucket.ends_with('-');
        if !named {
            return Err(problem(format!(
                "bucket {bucket:?} is not an S3 bucket name"
            )));
        }
        let prefix = self.location.prefix();
        let segments: Vec<&str> = prefix.strip_suffix('/').unwrap_or("").split('/').collect();
        if segments
            .iter()
            .any(|segment| segment.is_empty() || *segment == "." || *segment == "..")
        {
            return Err(problem(format!(
                "prefix {prefix:?} must be one or more key segments, each followed by /"
            )));
        }
        let endpoint = &self.s3.endpoint;
        if endpoint.is_empty() || endpoint.contains("://") || endpoint.contains('/') {
            return Err(problem(format!(
                "endpoint {endpoint:?} must be a host or host:port"
            )));
        }
        if !self.extension_directory.is_absolute() {
            return Err(problem(
                "extension_directory must be an absolute path".into(),
            ));
        }
        if let DuckDbLocation::Ducklake { catalog, .. } = &self.location
            && !catalog.is_absolute()
        {
            return Err(problem(
                "the DuckLake catalog must be an absolute path".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(location: &str) -> Result<DuckDbConnection, String> {
        let yaml = format!(
            "extension_directory: /opt/duckdb/extensions
s3: {{endpoint: 127.0.0.1:9000, region: us-east-1, url_style: path, use_ssl: false}}
credentials: {{access_key_id: test-key, secret_access_key: test-secret}}
location: {location}
"
        );
        orbit_utils::yaml::from_str(&yaml).map_err(|error| error.to_string())
    }

    fn problem(location: &str) -> String {
        parsed(location)
            .unwrap()
            .validate("jira_lake")
            .unwrap_err()
            .to_string()
    }

    #[test]
    fn both_modes_parse_into_one_s3_root() {
        let parquet = parsed("{mode: parquet, bucket: lake, prefix: jira/}").unwrap();
        parquet.validate("jira_lake").unwrap();
        assert_eq!(parquet.location.root(), "s3://lake/jira/");
        let ducklake = parsed(
            "{mode: ducklake, catalog: /var/lib/orbit/jira.ducklake, bucket: lake, prefix: jira/data/}",
        )
        .unwrap();
        ducklake.validate("jira_lake").unwrap();
        assert_eq!(ducklake.location.root(), "s3://lake/jira/data/");
    }

    #[test]
    fn a_location_without_a_prefix_fails_to_parse() {
        let error = parsed("{mode: parquet, bucket: lake}").unwrap_err();
        assert!(error.contains("missing field `prefix`"), "{error}");
    }

    #[test]
    fn a_location_outside_a_bucket_key_prefix_fails() {
        for prefix in [
            "''",
            "jira",
            "/jira/",
            "jira//",
            "jira/../other/",
            "./jira/",
        ] {
            assert_eq!(
                problem(&format!(
                    "{{mode: parquet, bucket: lake, prefix: {prefix}}}"
                )),
                format!(
                    "connection jira_lake: prefix {:?} must be one or more key segments, each followed by /",
                    prefix.trim_matches('\'')
                ),
                "{prefix}"
            );
        }
        assert_eq!(
            problem("{mode: parquet, bucket: 'http://lake', prefix: jira/}"),
            "connection jira_lake: bucket \"http://lake\" is not an S3 bucket name"
        );
        assert_eq!(
            problem("{mode: ducklake, catalog: jira.ducklake, bucket: lake, prefix: jira/}"),
            "connection jira_lake: the DuckLake catalog must be an absolute path"
        );
    }

    #[test]
    fn an_unknown_mode_fails_to_parse() {
        let error = parsed("{mode: iceberg, bucket: lake, prefix: jira/}").unwrap_err();
        assert!(error.contains("unknown variant `iceberg`"), "{error}");
    }

    #[test]
    fn credentials_never_print_their_secret() {
        let connection = parsed("{mode: parquet, bucket: lake, prefix: jira/}").unwrap();
        let printed = format!("{connection:?}");
        assert!(!printed.contains("test-secret"), "{printed}");
        assert!(printed.contains("test-key"), "{printed}");
    }
}

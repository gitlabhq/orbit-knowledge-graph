//! Object storage configuration for AWS S3, S3-compatible stores, and Google
//! Cloud Storage. The section is optional: when `object_storage` is absent,
//! Orbit opens no store. Client construction lives in `orbit-object-storage`;
//! this module only declares the shape and validates it.

use std::path::Path;
use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const OBJECT_STORAGE_SECRET_DIR: &str = "/etc/secrets/object_storage";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct ObjectStorageConfig {
    pub provider: ObjectStorageProvider,
    pub bucket: String,
    /// Key prefix inside the bucket; every path Orbit reads or writes is placed under it.
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub s3: S3Config,
    #[serde(default)]
    pub gcs: GcsConfig,
    #[serde(default)]
    pub tls: ObjectStorageTlsConfig,
    #[serde(default)]
    pub http: ObjectStorageHttpConfig,
    #[serde(default)]
    pub retry: ObjectStorageRetryConfig,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema, strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum ObjectStorageProvider {
    /// AWS S3 or any S3-compatible store (MinIO, Ceph RGW, Hetzner, ...).
    S3,
    /// Google Cloud Storage.
    Gcs,
}

/// Settings read when `provider` is `s3`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct S3Config {
    /// Required for AWS S3. Optional for S3-compatible stores, which mostly ignore the signing region.
    pub region: Option<String>,
    /// Only for S3-compatible stores; leave unset for AWS S3.
    pub endpoint: Option<String>,
    /// Address the bucket as `endpoint/bucket/key` instead of `bucket.endpoint/key`.
    pub path_style: bool,
    pub auth: S3Auth,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub session_token: Option<String>,
    /// KMS key for SSE-KMS on writes. Unset keeps the bucket default encryption.
    pub sse_kms_key_id: Option<String>,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum S3Auth {
    /// Credentials come from the runtime: IRSA / EKS Pod Identity, ECS task role,
    /// EC2 instance profile, or `AWS_*` environment variables.
    #[default]
    Identity,
    /// Access key pair from config, secret files, or environment.
    Static,
}

/// Settings read when `provider` is `gcs`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct GcsConfig {
    pub auth: GcsAuth,
    /// Path to a service account JSON key file.
    pub service_account_key_path: Option<String>,
    /// Service account JSON key content.
    pub service_account_key: Option<String>,
    /// Alternative API base URL, for emulators or private endpoints.
    pub endpoint: Option<String>,
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    JsonSchema,
    strum::IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum GcsAuth {
    /// Credentials come from the runtime: GKE Workload Identity / GCE metadata
    /// server, or an Application Default Credentials file.
    #[default]
    Identity,
    /// Service account JSON key from a file or inline.
    ServiceAccountKey,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct ObjectStorageTlsConfig {
    /// PEM bundle of extra root certificates, for stores behind a private CA.
    pub ca_cert_path: Option<String>,
    /// Permit `http://` endpoints. Local development only.
    pub allow_http: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct ObjectStorageHttpConfig {
    pub connect_timeout_secs: u64,
    /// Per-request timeout; covers a single HTTP round trip, not a whole multipart upload.
    pub request_timeout_secs: u64,
}

impl Default for ObjectStorageHttpConfig {
    fn default() -> Self {
        Self {
            connect_timeout_secs: 5,
            request_timeout_secs: 30,
        }
    }
}

impl ObjectStorageHttpConfig {
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    pub fn request_timeout(&self) -> Duration {
        Duration::from_secs(self.request_timeout_secs)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(deny_unknown_fields)]
pub struct ObjectStorageRetryConfig {
    pub max_retries: usize,
    /// Total time budget for one operation including retries.
    pub retry_timeout_secs: u64,
    pub max_backoff_secs: u64,
}

impl Default for ObjectStorageRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 10,
            retry_timeout_secs: 180,
            max_backoff_secs: 15,
        }
    }
}

impl ObjectStorageRetryConfig {
    pub fn retry_timeout(&self) -> Duration {
        Duration::from_secs(self.retry_timeout_secs)
    }

    pub fn max_backoff(&self) -> Duration {
        Duration::from_secs(self.max_backoff_secs)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageConfigError {
    #[error("object_storage.bucket is required")]
    BucketMissing,
    #[error("object_storage.s3.region is required when object_storage.s3.endpoint is not set")]
    S3RegionMissing,
    #[error(
        "object_storage.s3.auth is static but access_key_id or secret_access_key is missing (mount at {0}/s3/access_key_id and {0}/s3/secret_access_key, or set GKG_OBJECT_STORAGE__S3__ACCESS_KEY_ID and GKG_OBJECT_STORAGE__S3__SECRET_ACCESS_KEY)",
        OBJECT_STORAGE_SECRET_DIR
    )]
    S3StaticCredentialsMissing,
    #[error(
        "object_storage.s3.auth is identity but static credentials are set; use auth: static or remove access_key_id, secret_access_key and session_token"
    )]
    S3CredentialsWithIdentityAuth,
    #[error(
        "object_storage.gcs.auth is service_account_key but neither service_account_key_path nor service_account_key is set (mount the key at {0}/gcs/service_account_key, or set service_account_key_path)",
        OBJECT_STORAGE_SECRET_DIR
    )]
    GcsServiceAccountKeyMissing,
    #[error(
        "object_storage.gcs.service_account_key_path and object_storage.gcs.service_account_key are both set; use one"
    )]
    GcsServiceAccountKeyAmbiguous,
    #[error(
        "object_storage.gcs.auth is identity but a service account key is set; use auth: service_account_key or remove it"
    )]
    GcsKeyWithIdentityAuth,
    #[error("object_storage.{section} is configured but object_storage.provider is {provider}")]
    UnusedProviderSection {
        section: &'static str,
        provider: &'static str,
    },
    #[error("{field} must start with http:// or https://, got '{endpoint}'")]
    EndpointScheme {
        field: &'static str,
        endpoint: String,
    },
    #[error(
        "{field} is plaintext http ('{endpoint}'); set object_storage.tls.allow_http: true, which is meant for local development only"
    )]
    PlaintextEndpoint {
        field: &'static str,
        endpoint: String,
    },
    #[error("{field}: file not found at '{path}'")]
    FileNotFound { field: &'static str, path: String },
}

impl ObjectStorageConfig {
    pub fn validate(&self) -> Result<(), ObjectStorageConfigError> {
        if self.bucket.trim().is_empty() {
            return Err(ObjectStorageConfigError::BucketMissing);
        }
        match self.provider {
            ObjectStorageProvider::S3 => {
                reject_unused_section("gcs", self.provider, self.gcs == GcsConfig::default())?;
                self.s3.validate(&self.tls)?;
            }
            ObjectStorageProvider::Gcs => {
                reject_unused_section("s3", self.provider, self.s3 == S3Config::default())?;
                self.gcs.validate(&self.tls)?;
            }
        }
        self.tls.validate()
    }
}

fn reject_unused_section(
    section: &'static str,
    provider: ObjectStorageProvider,
    is_default: bool,
) -> Result<(), ObjectStorageConfigError> {
    if is_default {
        return Ok(());
    }
    Err(ObjectStorageConfigError::UnusedProviderSection {
        section,
        provider: provider.into(),
    })
}

impl S3Config {
    fn validate(&self, tls: &ObjectStorageTlsConfig) -> Result<(), ObjectStorageConfigError> {
        if let Some(endpoint) = &self.endpoint {
            validate_endpoint("object_storage.s3.endpoint", endpoint, tls.allow_http)?;
        }
        if self.region.is_none() && self.endpoint.is_none() {
            return Err(ObjectStorageConfigError::S3RegionMissing);
        }
        let has_static = [
            &self.access_key_id,
            &self.secret_access_key,
            &self.session_token,
        ]
        .iter()
        .any(|value| is_set(value));
        match self.auth {
            S3Auth::Static if !is_set(&self.access_key_id) || !is_set(&self.secret_access_key) => {
                Err(ObjectStorageConfigError::S3StaticCredentialsMissing)
            }
            S3Auth::Identity if has_static => {
                Err(ObjectStorageConfigError::S3CredentialsWithIdentityAuth)
            }
            _ => Ok(()),
        }
    }
}

impl GcsConfig {
    fn validate(&self, tls: &ObjectStorageTlsConfig) -> Result<(), ObjectStorageConfigError> {
        if let Some(endpoint) = &self.endpoint {
            validate_endpoint("object_storage.gcs.endpoint", endpoint, tls.allow_http)?;
        }
        let has_path = is_set(&self.service_account_key_path);
        let has_inline = is_set(&self.service_account_key);
        match self.auth {
            GcsAuth::ServiceAccountKey if has_path && has_inline => {
                return Err(ObjectStorageConfigError::GcsServiceAccountKeyAmbiguous);
            }
            GcsAuth::ServiceAccountKey if !has_path && !has_inline => {
                return Err(ObjectStorageConfigError::GcsServiceAccountKeyMissing);
            }
            GcsAuth::Identity if has_path || has_inline => {
                return Err(ObjectStorageConfigError::GcsKeyWithIdentityAuth);
            }
            _ => {}
        }
        require_file(
            "object_storage.gcs.service_account_key_path",
            self.service_account_key_path.as_deref(),
        )
    }
}

impl ObjectStorageTlsConfig {
    fn validate(&self) -> Result<(), ObjectStorageConfigError> {
        require_file(
            "object_storage.tls.ca_cert_path",
            self.ca_cert_path.as_deref(),
        )
    }
}

fn is_set(value: &Option<String>) -> bool {
    value.as_deref().is_some_and(|v| !v.trim().is_empty())
}

fn validate_endpoint(
    field: &'static str,
    endpoint: &str,
    allow_http: bool,
) -> Result<(), ObjectStorageConfigError> {
    if endpoint.starts_with("https://") {
        return Ok(());
    }
    if !endpoint.starts_with("http://") {
        return Err(ObjectStorageConfigError::EndpointScheme {
            field,
            endpoint: endpoint.to_string(),
        });
    }
    if allow_http {
        return Ok(());
    }
    Err(ObjectStorageConfigError::PlaintextEndpoint {
        field,
        endpoint: endpoint.to_string(),
    })
}

fn require_file(field: &'static str, path: Option<&str>) -> Result<(), ObjectStorageConfigError> {
    match path {
        Some(p) if !Path::new(p).exists() => Err(ObjectStorageConfigError::FileNotFound {
            field,
            path: p.to_string(),
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use config::{File, FileFormat};

    use super::*;
    use crate::app::AppConfig;
    use crate::secret_file_source::SecretFileSource;

    fn parse(yaml: &str) -> ObjectStorageConfig {
        orbit_utils::yaml::from_str(yaml).expect("valid object_storage yaml")
    }

    fn gcs_identity() -> ObjectStorageConfig {
        parse("provider: gcs\nbucket: gitlab-orbit-stg-storage\n")
    }

    fn s3_static_minio(allow_http: bool) -> ObjectStorageConfig {
        parse(&format!(
            r#"
provider: s3
bucket: orbit-probe
prefix: dev/
s3:
  endpoint: http://localhost:9010
  path_style: true
  auth: static
  access_key_id: example-key-id
  secret_access_key: example-secret
tls:
  allow_http: {allow_http}
"#
        ))
    }

    #[test]
    fn gcs_identity_needs_only_provider_and_bucket() {
        let cfg = gcs_identity();
        assert_eq!(cfg.provider, ObjectStorageProvider::Gcs);
        assert_eq!(cfg.gcs.auth, GcsAuth::Identity);
        assert_eq!(cfg.prefix, None);
        cfg.validate().expect("identity auth needs no credentials");
    }

    #[test]
    fn defaults_match_object_store_client_defaults() {
        let cfg = gcs_identity();
        assert_eq!(cfg.http.connect_timeout(), Duration::from_secs(5));
        assert_eq!(cfg.http.request_timeout(), Duration::from_secs(30));
        assert_eq!(cfg.retry.max_retries, 10);
        assert_eq!(cfg.retry.retry_timeout(), Duration::from_secs(180));
        assert_eq!(cfg.retry.max_backoff(), Duration::from_secs(15));
        assert!(!cfg.tls.allow_http);
        assert!(!cfg.s3.path_style);
    }

    #[test]
    fn s3_static_with_endpoint_parses_and_validates() {
        let cfg = s3_static_minio(true);
        assert_eq!(cfg.s3.auth, S3Auth::Static);
        assert_eq!(cfg.s3.endpoint.as_deref(), Some("http://localhost:9010"));
        assert_eq!(cfg.prefix.as_deref(), Some("dev/"));
        cfg.validate()
            .expect("endpoint replaces region for S3-compatible stores");
    }

    #[test]
    fn plaintext_endpoint_requires_allow_http() {
        let err = s3_static_minio(false).validate().unwrap_err();
        assert!(matches!(
            err,
            ObjectStorageConfigError::PlaintextEndpoint {
                field: "object_storage.s3.endpoint",
                ..
            }
        ));
    }

    #[test]
    fn endpoint_without_scheme_is_rejected() {
        let mut cfg = s3_static_minio(true);
        cfg.s3.endpoint = Some("fsn1.your-objectstorage.com".into());
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::EndpointScheme { .. }
        ));
    }

    #[test]
    fn aws_s3_without_endpoint_requires_region() {
        let cfg = parse("provider: s3\nbucket: b\n");
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::S3RegionMissing
        ));
        let cfg = parse("provider: s3\nbucket: b\ns3:\n  region: us-east-1\n");
        cfg.validate()
            .expect("identity auth on AWS needs only a region");
    }

    #[test]
    fn s3_static_auth_requires_both_keys() {
        let cfg = parse(
            "provider: s3\nbucket: b\ns3:\n  region: r\n  auth: static\n  access_key_id: k\n",
        );
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::S3StaticCredentialsMissing
        ));
    }

    #[test]
    fn s3_identity_auth_rejects_static_keys() {
        let cfg = parse("provider: s3\nbucket: b\ns3:\n  region: r\n  session_token: t\n");
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::S3CredentialsWithIdentityAuth
        ));
    }

    #[test]
    fn gcs_service_account_key_requires_exactly_one_source() {
        let neither = parse("provider: gcs\nbucket: b\ngcs:\n  auth: service_account_key\n");
        assert!(matches!(
            neither.validate().unwrap_err(),
            ObjectStorageConfigError::GcsServiceAccountKeyMissing
        ));
        let both = parse(
            "provider: gcs\nbucket: b\ngcs:\n  auth: service_account_key\n  service_account_key_path: /k.json\n  service_account_key: '{}'\n",
        );
        assert!(matches!(
            both.validate().unwrap_err(),
            ObjectStorageConfigError::GcsServiceAccountKeyAmbiguous
        ));
        let inline = parse(
            "provider: gcs\nbucket: b\ngcs:\n  auth: service_account_key\n  service_account_key: '{}'\n",
        );
        inline.validate().expect("inline key is a complete source");
    }

    #[test]
    fn gcs_identity_auth_rejects_service_account_key() {
        let cfg = parse("provider: gcs\nbucket: b\ngcs:\n  service_account_key: '{}'\n");
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::GcsKeyWithIdentityAuth
        ));
    }

    #[test]
    fn referenced_files_must_exist() {
        let mut cfg = gcs_identity();
        cfg.tls.ca_cert_path = Some("/nonexistent/ca.pem".into());
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::FileNotFound {
                field: "object_storage.tls.ca_cert_path",
                ..
            }
        ));
        let cfg = parse(
            "provider: gcs\nbucket: b\ngcs:\n  auth: service_account_key\n  service_account_key_path: /nonexistent/key.json\n",
        );
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::FileNotFound {
                field: "object_storage.gcs.service_account_key_path",
                ..
            }
        ));
    }

    #[test]
    fn section_for_the_other_provider_is_rejected() {
        let cfg = parse("provider: gcs\nbucket: b\ns3:\n  region: us-east-1\n");
        assert!(matches!(
            cfg.validate().unwrap_err(),
            ObjectStorageConfigError::UnusedProviderSection {
                section: "s3",
                provider: "gcs"
            }
        ));
    }

    #[test]
    fn app_config_without_section_has_no_object_storage() {
        let app: AppConfig =
            orbit_utils::yaml::from_str("bind_address: \"127.0.0.1:4200\"\n").unwrap();
        assert!(app.object_storage.is_none());
    }

    #[test]
    fn app_config_accepts_string_overrides_for_typed_fields() {
        // Secret files and env vars arrive as strings; bools, numbers and enums must still coerce.
        let yaml = "object_storage:\n  provider: gcs\n  bucket: b\n";
        let app: AppConfig = config::Config::builder()
            .add_source(File::from_str(yaml, FileFormat::Yaml))
            .set_override("object_storage.provider", "s3")
            .unwrap()
            .set_override("object_storage.s3.region", "eu-central-1")
            .unwrap()
            .set_override("object_storage.s3.path_style", "true")
            .unwrap()
            .set_override("object_storage.retry.max_retries", "3")
            .unwrap()
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        let cfg = app.object_storage.expect("section present");
        assert_eq!(cfg.provider, ObjectStorageProvider::S3);
        assert!(cfg.s3.path_style);
        assert_eq!(cfg.retry.max_retries, 3);
        cfg.validate().unwrap();
    }

    #[test]
    fn secret_files_populate_nested_credentials() {
        let dir = tempfile::tempdir().unwrap();
        let s3_dir = dir.path().join("object_storage/s3");
        std::fs::create_dir_all(&s3_dir).unwrap();
        std::fs::write(s3_dir.join("access_key_id"), "AKIA-test\n").unwrap();
        std::fs::write(s3_dir.join("secret_access_key"), "shh\n").unwrap();
        let yaml = "object_storage:\n  provider: s3\n  bucket: b\n  s3:\n    region: us-east-1\n    auth: static\n";
        let app: AppConfig = config::Config::builder()
            .add_source(File::from_str(yaml, FileFormat::Yaml))
            .add_source(SecretFileSource::new(dir.path().to_str().unwrap()))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        let cfg = app.object_storage.expect("section present");
        assert_eq!(cfg.s3.access_key_id.as_deref(), Some("AKIA-test"));
        assert_eq!(cfg.s3.secret_access_key.as_deref(), Some("shh"));
        cfg.validate().unwrap();
    }
}

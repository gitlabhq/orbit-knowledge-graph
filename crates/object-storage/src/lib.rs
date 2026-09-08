//! Builds an [`object_store`] client from Orbit's `object_storage` config section.
//!
//! The config crate owns the shape and validation; this crate owns the mapping
//! onto `object_store` builders so that no other crate needs to know which
//! provider is behind the bucket.

use std::sync::Arc;

use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path;
use object_store::prefix::PrefixStore;
use object_store::{
    BackoffConfig, Certificate, ClientConfigKey, ClientOptions, ObjectStore, RetryConfig,
};
use orbit_server_config::{
    GcsAuth, GcsConfig, ObjectStorageConfig, ObjectStorageConfigError, ObjectStorageProvider,
    S3Auth, S3Config,
};

const USER_AGENT: &str = concat!("gitlab-orbit/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, thiserror::Error)]
pub enum ObjectStorageError {
    #[error(transparent)]
    Config(#[from] ObjectStorageConfigError),
    #[error("object_storage.tls.ca_cert_path: cannot read '{path}': {source}")]
    ReadCaBundle {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("object_storage.tls.ca_cert_path: no PEM certificates found in '{path}'")]
    EmptyCaBundle { path: String },
    #[error(
        "object_storage.s3.endpoint '{endpoint}' must be a service endpoint without the bucket host"
    )]
    EndpointContainsBucket { endpoint: String },
    #[error(transparent)]
    Store(#[from] object_store::Error),
}

/// Validates the config and returns a store rooted at `object_storage.prefix`.
pub fn build_store(
    config: &ObjectStorageConfig,
) -> Result<Arc<dyn ObjectStore>, ObjectStorageError> {
    config.validate()?;
    let options = client_options(config)?;
    let retry = retry_config(config);
    let prefix = config.prefix.as_deref().filter(|p| !p.is_empty());
    match config.provider {
        ObjectStorageProvider::S3 => {
            let store = build_s3(&config.s3, &config.bucket, options, retry)?;
            Ok(with_prefix(store, prefix))
        }
        ObjectStorageProvider::Gcs => {
            let store = build_gcs(&config.gcs, &config.bucket, options, retry)?;
            Ok(with_prefix(store, prefix))
        }
    }
}

fn with_prefix<T: ObjectStore>(store: T, prefix: Option<&str>) -> Arc<dyn ObjectStore> {
    match prefix {
        Some(prefix) => Arc::new(PrefixStore::new(store, Path::from(prefix))),
        None => Arc::new(store),
    }
}

fn client_options(config: &ObjectStorageConfig) -> Result<ClientOptions, ObjectStorageError> {
    let mut options = ClientOptions::new()
        .with_allow_http(config.tls.allow_http)
        .with_connect_timeout(config.http.connect_timeout())
        .with_timeout(config.http.request_timeout())
        .with_config(ClientConfigKey::UserAgent, USER_AGENT);
    if let Some(path) = &config.tls.ca_cert_path {
        let pem = std::fs::read(path).map_err(|source| ObjectStorageError::ReadCaBundle {
            path: path.clone(),
            source,
        })?;
        let certificates = Certificate::from_pem_bundle(&pem)?;
        if certificates.is_empty() {
            return Err(ObjectStorageError::EmptyCaBundle { path: path.clone() });
        }
        for certificate in certificates {
            options = options.with_root_certificate(certificate);
        }
    }
    Ok(options)
}

fn retry_config(config: &ObjectStorageConfig) -> RetryConfig {
    RetryConfig {
        backoff: BackoffConfig {
            max_backoff: config.retry.max_backoff(),
            ..BackoffConfig::default()
        },
        max_retries: config.retry.max_retries,
        retry_timeout: config.retry.retry_timeout(),
    }
}

fn build_s3(
    s3: &S3Config,
    bucket: &str,
    options: ClientOptions,
    retry: RetryConfig,
) -> Result<AmazonS3, ObjectStorageError> {
    // `from_env` is what picks up IRSA, EKS Pod Identity and ECS variables injected into the pod.
    let mut builder = match s3.auth {
        S3Auth::Identity => AmazonS3Builder::from_env(),
        S3Auth::Static => AmazonS3Builder::new(),
    };
    builder = builder
        .with_bucket_name(bucket)
        .with_client_options(options)
        .with_retry(retry)
        .with_virtual_hosted_style_request(!s3.path_style);
    if let Some(region) = &s3.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &s3.endpoint {
        builder = builder.with_endpoint(s3_endpoint(endpoint, bucket, s3.path_style)?);
    }
    if s3.auth == S3Auth::Static {
        builder = builder
            .with_access_key_id(s3.access_key_id.as_deref().unwrap_or_default())
            .with_secret_access_key(s3.secret_access_key.as_deref().unwrap_or_default());
        if let Some(token) = &s3.session_token {
            builder = builder.with_token(token);
        }
    }
    if let Some(key_id) = &s3.sse_kms_key_id {
        builder = builder.with_sse_kms_encryption(key_id);
    }
    Ok(builder.build()?)
}

/// `object_store` uses a custom endpoint verbatim, so virtual-hosted addressing
/// needs the bucket spliced into the host here.
fn s3_endpoint(
    endpoint: &str,
    bucket: &str,
    path_style: bool,
) -> Result<String, ObjectStorageError> {
    let endpoint = endpoint.trim_end_matches('/');
    if path_style {
        return Ok(endpoint.to_string());
    }
    let (scheme, host) = endpoint.split_once("://").unwrap_or(("https", endpoint));
    if host.starts_with(&format!("{bucket}.")) {
        return Err(ObjectStorageError::EndpointContainsBucket {
            endpoint: endpoint.to_string(),
        });
    }
    Ok(format!("{scheme}://{bucket}.{host}"))
}

fn build_gcs(
    gcs: &GcsConfig,
    bucket: &str,
    options: ClientOptions,
    retry: RetryConfig,
) -> Result<GoogleCloudStorage, ObjectStorageError> {
    // `from_env` honours GOOGLE_APPLICATION_CREDENTIALS; without it the builder
    // falls back to the ADC file and then the metadata server (Workload Identity).
    let mut builder = match gcs.auth {
        GcsAuth::Identity => GoogleCloudStorageBuilder::from_env(),
        GcsAuth::ServiceAccountKey => GoogleCloudStorageBuilder::new(),
    };
    builder = builder
        .with_bucket_name(bucket)
        .with_client_options(options)
        .with_retry(retry);
    if let Some(endpoint) = &gcs.endpoint {
        builder = builder.with_base_url(endpoint);
    }
    if let Some(path) = &gcs.service_account_key_path {
        builder = builder.with_service_account_path(path);
    }
    if let Some(key) = &gcs.service_account_key {
        builder = builder.with_service_account_key(key);
    }
    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> ObjectStorageConfig {
        config::Config::builder()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap()
    }

    #[test]
    fn virtual_hosted_endpoint_gets_bucket_host() {
        assert_eq!(
            s3_endpoint("https://fsn1.your-objectstorage.com/", "orbit", false).unwrap(),
            "https://orbit.fsn1.your-objectstorage.com"
        );
        assert_eq!(
            s3_endpoint("http://localhost:9000", "orbit", true).unwrap(),
            "http://localhost:9000"
        );
        assert!(matches!(
            s3_endpoint("https://orbit.fsn1.your-objectstorage.com", "orbit", false),
            Err(ObjectStorageError::EndpointContainsBucket { .. })
        ));
    }

    #[test]
    fn s3_compatible_static_store_builds_offline() {
        let cfg = parse(
            r#"
provider: s3
bucket: orbit-probe
prefix: dev
s3:
  endpoint: http://localhost:9010
  path_style: true
  auth: static
  access_key_id: k
  secret_access_key: s
tls:
  allow_http: true
"#,
        );
        build_store(&cfg).expect("builder needs no network");
    }

    #[test]
    fn invalid_config_is_rejected_before_building() {
        let cfg = parse("provider: s3\nbucket: b\n");
        assert!(matches!(
            build_store(&cfg),
            Err(ObjectStorageError::Config(
                ObjectStorageConfigError::S3RegionMissing
            ))
        ));
    }

    #[test]
    fn ca_bundle_is_loaded_from_path() {
        let dir = tempfile::tempdir().unwrap();
        let ca = dir.path().join("ca.pem");
        std::fs::write(&ca, include_bytes!("../tests/fixtures/test-ca.crt")).unwrap();
        let cfg = parse(&format!(
            "provider: s3\nbucket: b\ns3:\n  endpoint: https://minio.internal:9000\n  path_style: true\n  auth: static\n  access_key_id: k\n  secret_access_key: s\ntls:\n  ca_cert_path: {}\n",
            ca.display()
        ));
        build_store(&cfg).expect("PEM bundle parses");

        std::fs::write(&ca, b"not a certificate").unwrap();
        assert!(matches!(
            build_store(&cfg),
            Err(ObjectStorageError::EmptyCaBundle { .. })
        ));
    }
}

//! The provider client: Orbit's side of the gRPC contract.
//!
//! Orbit signs a short-lived HS256 JWT per call, fetches the manifest and the
//! package documents, and watches the per-package digests so a changed document
//! can trigger a graceful restart. The client knows nothing about what a package
//! describes; it only moves documents and digests.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::Serialize;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::live::{LiveProvider, LiveStream};
use crate::package::{Package, PackageError};
use crate::proto::{
    GetManifestRequest, GetPackageRequest, LiveRequest, Manifest,
    provider_client::ProviderClient as RawClient,
};

const JWT_ISSUER: &str = "orbit";
const JWT_AUDIENCE: &str = "orbit-provider";
const JWT_SUBJECT: &str = "orbit";
const JWT_TTL_SECS: u64 = 60;
const MIN_KEY_BYTES: usize = 32;

#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub name: String,
    pub url: String,
    pub jwt_key_file: String,
    pub refresh_secs: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("provider {provider}: jwt key file {path}: {source}")]
    KeyFile {
        provider: String,
        path: String,
        source: std::io::Error,
    },
    #[error("provider {provider}: jwt key must be at least {MIN_KEY_BYTES} bytes, got {got}")]
    KeyTooShort { provider: String, got: usize },
    #[error("signing the request JWT failed: {0}")]
    Jwt(jsonwebtoken::errors::Error),
    #[error("provider {provider}: connecting to {url} failed: {detail}")]
    Connect {
        provider: String,
        url: String,
        detail: String,
    },
    #[error("provider {0}: {1}")]
    Rpc(String, tonic::Status),
    #[error(transparent)]
    Package(#[from] PackageError),
}

/// A gRPC client bound to one provider. Each call carries a freshly signed JWT.
pub struct ProviderClient {
    name: String,
    jwt_key: Vec<u8>,
    raw: RawClient<Channel>,
}

/// Remembers the digest of each package so a refresh can tell when a document
/// changed. A changed digest is the signal to finish in-flight work and exit,
/// so Kubernetes restarts the process with the new model.
#[derive(Debug, Default)]
pub struct DigestTracker {
    digests: BTreeMap<String, String>,
}

impl ProviderClient {
    pub async fn connect(config: &ProviderConfig) -> Result<Self, ProviderError> {
        let jwt_key = read_jwt_key(&config.name, &config.jwt_key_file)?;
        let channel = Channel::from_shared(config.url.clone())
            .map_err(|source| ProviderError::Connect {
                provider: config.name.clone(),
                url: config.url.clone(),
                detail: source.to_string(),
            })?
            .connect()
            .await
            .map_err(|source| ProviderError::Connect {
                provider: config.name.clone(),
                url: config.url.clone(),
                detail: source.to_string(),
            })?;
        Ok(Self::with_channel(config.name.clone(), jwt_key, channel))
    }

    pub fn with_channel(name: String, jwt_key: Vec<u8>, channel: Channel) -> Self {
        Self {
            name,
            jwt_key,
            raw: RawClient::new(channel),
        }
    }

    pub async fn get_manifest(&mut self) -> Result<Manifest, ProviderError> {
        let request = self.authed_request(GetManifestRequest {})?;
        self.raw
            .get_manifest(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| ProviderError::Rpc(self.name.clone(), status))
    }

    /// Fetches one package document and validates it. The digest from the
    /// manifest flows into the package so the extension table names match.
    pub async fn get_package(
        &mut self,
        name: &str,
        version: &str,
        digest: String,
    ) -> Result<Package, ProviderError> {
        let request = self.authed_request(GetPackageRequest {
            name: name.to_string(),
            version: version.to_string(),
        })?;
        let document = self
            .raw
            .get_package(request)
            .await
            .map(tonic::Response::into_inner)
            .map_err(|status| ProviderError::Rpc(self.name.clone(), status))?;
        Ok(Package::parse(&document.document_yaml, digest)?)
    }

    fn authed_metadata<T>(
        &self,
        mut request: tonic::Request<T>,
    ) -> Result<tonic::Request<T>, Status> {
        let token = sign_provider_jwt(&self.name, &self.jwt_key)
            .map_err(|e| Status::internal(e.to_string()))?;
        let bearer: MetadataValue<_> = format!("Bearer {token}")
            .parse()
            .map_err(|_| Status::internal("provider jwt is not a valid header"))?;
        request.metadata_mut().insert("authorization", bearer);
        Ok(request)
    }

    fn authed_request<T>(&self, message: T) -> Result<tonic::Request<T>, ProviderError> {
        let token = sign_provider_jwt(&self.name, &self.jwt_key)?;
        let bearer: MetadataValue<_> = format!("Bearer {token}").parse().map_err(|_| {
            ProviderError::Jwt(jsonwebtoken::errors::ErrorKind::InvalidToken.into())
        })?;
        let mut request = tonic::Request::new(message);
        request.metadata_mut().insert("authorization", bearer);
        Ok(request)
    }
}

#[tonic::async_trait]
impl LiveProvider for ProviderClient {
    /// One Live call for one operation. Clones the cheap channel-backed client so
    /// the trait can take `&self` while concurrent operations each hold their own.
    async fn live(&self, request: LiveRequest) -> Result<LiveStream, Status> {
        let mut raw = self.raw.clone();
        let request = self.authed_metadata(tonic::Request::new(request))?;
        let stream = raw.live(request).await?.into_inner();
        Ok(Box::pin(stream))
    }
}

impl DigestTracker {
    /// Records the manifest's digests and reports whether any package digest
    /// changed since the last snapshot. The first observation establishes the
    /// baseline and reports no change.
    pub fn observe(&mut self, manifest: &Manifest) -> bool {
        let next: BTreeMap<String, String> = manifest
            .packages
            .iter()
            .map(|package| (package.name.clone(), package.digest.clone()))
            .collect();
        let changed = !self.digests.is_empty() && next != self.digests;
        self.digests = next;
        changed
    }
}

#[derive(Serialize)]
struct ProviderClaims {
    iss: &'static str,
    aud: &'static str,
    sub: &'static str,
    iat: u64,
    exp: u64,
}

fn sign_provider_jwt(provider: &str, key: &[u8]) -> Result<String, ProviderError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let claims = ProviderClaims {
        iss: JWT_ISSUER,
        aud: JWT_AUDIENCE,
        sub: JWT_SUBJECT,
        iat: now,
        exp: now + JWT_TTL_SECS,
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(key),
    )
    .map_err(|source| {
        ProviderError::Rpc(
            provider.to_string(),
            tonic::Status::internal(source.to_string()),
        )
    })
}

fn read_jwt_key(provider: &str, path: &str) -> Result<Vec<u8>, ProviderError> {
    let bytes = std::fs::read(Path::new(path)).map_err(|source| ProviderError::KeyFile {
        provider: provider.to_string(),
        path: path.to_string(),
        source,
    })?;
    if bytes.len() < MIN_KEY_BYTES {
        return Err(ProviderError::KeyTooShort {
            provider: provider.to_string(),
            got: bytes.len(),
        });
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::PackageRef;
    use jsonwebtoken::{DecodingKey, Validation};

    fn manifest_with(digests: &[(&str, &str)]) -> Manifest {
        Manifest {
            provider: "des".into(),
            scopes_table: "ex_scopes".into(),
            packages: digests
                .iter()
                .map(|(name, digest)| PackageRef {
                    name: (*name).into(),
                    version: "0.1.0".into(),
                    digest: (*digest).into(),
                })
                .collect(),
        }
    }

    #[test]
    fn signs_a_scoped_short_lived_jwt() {
        let key = [7u8; 32];
        let token = sign_provider_jwt("des", &key).expect("signs");

        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_audience(&[JWT_AUDIENCE]);
        validation.set_issuer(&[JWT_ISSUER]);
        let data = jsonwebtoken::decode::<serde_json::Value>(
            &token,
            &DecodingKey::from_secret(&key),
            &validation,
        )
        .expect("verifies with the same key");

        assert_eq!(data.claims["sub"], JWT_SUBJECT);
        let ttl = data.claims["exp"].as_u64().unwrap() - data.claims["iat"].as_u64().unwrap();
        assert_eq!(ttl, JWT_TTL_SECS);
    }

    #[test]
    fn a_short_key_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("key");
        std::fs::write(&path, b"too short").unwrap();
        let err = read_jwt_key("des", path.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, ProviderError::KeyTooShort { .. }));
    }

    #[test]
    fn digest_tracker_signals_only_on_change() {
        let mut tracker = DigestTracker::default();
        assert!(
            !tracker.observe(&manifest_with(&[("example.widgets", "aaa")])),
            "baseline"
        );
        assert!(
            !tracker.observe(&manifest_with(&[("example.widgets", "aaa")])),
            "same digest"
        );
        assert!(
            tracker.observe(&manifest_with(&[("example.widgets", "bbb")])),
            "digest changed"
        );
        assert!(tracker.observe(&manifest_with(&[])), "package removed");
    }
}

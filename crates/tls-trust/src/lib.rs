//! Trust roots shared by every outbound TLS client (GitLab API, ClickHouse,
//! NATS, object storage).
//!
//! The platform trust store is always in effect. `tls.ca_bundle_path` names a
//! PEM file whose certificates are appended to it, so a private CA can be
//! trusted everywhere with one setting and without replacing public roots.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use orbit_server_config::TlsConfig;
use rustls::RootCertStore;
use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use tracing::{info, warn};

#[derive(Debug, thiserror::Error)]
pub enum TrustStoreError {
    #[error("tls.ca_bundle_path: cannot read '{path}': {source}")]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("tls.ca_bundle_path: '{path}' is not a PEM certificate bundle: {source}")]
    Pem {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    #[error("tls.ca_bundle_path: '{path}' contains no certificates")]
    Empty { path: PathBuf },
    #[error("tls.ca_bundle_path: '{path}' holds a certificate that is not a valid root: {source}")]
    Root {
        path: PathBuf,
        #[source]
        source: rustls::Error,
    },
    #[error("platform trust store could not be loaded: {0}")]
    Platform(String),
}

/// Root certificates for outbound TLS: the platform store plus the operator's
/// extra roots. Cheap to clone; built once at startup.
#[derive(Clone, Debug)]
pub struct TrustStore {
    extra_roots: Vec<CertificateDer<'static>>,
    roots: Option<Arc<RootCertStore>>,
}

impl TrustStore {
    /// Platform trust store only, the behaviour when no bundle is configured.
    pub fn platform_only() -> Self {
        Self {
            extra_roots: Vec::new(),
            roots: None,
        }
    }

    /// Reads the bundle named by `tls.ca_bundle_path` once. Fails when the
    /// file is missing, is not PEM, holds no certificate, or holds one that
    /// cannot serve as a trust anchor.
    pub fn load(tls: &TlsConfig) -> Result<Self, TrustStoreError> {
        let Some(path) = tls.ca_bundle_path.as_deref().map(Path::new) else {
            return Ok(Self::platform_only());
        };

        let extra_roots = read_bundle(path)?;
        let mut roots = platform_roots()?;
        for cert in &extra_roots {
            roots
                .add(cert.clone())
                .map_err(|source| TrustStoreError::Root {
                    path: path.to_path_buf(),
                    source,
                })?;
        }
        info!(
            path = %path.display(),
            extra_roots = extra_roots.len(),
            "extra TLS root certificates loaded for outbound clients"
        );

        Ok(Self {
            extra_roots,
            roots: Some(Arc::new(roots)),
        })
    }

    /// Certificates appended to the platform store; empty without a bundle.
    pub fn extra_roots(&self) -> &[CertificateDer<'static>] {
        &self.extra_roots
    }

    /// Platform roots plus the extra roots, for clients that take a rustls
    /// root store. `None` when no bundle is configured, so callers keep their
    /// default trust path.
    pub fn root_cert_store(&self) -> Option<RootCertStore> {
        self.roots.as_deref().cloned()
    }

    /// A rustls client config over [`Self::root_cert_store`], for clients
    /// that take a whole config. `None` when no bundle is configured.
    pub fn client_config(&self) -> Option<rustls::ClientConfig> {
        let roots = self.root_cert_store()?;
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Some(config)
    }
}

fn read_bundle(path: &Path) -> Result<Vec<CertificateDer<'static>>, TrustStoreError> {
    let pem = std::fs::read(path).map_err(|source| TrustStoreError::Read {
        path: path.to_path_buf(),
        source,
    })?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TrustStoreError::Pem {
            path: path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TrustStoreError::Empty {
            path: path.to_path_buf(),
        });
    }
    Ok(certs)
}

fn platform_roots() -> Result<RootCertStore, TrustStoreError> {
    let loaded = rustls_native_certs::load_native_certs();
    if loaded.certs.is_empty() && !loaded.errors.is_empty() {
        let errors: Vec<String> = loaded.errors.iter().map(ToString::to_string).collect();
        return Err(TrustStoreError::Platform(errors.join("; ")));
    }
    for error in &loaded.errors {
        warn!(%error, "skipping an unreadable platform root certificate");
    }
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(loaded.certs);
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn tls_with_bundle(path: &Path) -> TlsConfig {
        TlsConfig {
            cert_path: None,
            key_path: None,
            ca_bundle_path: Some(path.to_str().unwrap().to_string()),
        }
    }

    fn ca_pem(name: &str) -> String {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let key = rcgen::KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().pem()
    }

    #[test]
    fn no_bundle_keeps_platform_trust_only() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let tls = TlsConfig {
            cert_path: None,
            key_path: None,
            ca_bundle_path: None,
        };

        let trust = TrustStore::load(&tls).unwrap();

        assert!(trust.extra_roots().is_empty());
        assert!(trust.root_cert_store().is_none());
        assert!(trust.client_config().is_none());
    }

    #[test]
    fn missing_file_is_an_error() {
        let dir = TempDir::new().unwrap();
        let tls = tls_with_bundle(&dir.path().join("absent.pem"));

        let err = TrustStore::load(&tls).unwrap_err();

        assert!(matches!(err, TrustStoreError::Read { .. }), "{err}");
        assert!(err.to_string().contains("absent.pem"), "{err}");
    }

    #[test]
    fn non_pem_content_is_an_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("garbage.pem");
        fs::write(&path, "this is not a certificate").unwrap();

        let err = TrustStore::load(&tls_with_bundle(&path)).unwrap_err();

        assert!(
            matches!(
                err,
                TrustStoreError::Pem { .. } | TrustStoreError::Empty { .. }
            ),
            "{err}"
        );
    }

    #[test]
    fn pem_without_certificates_is_an_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("key-only.pem");
        fs::write(&path, rcgen::KeyPair::generate().unwrap().serialize_pem()).unwrap();

        let err = TrustStore::load(&tls_with_bundle(&path)).unwrap_err();

        assert!(matches!(err, TrustStoreError::Empty { .. }), "{err}");
    }

    #[test]
    fn every_certificate_in_the_bundle_becomes_an_extra_root() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("bundle.pem");
        fs::write(
            &path,
            format!("{}{}", ca_pem("First CA"), ca_pem("Second CA")),
        )
        .unwrap();

        let trust = TrustStore::load(&tls_with_bundle(&path)).unwrap();

        assert_eq!(trust.extra_roots().len(), 2);
        let roots = trust.root_cert_store().unwrap();
        assert!(roots.len() >= 2, "platform roots plus the two extras");
        assert!(trust.client_config().is_some());
    }
}

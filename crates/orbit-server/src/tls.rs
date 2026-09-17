use labkit::tls::ServerTls;
use orbit_server_config::TlsConfig;
use tonic::transport::Identity;
use tonic::transport::server::ServerTlsConfig;

/// TLS for the HTTP listeners this binary owns, resolved from config.
///
/// Both groups are off unless enabled, so a build with no TLS configured gets
/// `None` twice and every listener stays plaintext.
pub struct ListenerTls {
    /// Webserver, indexer health, dispatcher health and health-check.
    pub http: Option<ServerTls>,
    /// The labkit listener: `/-/metrics` and its own probe endpoints.
    pub probe_server: Option<ServerTls>,
}

impl ListenerTls {
    pub fn load(tls: &TlsConfig) -> anyhow::Result<Self> {
        Ok(Self {
            http: load_group(tls.http_paths()?)?,
            probe_server: load_group(tls.probe_server_paths()?)?,
        })
    }
}

fn load_group(paths: Option<(&str, &str)>) -> anyhow::Result<Option<ServerTls>> {
    paths
        .map(|(cert, key)| ServerTls::builder(cert, key).build())
        .transpose()
        .map_err(Into::into)
}

/// This lives in `orbit-server` (not `orbit-server-config`) because it depends on
/// `tonic`, which is a heavy runtime dependency.
pub async fn load_tls_config(tls: &TlsConfig) -> anyhow::Result<Option<ServerTlsConfig>> {
    match (&tls.cert_path, &tls.key_path) {
        (Some(cert_path), Some(key_path)) => {
            let cert = tokio::fs::read(cert_path).await?;
            let key = tokio::fs::read(key_path).await?;
            let identity = Identity::from_pem(cert, key);
            Ok(Some(ServerTlsConfig::new().identity(identity)))
        }
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("both `tls.cert_path` and `tls.key_path` must be set to enable TLS")
        }
        (None, None) => Ok(None),
    }
}

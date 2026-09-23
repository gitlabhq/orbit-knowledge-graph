use labkit::tls::ServerTls;
use orbit_server_config::TlsConfig;
use tonic::transport::Identity;
use tonic::transport::server::ServerTlsConfig;

/// TLS for the internal listeners, or `None` when they stay plaintext.
pub fn load_internal(tls: &TlsConfig) -> anyhow::Result<Option<ServerTls>> {
    tls.internal_paths()?
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

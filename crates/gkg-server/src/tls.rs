use gkg_server_config::TlsConfig;
use tonic::transport::Identity;
use tonic::transport::server::ServerTlsConfig;

/// Load TLS identity from the paths in [`TlsConfig`].
///
/// This lives in `gkg-server` (not `gkg-server-config`) because it depends on
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

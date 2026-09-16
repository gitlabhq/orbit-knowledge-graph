//! Every outbound TLS client must trust a private CA when `tls.ca_bundle_path`
//! names it, and must keep rejecting it otherwise. Runs without Docker: a TLS
//! endpoint minted by a fresh private CA answers in-process.

use std::net::SocketAddr;
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use clickhouse_client::ClickHouseConfigurationExt;
use gitlab_client::GitlabClient;
use orbit_server_config::{
    AppConfig, ClickHouseConfiguration, GitlabClientConfiguration, TlsConfig,
};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose, SanType,
};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tempfile::TempDir;
use tls_trust::TrustStore;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const PROJECT_INFO_BODY: &str = r#"{"project_id":42,"default_branch":"main"}"#;

struct PrivateCa {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
}

fn mint_private_ca() -> PrivateCa {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Tenant Private CA");
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().unwrap()).unwrap();

    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::Ipv4Addr::LOCALHOST.into()));
    server_params
        .distinguished_name
        .push(DnType::CommonName, "private-endpoint");
    server_params.is_ca = IsCa::NoCa;
    server_params.use_authority_key_identifier_extension = true;
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &*ca).unwrap();

    PrivateCa {
        ca_cert_pem: ca.as_ref().pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
    }
}

/// A minimal HTTPS responder: project info as JSON for the GitLab route, an
/// empty 200 for everything else (what a ClickHouse `execute` expects).
async fn serve_https(pki: &PrivateCa) -> SocketAddr {
    let certs = CertificateDer::pem_slice_iter(pki.server_cert_pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let key = PrivateKeyDer::from_pem_slice(pki.server_key_pem.as_bytes()).unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(tcp).await else {
                    return;
                };
                let mut request = vec![0u8; 8192];
                let read = tls.read(&mut request).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&request[..read]);
                let body = if head.contains("/api/v4/internal/orbit/project/") {
                    PROJECT_INFO_BODY
                } else {
                    ""
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = tls.write_all(response.as_bytes()).await;
                let _ = tls.shutdown().await;
            });
        }
    });
    addr
}

fn bundle_trust(pki: &PrivateCa, dir: &TempDir) -> TrustStore {
    let path = dir.path().join("private-ca.pem");
    std::fs::write(&path, &pki.ca_cert_pem).unwrap();
    let tls = TlsConfig {
        ca_bundle_path: Some(path.to_str().unwrap().to_string()),
        ..AppConfig::embedded_defaults().tls
    };
    TrustStore::load(&tls).unwrap()
}

fn gitlab_client(addr: SocketAddr, trust: &TrustStore) -> GitlabClient {
    let config = GitlabClientConfiguration {
        base_url: format!("https://{addr}"),
        signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
        resolve_host: None,
    };
    GitlabClient::new(config, trust).unwrap()
}

fn clickhouse_config(addr: SocketAddr) -> ClickHouseConfiguration {
    ClickHouseConfiguration {
        url: format!("https://{addr}"),
        ..AppConfig::embedded_defaults().graph
    }
}

#[tokio::test]
async fn gitlab_client_trusts_the_private_ca_through_the_bundle() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let pki = mint_private_ca();
    let addr = serve_https(&pki).await;
    let dir = TempDir::new().unwrap();

    let info = gitlab_client(addr, &bundle_trust(&pki, &dir))
        .project_info(42)
        .await
        .expect("private CA in tls.ca_bundle_path must be trusted");

    assert_eq!(info.project_id, 42);
    assert_eq!(info.default_branch, "main");
}

#[tokio::test]
async fn gitlab_client_rejects_the_private_ca_without_the_bundle() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let pki = mint_private_ca();
    let addr = serve_https(&pki).await;

    let result = gitlab_client(addr, &TrustStore::platform_only())
        .project_info(42)
        .await;

    assert!(
        result.is_err(),
        "platform roots alone must not trust a private CA"
    );
}

#[tokio::test]
async fn clickhouse_client_trusts_the_private_ca_through_the_bundle() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let pki = mint_private_ca();
    let addr = serve_https(&pki).await;
    let dir = TempDir::new().unwrap();

    clickhouse_config(addr)
        .build_client_with_trust(&bundle_trust(&pki, &dir))
        .execute("SELECT 1")
        .await
        .expect("private CA in tls.ca_bundle_path must be trusted");
}

#[tokio::test]
async fn clickhouse_client_rejects_the_private_ca_without_the_bundle() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let pki = mint_private_ca();
    let addr = serve_https(&pki).await;

    let result = clickhouse_config(addr)
        .build_client_with_trust(&TrustStore::platform_only())
        .execute("SELECT 1")
        .await;

    assert!(
        result.is_err(),
        "platform roots alone must not trust a private CA"
    );
}

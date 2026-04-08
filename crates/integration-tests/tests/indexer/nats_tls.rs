//! Integration tests for NATS mTLS connectivity.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use gkg_server_config::NatsConfiguration;
use indexer::metrics::EngineMetrics;
use indexer::nats::NatsBroker;
use indexer::types::{Envelope, Event, Subscription};
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

const TEST_STREAM: &str = "tls_test_stream";
const TEST_SUBJECT: &str = "tls.test.events";
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TlsTestEvent {
    id: String,
    value: i32,
}

impl Event for TlsTestEvent {
    fn subscription() -> Subscription {
        Subscription::new(TEST_STREAM, TEST_SUBJECT)
    }
}

struct TestPki {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

/// Generates a test PKI. `server_sans` must include all hostnames/IPs the client
/// will use to connect (e.g. "localhost", "127.0.0.1", "docker" in CI).
fn generate_test_pki(server_sans: &[String]) -> TestPki {
    // CA
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Test CA");
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let ca_key = KeyPair::generate().unwrap();
    let ca = CertifiedIssuer::self_signed(ca_params, ca_key).unwrap();

    // Server cert — SANs must match the hostname the client connects to
    let mut server_params = CertificateParams::new(server_sans.to_vec()).unwrap();
    server_params
        .distinguished_name
        .push(DnType::CommonName, "nats-server");
    server_params.is_ca = IsCa::NoCa;
    server_params.use_authority_key_identifier_extension = true;
    server_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ServerAuth);
    let server_key = KeyPair::generate().unwrap();
    let server_cert = server_params.signed_by(&server_key, &*ca).unwrap();

    // Client cert — CN used for NATS user mapping via verify_and_map
    let mut client_params = CertificateParams::new(vec!["test-client".into()]).unwrap();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "test-client");
    client_params.is_ca = IsCa::NoCa;
    client_params.use_authority_key_identifier_extension = true;
    client_params
        .extended_key_usages
        .push(ExtendedKeyUsagePurpose::ClientAuth);
    let client_key = KeyPair::generate().unwrap();
    let client_cert = client_params.signed_by(&client_key, &*ca).unwrap();

    TestPki {
        ca_cert_pem: ca.as_ref().pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    }
}

/// Resolves the testcontainers host to use for server cert SANs.
async fn resolve_container_host() -> String {
    // Start a throwaway container to discover the host (localhost vs docker in CI)
    let probe = GenericImage::new("nats", "2.11-alpine")
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .start()
        .await
        .expect("failed to start probe container");
    let host = probe
        .get_host()
        .await
        .expect("failed to get host")
        .to_string();
    drop(probe);
    host
}

fn nats_tls_config() -> String {
    serde_json::json!({
        "port": 4222,
        "http_port": 8222,
        "jetstream": { "store_dir": "/data" },
        "tls": {
            "cert_file": "/etc/nats/server.pem",
            "key_file": "/etc/nats/server-key.pem",
            "ca_file": "/etc/nats/ca.pem",
            "verify_and_map": true
        },
        "authorization": {
            "users": [
                { "user": "test-client" }
            ]
        }
    })
    .to_string()
}

/// Writes client certs to a temp dir and returns (temp_dir, NatsConfiguration).
fn client_config(pki: &TestPki, url: &str, temp_dir: &TempDir) -> NatsConfiguration {
    let ca_path = temp_dir.path().join("ca.pem");
    let cert_path = temp_dir.path().join("client.pem");
    let key_path = temp_dir.path().join("client-key.pem");
    std::fs::write(&ca_path, &pki.ca_cert_pem).unwrap();
    std::fs::write(&cert_path, &pki.client_cert_pem).unwrap();
    std::fs::write(&key_path, &pki.client_key_pem).unwrap();

    NatsConfiguration {
        url: url.to_string(),
        tls_ca_cert_path: Some(ca_path.to_str().unwrap().into()),
        tls_cert_path: Some(cert_path.to_str().unwrap().into()),
        tls_key_path: Some(key_path.to_str().unwrap().into()),
        ..Default::default()
    }
}

async fn start_nats_tls_container(
    pki: &TestPki,
) -> (testcontainers::ContainerAsync<GenericImage>, String) {
    let config_data = nats_tls_config().into_bytes();

    let container = GenericImage::new("nats", "2.11-alpine")
        .with_copy_to("/etc/nats/nats.conf", config_data)
        .with_copy_to("/etc/nats/ca.pem", pki.ca_cert_pem.as_bytes().to_vec())
        .with_copy_to(
            "/etc/nats/server.pem",
            pki.server_cert_pem.as_bytes().to_vec(),
        )
        .with_copy_to(
            "/etc/nats/server-key.pem",
            pki.server_key_pem.as_bytes().to_vec(),
        )
        .with_cmd(vec!["--config", "/etc/nats/nats.conf"])
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .with_ready_conditions(vec![WaitFor::seconds(5)])
        .start()
        .await
        .expect("failed to start NATS TLS container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to get NATS port");

    (container, format!("{host}:{port}"))
}

#[tokio::test]
async fn mtls_publish_and_subscribe() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let host = resolve_container_host().await;
    let sans = vec![host, "localhost".into(), "127.0.0.1".into()];
    let pki = generate_test_pki(&sans);
    let (_container, url) = start_nats_tls_container(&pki).await;
    let temp_dir = TempDir::new().unwrap();
    let config = client_config(&pki, &url, &temp_dir);

    let broker = NatsBroker::connect(&config)
        .await
        .expect("mTLS connection should succeed");

    let subscription = Subscription::new(TEST_STREAM, TEST_SUBJECT);
    broker
        .ensure_streams(std::slice::from_ref(&subscription))
        .await
        .expect("stream creation should work over mTLS");

    let mut messages = broker
        .subscribe(&subscription, Arc::new(EngineMetrics::new()))
        .await
        .expect("subscribe should work over mTLS");

    let event = TlsTestEvent {
        id: "mtls-1".into(),
        value: 42,
    };
    let envelope = Envelope::new(&event).unwrap();
    broker
        .publish(&subscription, &envelope)
        .await
        .expect("publish should work over mTLS");

    let received = tokio::time::timeout(RECEIVE_TIMEOUT, messages.next())
        .await
        .expect("timed out")
        .expect("stream ended")
        .expect("receive failed");

    let decoded: TlsTestEvent = received.envelope.to_event().unwrap();
    assert_eq!(decoded, event);
    received.ack().await.unwrap();
}

#[tokio::test]
async fn mtls_rejects_without_client_cert() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let host = resolve_container_host().await;
    let sans = vec![host, "localhost".into(), "127.0.0.1".into()];
    let pki = generate_test_pki(&sans);
    let (_container, url) = start_nats_tls_container(&pki).await;
    let temp_dir = TempDir::new().unwrap();

    // CA only — no client cert — server requires verify_and_map
    let ca_path = temp_dir.path().join("ca.pem");
    std::fs::write(&ca_path, &pki.ca_cert_pem).unwrap();

    let config = NatsConfiguration {
        url: url.to_string(),
        tls_ca_cert_path: Some(ca_path.to_str().unwrap().into()),
        ..Default::default()
    };

    let result = NatsBroker::connect(&config).await;
    assert!(
        result.is_err(),
        "connection without client cert should fail"
    );
}

#[tokio::test]
async fn mtls_rejects_wrong_ca() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let host = resolve_container_host().await;
    let sans = vec![host, "localhost".into(), "127.0.0.1".into()];
    let pki = generate_test_pki(&sans);
    let (_container, url) = start_nats_tls_container(&pki).await;

    // Generate a completely separate CA — server won't trust certs signed by it
    let wrong_pki = generate_test_pki(&sans);
    let temp_dir = TempDir::new().unwrap();
    let config = client_config(&wrong_pki, &url, &temp_dir);

    let result = NatsBroker::connect(&config).await;
    assert!(result.is_err(), "connection with wrong CA should fail");
}

#[tokio::test]
async fn connect_fails_on_cert_without_key() {
    let temp_dir = TempDir::new().unwrap();
    let cert_path = temp_dir.path().join("client.pem");
    std::fs::write(&cert_path, "dummy").unwrap();

    let config = NatsConfiguration {
        url: "localhost:4222".into(),
        tls_cert_path: Some(cert_path.to_str().unwrap().into()),
        ..Default::default()
    };

    let result = NatsBroker::connect(&config).await;
    let err = result.err().expect("should reject cert without key");
    assert!(
        err.to_string().contains("tls_key_path is missing"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn connect_fails_on_missing_ca_file() {
    let config = NatsConfiguration {
        url: "localhost:4222".into(),
        tls_ca_cert_path: Some("/nonexistent/ca.pem".into()),
        ..Default::default()
    };

    let result = NatsBroker::connect(&config).await;
    let err = result.err().expect("should reject missing CA file");
    assert!(
        err.to_string().contains("file not found"),
        "unexpected error: {err}"
    );
}

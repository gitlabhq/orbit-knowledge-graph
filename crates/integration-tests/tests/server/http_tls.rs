//! TLS on the HTTP listeners: the webserver, the indexer/dispatcher health
//! port, and the health-check service the webserver calls over a verified
//! connection.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use labkit::tls::ServerTls;
use orbit_server::active_schema::ActiveSchema;
use orbit_server::webserver::{InfrastructureHealthClient, Server as HttpServer};

use super::tls_fixtures::{generate_test_certs, init_crypto_provider};

/// A `ServerTls` backed by a fresh self-signed certificate, plus the CA a
/// client needs to trust it.
struct TestTls {
    server: ServerTls,
    ca_pem: String,
}

fn test_tls() -> TestTls {
    init_crypto_provider();
    let (cert_pem, key_pem) = generate_test_certs();

    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, &cert_pem).expect("failed to write certificate");
    std::fs::write(&key_path, key_pem).expect("failed to write key");

    // `from_pem_files` reads both files eagerly, so the directory can go now.
    TestTls {
        server: ServerTls::from_pem_files(&cert_path, &key_path).expect("failed to load TLS"),
        ca_pem: cert_pem,
    }
}

fn client_trusting(ca_pem: &str) -> reqwest::Client {
    let root = reqwest::Certificate::from_pem(ca_pem.as_bytes()).expect("failed to parse CA");
    reqwest::Client::builder()
        .add_root_certificate(root)
        .build()
        .expect("failed to build client")
}

/// Binds loopback on an ephemeral port and hands the socket straight to the
/// server, so nothing can take the port in between.
fn bind_loopback() -> (std::net::TcpListener, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    let port = listener.local_addr().expect("local addr").port();
    (listener, port)
}

fn spawn_webserver(tls: Option<ServerTls>) -> (u16, tokio::task::JoinHandle<()>) {
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    let server = HttpServer::bind(
        "127.0.0.1:0".parse().expect("valid address"),
        ActiveSchema::pinned(ontology),
        tls,
    )
    .expect("failed to bind webserver");
    let port = server.local_addr().expect("local addr").port();

    let handle = tokio::spawn(async move {
        let _ = server.run().await;
    });

    (port, handle)
}

fn spawn_indexer_health(tls: Option<ServerTls>) -> (u16, tokio::task::JoinHandle<()>) {
    let (listener, port) = bind_loopback();

    let handle = tokio::spawn(async move {
        let _ = indexer::health::run_health_server(listener, Arc::new(AtomicBool::new(true)), tls)
            .await;
    });

    (port, handle)
}

#[tokio::test]
async fn webserver_serves_probes_over_tls_and_refuses_plaintext() {
    let tls = test_tls();
    let (port, handle) = spawn_webserver(Some(tls.server.clone()));
    let client = client_trusting(&tls.ca_pem);

    for path in ["/live", "/ready"] {
        let response = client
            .get(format!("https://localhost:{port}{path}"))
            .send()
            .await
            .unwrap_or_else(|err| panic!("{path} over HTTPS should answer: {err}"));
        assert_eq!(response.status().as_u16(), 200, "{path} should return 200");
    }

    let plaintext = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/live"))
        .send()
        .await;
    assert!(
        plaintext.is_err(),
        "a plaintext request must not be answered once TLS is on"
    );

    handle.abort();
}

#[tokio::test]
async fn webserver_stays_plaintext_without_tls() {
    let (port, handle) = spawn_webserver(None);

    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/live"))
        .send()
        .await
        .expect("the webserver must stay plaintext when no TLS is configured");

    assert_eq!(response.status().as_u16(), 200);

    handle.abort();
}

#[tokio::test]
async fn indexer_health_serves_probes_over_tls() {
    let tls = test_tls();
    let (port, handle) = spawn_indexer_health(Some(tls.server.clone()));

    let response = client_trusting(&tls.ca_pem)
        .get(format!("https://localhost:{port}/ready"))
        .send()
        .await
        .expect("/ready over HTTPS should answer");

    assert_eq!(response.status().as_u16(), 200);

    handle.abort();
}

#[tokio::test]
async fn indexer_health_stays_plaintext_without_tls() {
    let (port, handle) = spawn_indexer_health(None);

    let response = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{port}/ready"))
        .send()
        .await
        .expect("the health server must stay plaintext when no TLS is configured");

    assert_eq!(response.status().as_u16(), 200);

    handle.abort();
}

/// The webserver to health-check hop is the only place a gkg client verifies a
/// server certificate, so it is the one that breaks if the SANs are wrong.
#[tokio::test]
async fn the_webserver_verifies_the_health_check_certificate() {
    let tls = test_tls();
    let (listener, port) = bind_loopback();

    let app = axum::Router::new().route(
        "/health",
        axum::routing::get(|| async {
            axum::Json(health_check::HealthStatus {
                status: health_check::Status::Healthy,
                services: vec![],
                clickhouse: vec![],
            })
        }),
    );
    let handle = tokio::spawn(async move {
        let _ = labkit::tls::serve(listener, app, Some(tls.server)).await;
    });

    // The default client has only the public roots, so it cannot verify a
    // self-signed certificate. `check_or_unavailable` degrades rather than
    // failing the request, which is exactly how a SAN mismatch would present.
    let status = InfrastructureHealthClient::new(format!("https://localhost:{port}"))
        .check_or_unavailable()
        .await;

    assert_eq!(
        status.status,
        health_check::Status::Unhealthy,
        "an unverifiable health-check certificate must surface as unhealthy, not as a success"
    );
    assert!(
        status.clickhouse[0]
            .error
            .as_ref()
            .is_some_and(|error| error.contains("unreachable")),
        "the reason must name the failure: {:?}",
        status.clickhouse[0].error
    );

    handle.abort();
}

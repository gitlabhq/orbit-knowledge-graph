//! TLS on the health-check service, which the webserver calls over a verified
//! connection.

use labkit::tls::ServerTls;
use orbit_server::webserver::InfrastructureHealthClient;

use super::tls_fixtures::{generate_test_certs, init_crypto_provider};

/// A `ServerTls` backed by a fresh self-signed certificate.
fn test_tls() -> ServerTls {
    init_crypto_provider();
    let (cert_pem, key_pem) = generate_test_certs();

    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, cert_pem).expect("failed to write certificate");
    std::fs::write(&key_path, key_pem).expect("failed to write key");

    // Both files are read eagerly, so the directory can go now.
    ServerTls::builder(&cert_path, &key_path)
        .build()
        .expect("failed to load TLS")
}

/// Binds loopback on an ephemeral port and hands the socket straight to the
/// server, so nothing can take the port in between.
fn bind_loopback() -> (std::net::TcpListener, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    let port = listener.local_addr().expect("local addr").port();
    (listener, port)
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
        let _ = labkit::server::serve(listener, app, Some(tls)).await;
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

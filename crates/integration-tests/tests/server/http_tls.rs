use labkit::tls::ServerTls;
use orbit_server::health_client::InfrastructureHealthClient;

use super::tls_fixtures::{generate_test_certs, init_crypto_provider};

fn test_tls() -> ServerTls {
    init_crypto_provider();
    let (cert_pem, key_pem) = generate_test_certs();

    let dir = tempfile::tempdir().expect("failed to create temp dir");
    let cert_path = dir.path().join("tls.crt");
    let key_path = dir.path().join("tls.key");
    std::fs::write(&cert_path, cert_pem).expect("failed to write certificate");
    std::fs::write(&key_path, key_pem).expect("failed to write key");

    ServerTls::builder(&cert_path, &key_path)
        .build()
        .expect("failed to load TLS")
}

fn bind_loopback() -> (std::net::TcpListener, u16) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    let port = listener.local_addr().expect("local addr").port();
    (listener, port)
}

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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use gkg_server::auth::JwtValidator;
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::grpc::GrpcServer;
use gkg_server::proto::GetClusterHealthRequest;
use gkg_server::proto::knowledge_graph_service_client::KnowledgeGraphServiceClient;
use gkg_server_config::{AnalyticsConfig, ClickHouseConfiguration, GrpcConfig};
use tonic::transport::server::ServerTlsConfig;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity};

fn init_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn generate_test_certs() -> (String, String, String) {
    let key_pair = rcgen::KeyPair::generate().expect("failed to generate key pair");
    let mut params = rcgen::CertificateParams::new(vec!["localhost".to_string()])
        .expect("failed to create cert params");
    params
        .subject_alt_names
        .push(rcgen::SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let cert = params
        .self_signed(&key_pair)
        .expect("failed to self-sign certificate");
    let key_pem = key_pair.serialize_pem();
    let cert_pem = cert.pem();
    (cert_pem.clone(), key_pem, cert_pem)
}

fn build_grpc_server(addr: SocketAddr, tls_config: Option<ServerTlsConfig>) -> GrpcServer {
    let validator =
        Arc::new(JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap());
    let ontology = Arc::new(ontology::Ontology::load_embedded().expect("ontology must load"));
    let clickhouse_config = ClickHouseConfiguration::default();
    let cluster_health = ClusterHealthChecker::default().into_arc();
    GrpcServer::new(
        addr,
        validator,
        ontology,
        &clickhouse_config,
        cluster_health,
        tls_config,
        GrpcConfig::default(),
        Arc::new(AnalyticsConfig::default()),
    )
}

async fn connect_with_retry(endpoint: Endpoint, retries: u32) -> tonic::transport::Channel {
    for i in 0..retries {
        match endpoint.connect().await {
            Ok(ch) => return ch,
            Err(_) if i + 1 < retries => {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
            Err(e) => panic!("failed to connect after {retries} attempts: {e}"),
        }
    }
    unreachable!()
}

fn tls_endpoint(port: u16, ca_pem: &str) -> Endpoint {
    let client_tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .domain_name("localhost");

    Endpoint::from_shared(format!("https://127.0.0.1:{port}"))
        .expect("failed to create endpoint")
        .tls_config(client_tls)
        .expect("failed to configure client TLS")
}

#[tokio::test]
async fn grpc_tls_handshake_succeeds() {
    init_crypto_provider();

    let (cert_pem, key_pem, ca_pem) = generate_test_certs();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();
    drop(listener);

    let identity = Identity::from_pem(&cert_pem, &key_pem);
    let tls_config = ServerTlsConfig::new().identity(identity);
    let server = build_grpc_server(bound_addr, Some(tls_config));

    let server_handle = tokio::spawn(server.run());

    let channel = connect_with_retry(tls_endpoint(bound_addr.port(), &ca_pem), 20).await;
    let mut client = KnowledgeGraphServiceClient::new(channel);

    // No auth token → Unauthenticated. Getting a gRPC status proves TLS worked.
    let status = client
        .get_cluster_health(GetClusterHealthRequest { format: 0 })
        .await
        .unwrap_err();

    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    server_handle.abort();
}

#[tokio::test]
async fn grpc_plaintext_client_rejected_by_tls_server() {
    init_crypto_provider();

    let (cert_pem, key_pem, ca_pem) = generate_test_certs();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound_addr = listener.local_addr().unwrap();
    drop(listener);

    let identity = Identity::from_pem(&cert_pem, &key_pem);
    let tls_config = ServerTlsConfig::new().identity(identity);
    let server = build_grpc_server(bound_addr, Some(tls_config));

    let server_handle = tokio::spawn(server.run());

    // Wait for server readiness via TLS before testing plaintext rejection
    let _ = connect_with_retry(tls_endpoint(bound_addr.port(), &ca_pem), 20).await;

    let result =
        KnowledgeGraphServiceClient::connect(format!("http://127.0.0.1:{}", bound_addr.port()))
            .await;

    match result {
        Err(_) => {}
        Ok(mut client) => {
            let call = client
                .get_cluster_health(GetClusterHealthRequest { format: 0 })
                .await;
            assert!(call.is_err(), "plaintext call to TLS server should fail");
        }
    }

    server_handle.abort();
}

use std::sync::Mutex;

use axum::Router;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::routing::head;
use orbit_server_config::{AppConfig, QuotaAuthMode};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;

use super::*;
use crate::proto::orbit_service_client::OrbitServiceClient;
use crate::proto::orbit_service_server::OrbitServiceServer;

const CHECKSUM: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

type Recorded = Arc<Mutex<Vec<(HeaderMap, String)>>>;

async fn customers_dot_denying_every_request() -> (String, Recorded) {
    let recorded: Recorded = Arc::default();
    let recorder = recorded.clone();
    let app = Router::new().route(
        "/api/v1/consumers/resolve",
        head(move |headers: HeaderMap, uri: Uri| {
            let recorder = recorder.clone();
            async move {
                let query = uri.query().unwrap_or_default().to_string();
                recorder.lock().unwrap().push((headers, query));
                StatusCode::PAYMENT_REQUIRED
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), recorded)
}

fn license_checksum_quota(customers_dot_url: String) -> QuotaService {
    let mut billing = AppConfig::embedded_defaults().billing;
    billing.quota.enabled = true;
    billing.quota.customers_dot_url = customers_dot_url;
    billing.quota.auth_mode = QuotaAuthMode::LicenseChecksum;
    QuotaService::from_config(&billing).unwrap()
}

async fn serve(service: OrbitServiceImpl) -> OrbitServiceClient<tonic::transport::Channel> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(OrbitServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    OrbitServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap()
}

// `Claims` never serializes the checksum, and serializes `source_type` in a form its own
// deserializer does not read back, so both are set in the raw JSON the way Rails signs it.
fn self_managed_token_with_license_checksum() -> String {
    let now = chrono::Utc::now().timestamp();
    let mut claims = serde_json::to_value(Claims {
        iat: now,
        exp: now + 3600,
        realm: Some("self-managed".into()),
        instance_id: Some("inst-1".into()),
        unique_instance_id: Some("uniq-1".into()),
        instance_version: Some("19.5.0".into()),
        ..test_claims()
    })
    .unwrap();
    claims["source_type"] = "mcp".into();
    claims["license_checksum"] = CHECKSUM.into();
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-that-is-at-least-32-bytes-long"),
    )
    .unwrap()
}

#[tokio::test]
async fn execute_query_presents_the_jwt_license_checksum_to_customers_dot() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let (customers_dot_url, recorded) = customers_dot_denying_every_request().await;
    let service = test_service().with_quota(Arc::new(license_checksum_quota(customers_dot_url)));
    let mut client = serve(service).await;

    let mut request = Request::new(tokio_stream::empty::<ExecuteQueryMessage>());
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!(
            "Bearer {}",
            self_managed_token_with_license_checksum()
        ))
        .unwrap(),
    );
    let status = client.execute_query(request).await.unwrap_err();

    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    let recorded = recorded.lock().unwrap();
    let [(headers, query)] = recorded.as_slice() else {
        panic!(
            "expected exactly one CustomersDot call, got {}",
            recorded.len()
        );
    };
    assert_eq!(headers.get("x-license-token").unwrap(), CHECKSUM);
    assert!(headers.get("x-admin-token").is_none());
    for param in [
        "realm=self-managed",
        "instance_id=inst-1",
        "unique_instance_id=uniq-1",
        "instance_version=19.5.0",
        "feature_qualified_name=orbit_mcp",
    ] {
        assert!(
            query.split('&').any(|p| p == param),
            "{param} missing from {query}"
        );
    }
}

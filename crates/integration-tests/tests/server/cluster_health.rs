use axum::{Json, Router, routing::get};
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::proto::{
    ClusterStatus, GetClusterHealthResponse, ResponseFormat, StructuredClusterHealth,
    get_cluster_health_response,
};
use indexer::schema::version::{ensure_version_table, write_migrating_version};
use integration_testkit::TestContext;
use serde_json::json;
use tokio::net::TcpListener;

fn install_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

async fn start_mock_sidecar(payload: serde_json::Value) -> String {
    install_crypto_provider();
    let app = Router::new().route(
        "/health",
        get(move || {
            let p = payload.clone();
            async move { Json(p) }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn unready_indexer_payload() -> serde_json::Value {
    json!({
        "status": "Unhealthy",
        "services": [{
            "name": "indexer",
            "namespace": "gkg",
            "kind": "Deployment",
            "status": "Unhealthy",
            "ready_replicas": 0,
            "desired_replicas": 2
        }],
        "clickhouse": [{
            "name": "clickhouse",
            "status": "Healthy"
        }]
    })
}

fn extract_structured(response: GetClusterHealthResponse) -> StructuredClusterHealth {
    match response.content {
        Some(get_cluster_health_response::Content::Structured(s)) => s,
        _ => panic!("expected structured response"),
    }
}

#[tokio::test]
async fn cluster_health_migrating_when_migration_active() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_migrating_version(&client, 2).await.unwrap();

    let sidecar = start_mock_sidecar(unready_indexer_payload()).await;
    let checker = ClusterHealthChecker::new(Some(sidecar), Some(client));

    let s = extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

    assert_eq!(s.status, ClusterStatus::Migrating as i32);
    let migration = s
        .components
        .iter()
        .find(|c| c.name == "schema_migration")
        .expect("schema_migration component present");
    assert_eq!(migration.status, ClusterStatus::Migrating as i32);
    assert_eq!(
        migration.metrics.get("migrating_version"),
        Some(&"2".to_string())
    );

    let indexer = s.components.iter().find(|c| c.name == "indexer").unwrap();
    assert_eq!(indexer.status, ClusterStatus::Unhealthy as i32);
    assert_eq!(indexer.replicas.as_ref().unwrap().ready, 0);

    let text = match checker
        .get_cluster_health(ResponseFormat::Llm as i32)
        .await
        .content
    {
        Some(get_cluster_health_response::Content::FormattedText(t)) => t,
        _ => panic!("expected formatted text response"),
    };
    assert!(text.contains("migrating"), "TOON should report migrating");
}

#[tokio::test]
async fn cluster_health_stays_unhealthy_when_no_migration() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();

    let sidecar = start_mock_sidecar(unready_indexer_payload()).await;
    let checker = ClusterHealthChecker::new(Some(sidecar), Some(client));

    let s = extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

    assert_eq!(s.status, ClusterStatus::Unhealthy as i32);
    assert!(
        !s.components.iter().any(|c| c.name == "schema_migration"),
        "no schema_migration component without an active migration"
    );
}

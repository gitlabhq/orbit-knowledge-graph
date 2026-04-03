//! Integration tests for the indexer readiness probe.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use clickhouse_client::ArrowClickHouseClient;
use gitlab_client::GitlabClient;
use gkg_server_config::GitlabClientConfiguration;
use indexer::health::{HealthState, create_health_router};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::nats::{Nats, NatsServerCmd};
use tokio::net::TcpListener;
use tower::ServiceExt;

const CH_IMAGE: &str = "clickhouse/clickhouse-server";
const CH_TAG: &str = "25.12";
const CH_PORT: u16 = 8123;
const CH_USER: &str = "default";
const CH_PASS: &str = "testpass";

struct Infra {
    _nats_container: ContainerAsync<Nats>,
    _ch_container: ContainerAsync<GenericImage>,
    nats_client: async_nats::Client,
    ch_client: ArrowClickHouseClient,
}

async fn start_infra() -> Infra {
    let nats_cmd = NatsServerCmd::default().with_jetstream();
    let nats_container = Nats::default()
        .with_cmd(&nats_cmd)
        .with_tag("2.11-alpine")
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .with_ready_conditions(vec![WaitFor::seconds(3)])
        .start()
        .await
        .expect("failed to start NATS");

    let nats_host = nats_container.get_host().await.unwrap();
    let nats_port = nats_container.get_host_port_ipv4(4222).await.unwrap();
    let nats_client = async_nats::connect(format!("nats://{nats_host}:{nats_port}"))
        .await
        .expect("failed to connect to NATS");

    let ch_container = GenericImage::new(CH_IMAGE, CH_TAG)
        .with_exposed_port(ContainerPort::Tcp(CH_PORT))
        .with_env_var("CLICKHOUSE_USER", CH_USER)
        .with_env_var("CLICKHOUSE_PASSWORD", CH_PASS)
        .with_env_var("CLICKHOUSE_DB", "default")
        .start()
        .await
        .expect("failed to start ClickHouse");

    let ch_host = ch_container.get_host().await.unwrap();
    let ch_port = ch_container
        .get_host_port_ipv4(ContainerPort::Tcp(CH_PORT))
        .await
        .unwrap();
    let ch_url = format!("http://{ch_host}:{ch_port}");
    let ch_client = ArrowClickHouseClient::new(
        &ch_url,
        "default",
        CH_USER,
        Some(CH_PASS),
        &std::collections::HashMap::new(),
    );

    // Wait for ClickHouse to accept queries
    for attempt in 1..=30 {
        if ch_client.execute("SELECT 1").await.is_ok() {
            break;
        }
        if attempt == 30 {
            panic!("ClickHouse not ready after 30 attempts");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Infra {
        _nats_container: nats_container,
        _ch_container: ch_container,
        nats_client,
        ch_client,
    }
}

fn build_gitlab_client(base_url: &str) -> GitlabClient {
    let config = GitlabClientConfiguration {
        base_url: base_url.to_string(),
        signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
        resolve_host: None,
    };
    GitlabClient::new(config).unwrap()
}

async fn start_mock_gitlab(status: StatusCode) -> SocketAddr {
    let app = Router::new().route(
        "/api/v4/internal/orbit/project/{id}/info",
        get(move || async move { status }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

fn ready_request() -> Request<Body> {
    Request::get("/ready").body(Body::empty()).unwrap()
}

async fn parse_response(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

fn unhealthy_components(json: &serde_json::Value) -> Vec<String> {
    json["unhealthy_components"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn readiness_probe_gitlab_scenarios() {
    let infra = start_infra().await;

    // No GitLab configured — "gitlab" should not appear in unhealthy_components
    {
        let state = HealthState {
            nats_client: infra.nats_client.clone(),
            graph_client: infra.ch_client.clone(),
            datalake_client: infra.ch_client.clone(),
            gitlab_client: None,
        };
        let router = create_health_router(state);
        let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;
        let components = unhealthy_components(&json);

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert!(!components.contains(&"gitlab".to_string()));
    }

    // GitLab returns 404 — auth works, project just doesn't exist → healthy
    {
        let addr = start_mock_gitlab(StatusCode::NOT_FOUND).await;
        let state = HealthState {
            nats_client: infra.nats_client.clone(),
            graph_client: infra.ch_client.clone(),
            datalake_client: infra.ch_client.clone(),
            gitlab_client: Some(Arc::new(build_gitlab_client(&format!("http://{addr}")))),
        };
        let router = create_health_router(state);
        let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;
        let components = unhealthy_components(&json);

        assert_eq!(status, StatusCode::OK);
        assert!(!components.contains(&"gitlab".to_string()));
    }

    // GitLab returns 401 — auth broken → unhealthy
    {
        let addr = start_mock_gitlab(StatusCode::UNAUTHORIZED).await;
        let state = HealthState {
            nats_client: infra.nats_client.clone(),
            graph_client: infra.ch_client.clone(),
            datalake_client: infra.ch_client.clone(),
            gitlab_client: Some(Arc::new(build_gitlab_client(&format!("http://{addr}")))),
        };
        let router = create_health_router(state);
        let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;
        let components = unhealthy_components(&json);

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(components.contains(&"gitlab".to_string()));
    }

    // GitLab unreachable — connection refused → unhealthy
    {
        let state = HealthState {
            nats_client: infra.nats_client.clone(),
            graph_client: infra.ch_client.clone(),
            datalake_client: infra.ch_client.clone(),
            gitlab_client: Some(Arc::new(build_gitlab_client("http://127.0.0.1:1"))),
        };
        let router = create_health_router(state);
        let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;
        let components = unhealthy_components(&json);

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(components.contains(&"gitlab".to_string()));
    }
}

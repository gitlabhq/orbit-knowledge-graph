use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::{Json, Router, routing::get};
use gkg_server::auth::JwtValidator;
use gkg_server::cluster_health::ClusterHealthChecker;
use gkg_server::proto::ResponseFormat;
use gkg_server::webserver::create_router;
use health_check::{ComponentHealth, HealthStatus, ServiceHealth, Status};
use tokio::net::TcpListener;
use tower::ServiceExt;

fn test_validator() -> JwtValidator {
    JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap()
}

// ---------------------------------------------------------------------------
// Stubbed mode (no sidecar configured)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn liveness_probe_returns_ok() {
    let checker = ClusterHealthChecker::default().into_arc();
    let router = create_router(test_validator(), checker);

    let response = router
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn stubbed_cluster_health_returns_all_components() {
    let checker = ClusterHealthChecker::default().into_arc();
    let router = create_router(test_validator(), checker);

    let response = router
        .oneshot(
            Request::get("/api/v1/cluster_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "healthy");

    let components = json["components"].as_array().unwrap();
    let names: Vec<&str> = components
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"webserver"));
    assert!(names.contains(&"clickhouse"));
    assert!(names.contains(&"indexer"));

    for component in components {
        assert_eq!(component["status"], "healthy");
        assert_eq!(component["metrics"]["mode"], "stubbed");
    }
}

#[tokio::test]
async fn shared_checker_serves_same_components_over_http_and_grpc() {
    let checker = ClusterHealthChecker::default().into_arc();
    let router = create_router(test_validator(), Arc::clone(&checker));

    let http_response = router
        .oneshot(
            Request::get("/api/v1/cluster_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let http_body = axum::body::to_bytes(http_response.into_body(), 4096)
        .await
        .unwrap();
    let http_json: serde_json::Value = serde_json::from_slice(&http_body).unwrap();

    let grpc_response = checker.get_cluster_health(ResponseFormat::Raw as i32).await;

    let grpc_structured = match grpc_response.content {
        Some(gkg_server::proto::get_cluster_health_response::Content::Structured(s)) => s,
        _ => panic!("expected structured response from gRPC"),
    };

    assert_eq!(
        http_json["components"].as_array().unwrap().len(),
        grpc_structured.components.len()
    );

    let http_names: Vec<&str> = http_json["components"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    let grpc_names: Vec<&str> = grpc_structured
        .components
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(http_names, grpc_names);
}

// ---------------------------------------------------------------------------
// Real mode (sidecar returns health data over HTTP)
// ---------------------------------------------------------------------------

async fn start_mock_sidecar(health: HealthStatus) -> SocketAddr {
    let app = Router::new().route(
        "/health",
        get(move || {
            let h = health.clone();
            async move { Json(h) }
        }),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    addr
}

fn healthy_sidecar_response() -> HealthStatus {
    HealthStatus {
        status: Status::Healthy,
        services: vec![
            ServiceHealth {
                name: "webserver".to_string(),
                status: Status::Healthy,
                ready_replicas: 2,
                desired_replicas: 2,
            },
            ServiceHealth {
                name: "indexer".to_string(),
                status: Status::Healthy,
                ready_replicas: 1,
                desired_replicas: 1,
            },
        ],
        clickhouse: ComponentHealth {
            status: Status::Healthy,
            error: None,
        },
    }
}

fn degraded_sidecar_response() -> HealthStatus {
    HealthStatus {
        status: Status::Unhealthy,
        services: vec![ServiceHealth {
            name: "indexer".to_string(),
            status: Status::Unhealthy,
            ready_replicas: 0,
            desired_replicas: 2,
        }],
        clickhouse: ComponentHealth {
            status: Status::Healthy,
            error: None,
        },
    }
}

#[tokio::test]
async fn real_mode_healthy_sidecar() {
    let addr = start_mock_sidecar(healthy_sidecar_response()).await;
    let checker = ClusterHealthChecker::new(Some(format!("http://{addr}"))).into_arc();
    let router = create_router(test_validator(), Arc::clone(&checker));

    let response = router
        .oneshot(
            Request::get("/api/v1/cluster_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "healthy");

    let components = json["components"].as_array().unwrap();
    let names: Vec<&str> = components
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"webserver"));
    assert!(names.contains(&"indexer"));
    assert!(names.contains(&"clickhouse"));

    let webserver = components
        .iter()
        .find(|c| c["name"] == "webserver")
        .unwrap();
    assert_eq!(webserver["replicas"]["ready"], 2);
    assert_eq!(webserver["replicas"]["desired"], 2);
}

#[tokio::test]
async fn real_mode_unhealthy_component_propagates() {
    let addr = start_mock_sidecar(degraded_sidecar_response()).await;
    let checker = ClusterHealthChecker::new(Some(format!("http://{addr}"))).into_arc();
    let router = create_router(test_validator(), checker);

    let response = router
        .oneshot(
            Request::get("/api/v1/cluster_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "unhealthy");

    let indexer = json["components"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "indexer")
        .unwrap();
    assert_eq!(indexer["status"], "unhealthy");
    assert_eq!(indexer["replicas"]["ready"], 0);
    assert_eq!(indexer["replicas"]["desired"], 2);
}

#[tokio::test]
async fn real_mode_unreachable_sidecar_returns_unhealthy() {
    let checker = ClusterHealthChecker::new(Some("http://127.0.0.1:1".to_string())).into_arc();
    let router = create_router(test_validator(), checker);

    let response = router
        .oneshot(
            Request::get("/api/v1/cluster_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["status"], "unhealthy");

    let clickhouse = json["components"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "clickhouse")
        .unwrap();
    assert!(
        clickhouse["metrics"]["error"]
            .as_str()
            .unwrap()
            .contains("unreachable")
    );
}

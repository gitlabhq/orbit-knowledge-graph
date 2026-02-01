//! Smoke tests for labkit-rs integration.
//!
//! These tests verify that the shared labkit crate is properly integrated
//! and functioning for correlation ID propagation and metrics.

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
    routing::get,
};
use tower::ServiceExt;

/// Test that CorrelationLayer propagates incoming correlation IDs.
#[tokio::test]
async fn test_correlation_id_propagation() {
    use labkit::correlation::CorrelationLayer;

    let app = Router::new().route("/test", get(|| async { "ok" })).layer(
        CorrelationLayer::new()
            .propagate_incoming(true)
            .send_response_header(true),
    );

    let request = Request::builder()
        .uri("/test")
        .header("X-Request-ID", "test-correlation-id-12345")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // The correlation ID should be propagated to the response
    let correlation_header = response.headers().get("x-request-id");
    assert!(
        correlation_header.is_some(),
        "Response should include x-request-id header"
    );
    assert_eq!(
        correlation_header.unwrap().to_str().unwrap(),
        "test-correlation-id-12345",
        "Correlation ID should be propagated unchanged"
    );
}

/// Test that CorrelationLayer generates IDs when none provided.
#[tokio::test]
async fn test_correlation_id_generation() {
    use labkit::correlation::CorrelationLayer;

    let app = Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(CorrelationLayer::new().send_response_header(true));

    let request = Request::builder().uri("/test").body(Body::empty()).unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    // A correlation ID should be generated
    let correlation_header = response.headers().get("x-request-id");
    assert!(
        correlation_header.is_some(),
        "Response should include generated x-request-id header"
    );

    let id = correlation_header.unwrap().to_str().unwrap();
    assert!(
        !id.is_empty(),
        "Generated correlation ID should not be empty"
    );
}

/// Test that MetricsLayer doesn't panic and allows requests through.
#[tokio::test]
async fn test_metrics_layer_passthrough() {
    use labkit::metrics::MetricsLayer;

    let app = Router::new()
        .route("/health", get(|| async { "healthy" }))
        .layer(MetricsLayer::new());

    let request = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

/// Test that both layers work together (as used in production).
#[tokio::test]
async fn test_combined_layers() {
    use labkit::correlation::CorrelationLayer;
    use labkit::metrics::MetricsLayer;
    use tower_http::trace::TraceLayer;

    let app = Router::new()
        .route("/api/test", get(|| async { "success" }))
        .layer(MetricsLayer::new())
        .layer(
            CorrelationLayer::new()
                .propagate_incoming(true)
                .send_response_header(true),
        )
        .layer(TraceLayer::new_for_http());

    // Test with correlation ID
    let request = Request::builder()
        .uri("/api/test")
        .header("X-Request-ID", "combined-test-id")
        .body(Body::empty())
        .unwrap();

    let response = app.clone().oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("x-request-id")
            .unwrap()
            .to_str()
            .unwrap(),
        "combined-test-id"
    );

    // Test without correlation ID (should generate one)
    let request = Request::builder()
        .uri("/api/test")
        .body(Body::empty())
        .unwrap();

    let response = app.oneshot(request).await.unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get("x-request-id").is_some());
}

/// Test gRPC server interceptor functionality.
#[tokio::test]
async fn test_grpc_server_interceptor() {
    use labkit::correlation::grpc::server_interceptor;
    use tonic::Request;

    // Create a request without correlation ID
    let request: Request<()> = Request::new(());

    let result = server_interceptor(request);
    assert!(result.is_ok(), "Server interceptor should succeed");

    let processed = result.unwrap();

    // Check that a correlation ID was added to extensions
    let correlation_id = processed
        .extensions()
        .get::<labkit::correlation::CorrelationId>();
    assert!(
        correlation_id.is_some(),
        "Server interceptor should add CorrelationId to extensions"
    );
}

/// Test gRPC server interceptor with existing correlation ID.
#[tokio::test]
async fn test_grpc_server_interceptor_with_existing_id() {
    use labkit::correlation::grpc::server_interceptor;
    use tonic::Request;
    use tonic::metadata::MetadataValue;

    let mut request: Request<()> = Request::new(());
    request.metadata_mut().insert(
        "x-request-id",
        MetadataValue::from_static("grpc-correlation-test-id"),
    );

    let result = server_interceptor(request);
    assert!(result.is_ok(), "Server interceptor should succeed");

    let processed = result.unwrap();
    let correlation_id = processed
        .extensions()
        .get::<labkit::correlation::CorrelationId>();

    assert!(correlation_id.is_some());
    assert_eq!(
        correlation_id.unwrap().as_ref(),
        "grpc-correlation-test-id",
        "Should preserve incoming correlation ID"
    );
}

/// Test context_from_request helper.
#[tokio::test]
async fn test_context_from_request() {
    use labkit::correlation::grpc::{context_from_request, server_interceptor};
    use tonic::Request;

    let request: Request<()> = Request::new(());
    let processed = server_interceptor(request).unwrap();

    let correlation_id = context_from_request(&processed);
    assert!(
        correlation_id.is_some(),
        "context_from_request should return correlation ID from processed request"
    );
}

/// Test logging initialization (smoke test).
#[test]
fn test_logging_init_does_not_panic() {
    // Note: We can't actually call init_default() in tests as it sets global state.
    // Instead, we verify the types exist and the module is accessible.

    // This just verifies the labkit::log module is properly exported
    let _ = std::any::type_name::<labkit::log::LogGuard>();
}

/// Test CorrelationId type functionality.
#[test]
fn test_correlation_id_type() {
    use labkit::correlation::CorrelationId;

    let id = CorrelationId::new();
    assert!(!id.as_ref().is_empty(), "Generated ID should not be empty");

    let id_from_str = CorrelationId::from_string("custom-id");
    assert_eq!(id_from_str.as_ref(), "custom-id");

    let id_string = id.to_string();
    assert!(!id_string.is_empty());
}

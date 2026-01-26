//! Tests for HTTP correlation ID middleware.

use http::{Request, Response, StatusCode};
use std::convert::Infallible;
use tower::{ServiceBuilder, ServiceExt};

use crate::CorrelationId;
use crate::correlation::context::current;
use crate::correlation::http::{
    CorrelationIdLayer, InjectCorrelationIdLayer, PropagateCorrelationIdLayer, extract_from_request,
};
use crate::correlation::id::HTTP_HEADER_CORRELATION_ID;

/// A simple echo service for testing
async fn echo_handler<B>(request: Request<B>) -> Result<Response<String>, Infallible> {
    // Check if correlation ID is in context
    let from_context = current();
    // Check if correlation ID is in extensions
    let from_extensions = extract_from_request(&request);

    let body = format!(
        "context:{:?},extensions:{:?}",
        from_context.map(|id| id.to_string()),
        from_extensions.map(|id| id.to_string())
    );

    Ok(Response::builder()
        .status(StatusCode::OK)
        .body(body)
        .unwrap())
}

#[tokio::test]
async fn extracts_correlation_id_from_header() {
    let service = ServiceBuilder::new()
        .layer(CorrelationIdLayer::new())
        .service_fn(echo_handler);

    let request = Request::builder()
        .header(HTTP_HEADER_CORRELATION_ID, "test-correlation-123")
        .body(())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    let body = response.into_body();

    // Should have the ID from header in both context and extensions
    assert!(body.contains("test-correlation-123"));
}

#[tokio::test]
async fn generates_correlation_id_when_missing() {
    let service = ServiceBuilder::new()
        .layer(CorrelationIdLayer::new())
        .service_fn(echo_handler);

    let request = Request::builder().body(()).unwrap();

    let response = service.oneshot(request).await.unwrap();
    let body = response.into_body();

    // Should have generated a ULID (26 chars)
    // Context and extensions should both have values
    assert!(!body.contains("None"));
}

#[tokio::test]
async fn ignores_empty_correlation_id_header() {
    let service = ServiceBuilder::new()
        .layer(CorrelationIdLayer::new())
        .service_fn(echo_handler);

    let request = Request::builder()
        .header(HTTP_HEADER_CORRELATION_ID, "")
        .body(())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    let body = response.into_body();

    // Should have generated a new ID, not use empty string
    assert!(!body.contains(r#"Some("")"#));
    assert!(!body.contains("None"));
}

#[tokio::test]
async fn propagates_correlation_id_to_response_headers() {
    // Layer order matters: CorrelationIdLayer must be outer (runs first on request)
    // so PropagateCorrelationIdLayer (inner) can see the extensions it sets
    let service = ServiceBuilder::new()
        .layer(CorrelationIdLayer::new())
        .layer(PropagateCorrelationIdLayer::new())
        .service_fn(echo_handler);

    let request = Request::builder()
        .header(HTTP_HEADER_CORRELATION_ID, "response-test-456")
        .body(())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();

    // Response should have the correlation ID header
    let header_value = response
        .headers()
        .get(HTTP_HEADER_CORRELATION_ID)
        .expect("should have correlation ID header");

    assert_eq!(header_value.to_str().unwrap(), "response-test-456");
}

#[tokio::test]
async fn context_available_for_nested_calls() {
    // Simulates a handler that makes an outgoing request
    async fn handler_that_checks_context<B>(
        _request: Request<B>,
    ) -> Result<Response<String>, Infallible> {
        // This simulates what an outgoing request layer would do
        let id = current().expect("correlation ID should be in context");
        Ok(Response::new(id.to_string()))
    }

    let service = ServiceBuilder::new()
        .layer(CorrelationIdLayer::new())
        .service_fn(handler_that_checks_context);

    let request = Request::builder()
        .header(HTTP_HEADER_CORRELATION_ID, "nested-test-789")
        .body(())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    assert_eq!(response.into_body(), "nested-test-789");
}

#[tokio::test]
async fn inject_layer_adds_correlation_id_to_outgoing_requests() {
    use crate::correlation::context::scope;

    // Service that captures the request headers
    async fn capture_headers<B>(request: Request<B>) -> Result<Response<String>, Infallible> {
        let header = request
            .headers()
            .get(HTTP_HEADER_CORRELATION_ID)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        Ok(Response::new(header))
    }

    let service = ServiceBuilder::new()
        .layer(InjectCorrelationIdLayer::new())
        .service_fn(capture_headers);

    // Simulate being inside a request handler with correlation ID in context
    let result = scope(CorrelationId::from_string("inject-test-abc"), async {
        let request = Request::builder().body(()).unwrap();
        service.oneshot(request).await.unwrap().into_body()
    })
    .await;

    assert_eq!(result, "inject-test-abc");
}

#[tokio::test]
async fn inject_layer_generates_id_when_no_context() {
    async fn capture_headers<B>(request: Request<B>) -> Result<Response<String>, Infallible> {
        let header = request
            .headers()
            .get(HTTP_HEADER_CORRELATION_ID)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        Ok(Response::new(header))
    }

    let service = ServiceBuilder::new()
        .layer(InjectCorrelationIdLayer::new())
        .service_fn(capture_headers);

    // No correlation ID in context - should generate one
    let request = Request::builder().body(()).unwrap();
    let result = service.oneshot(request).await.unwrap().into_body();

    // Should have generated a ULID (26 chars)
    assert_eq!(result.len(), 26);
}

#[tokio::test]
async fn inject_layer_adds_client_name_when_configured() {
    use crate::correlation::id::HTTP_HEADER_CLIENT_NAME;

    async fn capture_headers<B>(request: Request<B>) -> Result<Response<String>, Infallible> {
        let client_name = request
            .headers()
            .get(HTTP_HEADER_CLIENT_NAME)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_default();
        Ok(Response::new(client_name))
    }

    let service = ServiceBuilder::new()
        .layer(InjectCorrelationIdLayer::new().with_client_name("my-service"))
        .service_fn(capture_headers);

    let request = Request::builder().body(()).unwrap();
    let result = service.oneshot(request).await.unwrap().into_body();

    assert_eq!(result, "my-service");
}

#[tokio::test]
async fn full_http_to_http_propagation() {
    // This test simulates:
    // 1. Incoming HTTP request with correlation ID
    // 2. Handler makes outgoing HTTP request
    // 3. Outgoing request should have same correlation ID

    // Inner "downstream" service that captures the correlation ID
    async fn downstream<B>(request: Request<B>) -> Result<Response<String>, Infallible> {
        let id = request
            .headers()
            .get(HTTP_HEADER_CORRELATION_ID)
            .map(|v| v.to_str().unwrap().to_string())
            .unwrap_or_else(|| "missing".to_string());
        Ok(Response::new(id))
    }

    // Handler that makes an "outgoing" request
    async fn handler<B>(_request: Request<B>) -> Result<Response<String>, Infallible> {
        // In real code, this would be an HTTP client call
        // Here we simulate by creating a service with InjectCorrelationIdLayer
        let client = ServiceBuilder::new()
            .layer(InjectCorrelationIdLayer::new())
            .service_fn(downstream);

        let outgoing_request = Request::builder().body(()).unwrap();
        let response = client.oneshot(outgoing_request).await.unwrap();
        Ok(Response::new(response.into_body()))
    }

    // The incoming request handler with CorrelationIdLayer
    let service = ServiceBuilder::new()
        .layer(CorrelationIdLayer::new())
        .service_fn(handler);

    let request = Request::builder()
        .header(HTTP_HEADER_CORRELATION_ID, "e2e-test-xyz")
        .body(())
        .unwrap();

    let response = service.oneshot(request).await.unwrap();
    let downstream_received = response.into_body();

    // The downstream service should have received the same correlation ID
    assert_eq!(downstream_received, "e2e-test-xyz");
}

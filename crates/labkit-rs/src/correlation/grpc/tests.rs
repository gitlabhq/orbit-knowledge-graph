//! Tests for gRPC correlation ID interceptors.

use futures_core::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};
use tonic::Request;
use tonic::metadata::MetadataValue;

use crate::CorrelationId;
use crate::correlation::context::{current, scope};
use crate::correlation::grpc::{
    ClientConfig, client_interceptor, create_client_interceptor, extract_from_request,
    inject_correlation_id, server_interceptor, with_correlation, with_correlation_id_stream,
};
use crate::correlation::id::{GRPC_METADATA_CLIENT_NAME, GRPC_METADATA_CORRELATION_ID};

#[test]
fn server_interceptor_extracts_correlation_id_from_metadata() {
    let mut request = Request::new(());
    request.metadata_mut().insert(
        GRPC_METADATA_CORRELATION_ID,
        MetadataValue::from_static("grpc-test-123"),
    );

    let result = server_interceptor(request).unwrap();
    let extracted = extract_from_request(&result);

    assert_eq!(extracted.unwrap().as_str(), "grpc-test-123");
}

#[test]
fn server_interceptor_generates_correlation_id_when_missing() {
    let request = Request::new(());
    let result = server_interceptor(request).unwrap();
    let extracted = extract_from_request(&result);

    // Should have generated a ULID (26 chars)
    assert!(extracted.is_some());
    assert_eq!(extracted.unwrap().as_str().len(), 26);
}

#[test]
fn server_interceptor_ignores_empty_metadata() {
    let mut request = Request::new(());
    request
        .metadata_mut()
        .insert(GRPC_METADATA_CORRELATION_ID, MetadataValue::from_static(""));

    let result = server_interceptor(request).unwrap();
    let extracted = extract_from_request(&result);

    // Should have generated a new ID, not use empty string
    assert!(extracted.is_some());
    let id = extracted.unwrap();
    assert!(!id.as_str().is_empty());
    assert_eq!(id.as_str().len(), 26); // ULID length
}

#[tokio::test]
async fn client_interceptor_injects_correlation_id_from_context() {
    let result = scope(CorrelationId::from_string("client-test-456"), async {
        let request = Request::new(());
        client_interceptor(request).unwrap()
    })
    .await;

    let metadata_value = result.metadata().get(GRPC_METADATA_CORRELATION_ID).unwrap();

    assert_eq!(metadata_value.to_str().unwrap(), "client-test-456");
}

#[tokio::test]
async fn client_interceptor_generates_id_when_no_context() {
    // No correlation ID in context
    let request = Request::new(());
    let result = client_interceptor(request).unwrap();

    let metadata_value = result.metadata().get(GRPC_METADATA_CORRELATION_ID).unwrap();

    // Should have generated a ULID
    assert_eq!(metadata_value.to_str().unwrap().len(), 26);
}

#[tokio::test]
async fn create_client_interceptor_adds_client_name() {
    let config = ClientConfig::new().with_client_name("my-grpc-service");
    let mut interceptor = create_client_interceptor(config);

    let request = Request::new(());
    let result = interceptor(request).unwrap();

    let client_name = result.metadata().get(GRPC_METADATA_CLIENT_NAME).unwrap();

    assert_eq!(client_name.to_str().unwrap(), "my-grpc-service");
}

#[test]
fn inject_correlation_id_modifies_request() {
    let mut request = Request::new(());
    let id = CorrelationId::from_string("manual-inject-789");
    inject_correlation_id(&mut request, &id);

    let metadata_value = request
        .metadata()
        .get(GRPC_METADATA_CORRELATION_ID)
        .unwrap();

    assert_eq!(metadata_value.to_str().unwrap(), "manual-inject-789");
}

#[tokio::test]
async fn with_correlation_sets_context_for_handler() {
    let mut request = Request::new(());
    request.metadata_mut().insert(
        GRPC_METADATA_CORRELATION_ID,
        MetadataValue::from_static("handler-test-abc"),
    );
    // Apply server interceptor to populate extensions
    let request = server_interceptor(request).unwrap();

    let result = with_correlation(&request, async {
        // Inside the handler, context should be available
        current().map(|id| id.to_string())
    })
    .await;

    assert_eq!(result, Some("handler-test-abc".to_string()));
}

#[tokio::test]
async fn with_correlation_enables_outgoing_propagation() {
    let mut request = Request::new(());
    request.metadata_mut().insert(
        GRPC_METADATA_CORRELATION_ID,
        MetadataValue::from_static("propagation-test-def"),
    );
    let request = server_interceptor(request).unwrap();

    let outgoing_id = with_correlation(&request, async {
        // Simulate making an outgoing gRPC call
        let outgoing_request = Request::new(());
        let result = client_interceptor(outgoing_request).unwrap();

        result
            .metadata()
            .get(GRPC_METADATA_CORRELATION_ID)
            .map(|v| v.to_str().unwrap().to_string())
    })
    .await;

    assert_eq!(outgoing_id, Some("propagation-test-def".to_string()));
}

#[tokio::test]
async fn with_correlation_generates_id_if_not_in_request() {
    // Request without correlation ID metadata
    let request = Request::new(());
    let request = server_interceptor(request).unwrap();

    let result = with_correlation(&request, async { current().map(|id| id.to_string()) }).await;

    // Should have generated a ULID
    assert!(result.is_some());
    assert_eq!(result.unwrap().len(), 26);
}

// Helper stream for testing
struct TestStream {
    items: Vec<i32>,
    index: usize,
}

impl TestStream {
    fn new(items: Vec<i32>) -> Self {
        Self { items, index: 0 }
    }
}

impl Stream for TestStream {
    type Item = i32;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.index < self.items.len() {
            let item = self.items[self.index];
            self.index += 1;
            Poll::Ready(Some(item))
        } else {
            Poll::Ready(None)
        }
    }
}

#[tokio::test]
async fn with_correlation_stream_provides_context_during_poll() {
    use tokio_stream::StreamExt;

    let correlation_id = Some(CorrelationId::from_string("stream-test-ghi"));

    // Create a stream that captures the correlation ID during each poll
    struct ContextCapturingStream;

    impl Stream for ContextCapturingStream {
        type Item = Option<String>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Capture whatever correlation ID is in context right now
            let id = current().map(|id| id.to_string());
            Poll::Ready(Some(id))
        }
    }

    let wrapped_stream = with_correlation_id_stream(correlation_id, ContextCapturingStream);
    let mut wrapped_stream = Box::pin(wrapped_stream);

    // Poll once and check the captured ID
    let captured = wrapped_stream.next().await.flatten();
    assert_eq!(captured, Some("stream-test-ghi".to_string()));
}

#[tokio::test]
async fn with_correlation_stream_works_with_real_stream() {
    let correlation_id = Some(CorrelationId::from_string("real-stream-jkl"));
    let inner_stream = TestStream::new(vec![1, 2, 3]);

    let wrapped_stream = with_correlation_id_stream(correlation_id, inner_stream);
    let items: Vec<_> = tokio_stream::StreamExt::collect(wrapped_stream).await;

    assert_eq!(items, vec![1, 2, 3]);
}

#[tokio::test]
async fn with_correlation_stream_generates_id_if_none() {
    struct ContextCapturingStream {
        polled: bool,
    }

    impl Stream for ContextCapturingStream {
        type Item = String;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.polled {
                return Poll::Ready(None);
            }
            self.polled = true;
            let id = current().map(|id| id.to_string()).unwrap_or_default();
            Poll::Ready(Some(id))
        }
    }

    // Pass None - should generate an ID
    let wrapped_stream = with_correlation_id_stream(None, ContextCapturingStream { polled: false });
    let items: Vec<_> = tokio_stream::StreamExt::collect(wrapped_stream).await;

    assert_eq!(items.len(), 1);
    // Should have generated a ULID (26 chars)
    assert_eq!(items[0].len(), 26);
}

#[tokio::test]
async fn full_grpc_to_grpc_propagation() {
    // Simulates:
    // 1. Incoming gRPC request with correlation ID
    // 2. Handler uses with_correlation wrapper
    // 3. Handler makes outgoing gRPC request
    // 4. Outgoing request should have same correlation ID

    let mut incoming = Request::new(());
    incoming.metadata_mut().insert(
        GRPC_METADATA_CORRELATION_ID,
        MetadataValue::from_static("e2e-grpc-mno"),
    );
    let incoming = server_interceptor(incoming).unwrap();

    let outgoing_id = with_correlation(&incoming, async {
        // Make an "outgoing" gRPC call
        let outgoing = Request::new(());
        let result = client_interceptor(outgoing).unwrap();

        result
            .metadata()
            .get(GRPC_METADATA_CORRELATION_ID)
            .map(|v| v.to_str().unwrap().to_string())
    })
    .await;

    assert_eq!(outgoing_id, Some("e2e-grpc-mno".to_string()));
}

#[tokio::test]
async fn full_grpc_to_http_propagation() {
    use http::Request as HttpRequest;
    use http::Response as HttpResponse;
    use std::convert::Infallible;
    use tower::{ServiceBuilder, ServiceExt};

    use crate::correlation::http::InjectCorrelationIdLayer;
    use crate::correlation::id::HTTP_HEADER_CORRELATION_ID;

    // Simulates:
    // 1. Incoming gRPC request with correlation ID
    // 2. Handler uses with_correlation wrapper
    // 3. Handler makes outgoing HTTP request
    // 4. Outgoing HTTP request should have same correlation ID

    let mut incoming = Request::new(());
    incoming.metadata_mut().insert(
        GRPC_METADATA_CORRELATION_ID,
        MetadataValue::from_static("grpc-to-http-pqr"),
    );
    let incoming = server_interceptor(incoming).unwrap();

    let http_correlation_id = with_correlation(&incoming, async {
        // Simulate HTTP client with InjectCorrelationIdLayer
        async fn capture_header<B>(
            request: HttpRequest<B>,
        ) -> Result<HttpResponse<String>, Infallible> {
            let id = request
                .headers()
                .get(HTTP_HEADER_CORRELATION_ID)
                .map(|v| v.to_str().unwrap().to_string())
                .unwrap_or_default();
            Ok(HttpResponse::new(id))
        }

        let http_client = ServiceBuilder::new()
            .layer(InjectCorrelationIdLayer::new())
            .service_fn(capture_header);

        let http_request = HttpRequest::builder().body(()).unwrap();
        http_client.oneshot(http_request).await.unwrap().into_body()
    })
    .await;

    assert_eq!(http_correlation_id, "grpc-to-http-pqr");
}

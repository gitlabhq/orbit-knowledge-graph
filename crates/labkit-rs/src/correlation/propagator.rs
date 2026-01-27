//! Header extraction and injection for correlation ID propagation.

use opentelemetry::{Context, KeyValue, baggage::BaggageExt};

use crate::correlation::context::BAGGAGE_CORRELATION_ID;
use crate::correlation::id::{
    CorrelationId, GRPC_METADATA_CORRELATION_ID, HTTP_HEADER_CORRELATION_ID,
};

pub fn extract_from_http_headers(headers: &http::HeaderMap) -> Option<CorrelationId> {
    headers
        .get(HTTP_HEADER_CORRELATION_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| CorrelationId::from_string(s.to_string()))
}

pub fn inject_to_http_headers(headers: &mut http::HeaderMap, id: &CorrelationId) {
    if let Ok(value) = http::HeaderValue::from_str(id.as_str()) {
        headers.insert(HTTP_HEADER_CORRELATION_ID, value);
    }
}

pub fn inject_client_name_to_http_headers(headers: &mut http::HeaderMap, name: &str) {
    use crate::correlation::id::HTTP_HEADER_CLIENT_NAME;
    if let Ok(value) = http::HeaderValue::from_str(name) {
        headers.insert(HTTP_HEADER_CLIENT_NAME, value);
    }
}

#[cfg(feature = "grpc")]
pub fn extract_from_grpc_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Option<CorrelationId> {
    metadata
        .get(GRPC_METADATA_CORRELATION_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(|s| CorrelationId::from_string(s.to_string()))
}

#[cfg(feature = "grpc")]
pub fn inject_to_grpc_metadata(metadata: &mut tonic::metadata::MetadataMap, id: &CorrelationId) {
    if let Ok(value) = id.as_str().parse() {
        metadata.insert(GRPC_METADATA_CORRELATION_ID, value);
    }
}

#[cfg(feature = "grpc")]
pub fn inject_client_name_to_grpc_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    name: &str,
) {
    use crate::correlation::id::GRPC_METADATA_CLIENT_NAME;
    if let Ok(value) = name.parse() {
        metadata.insert(GRPC_METADATA_CLIENT_NAME, value);
    }
}

pub fn ensure_correlation_id(cx: Context) -> (Context, CorrelationId) {
    if let Some(id) = cx
        .baggage()
        .get(BAGGAGE_CORRELATION_ID)
        .map(|v| CorrelationId::from_string(v.to_string()))
    {
        (cx, id)
    } else {
        let id = CorrelationId::generate();
        let cx = cx.with_baggage(vec![KeyValue::new(
            BAGGAGE_CORRELATION_ID,
            id.clone().into_string(),
        )]);
        (cx, id)
    }
}

pub fn context_with_id(id: CorrelationId) -> Context {
    Context::current().with_baggage(vec![KeyValue::new(
        BAGGAGE_CORRELATION_ID,
        id.into_string(),
    )])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_from_http() {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-request-id", "test-123".parse().unwrap());
        let id = extract_from_http_headers(&headers);
        assert_eq!(id.map(|i| i.into_string()), Some("test-123".to_string()));
    }

    #[test]
    fn inject_to_http() {
        let mut headers = http::HeaderMap::new();
        let id = CorrelationId::from_string("inject-test".to_string());
        inject_to_http_headers(&mut headers, &id);
        assert_eq!(
            headers.get("x-request-id").map(|v| v.to_str().unwrap()),
            Some("inject-test")
        );
    }

    #[test]
    fn ensure_generates_when_missing() {
        let cx = Context::new();
        let (cx, id) = ensure_correlation_id(cx);
        assert_eq!(id.as_str().len(), 26);
        let extracted = cx
            .baggage()
            .get(BAGGAGE_CORRELATION_ID)
            .map(|v| v.to_string());
        assert_eq!(extracted, Some(id.into_string()));
    }

    #[test]
    fn ensure_preserves_existing() {
        let cx = Context::new().with_baggage(vec![KeyValue::new(
            BAGGAGE_CORRELATION_ID,
            "existing".to_string(),
        )]);
        let (_, id) = ensure_correlation_id(cx);
        assert_eq!(id.into_string(), "existing");
    }
}

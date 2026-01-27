//! Client-side gRPC interceptor for correlation ID injection.

use opentelemetry::Context as OtelContext;
use tonic::{Request, Status};

use crate::correlation::id::CorrelationId;
use crate::correlation::propagator::{
    ensure_correlation_id, inject_client_name_to_grpc_metadata, inject_to_grpc_metadata,
};

/// Client interceptor that injects correlation ID into outgoing gRPC requests.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::client_interceptor;
///
/// let channel = Channel::from_static("http://[::1]:50051").connect().await?;
/// let client = MyServiceClient::with_interceptor(channel, client_interceptor);
/// ```
pub fn client_interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    let (_, id) = ensure_correlation_id(OtelContext::current());
    inject_to_grpc_metadata(request.metadata_mut(), &id);
    Ok(request)
}

pub fn inject_correlation_id<T>(request: &mut Request<T>, correlation_id: &CorrelationId) {
    inject_to_grpc_metadata(request.metadata_mut(), correlation_id);
}

#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    pub client_name: Option<String>,
}

impl ClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }
}

/// Create a configurable client interceptor.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::{ClientConfig, create_client_interceptor};
///
/// let interceptor = create_client_interceptor(ClientConfig::new().with_client_name("my-service"));
/// let client = MyServiceClient::with_interceptor(channel, interceptor);
/// ```
pub fn create_client_interceptor(
    config: ClientConfig,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut request: Request<()>| {
        let (_, id) = ensure_correlation_id(OtelContext::current());
        inject_to_grpc_metadata(request.metadata_mut(), &id);
        if let Some(ref name) = config.client_name {
            inject_client_name_to_grpc_metadata(request.metadata_mut(), name);
        }
        Ok(request)
    }
}

pub fn inject_to_request<T>(request: &mut Request<T>, correlation_id: &CorrelationId) {
    inject_correlation_id(request, correlation_id);
}

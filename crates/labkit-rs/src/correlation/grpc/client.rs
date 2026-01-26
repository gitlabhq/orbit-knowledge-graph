//! Client-side gRPC interceptor for correlation ID injection.
//!
//! Injects correlation IDs into outgoing gRPC requests.

use tonic::metadata::MetadataValue;
use tonic::{Request, Status};

use crate::correlation::context::current_or_generate;
use crate::correlation::id::{
    CorrelationId, GRPC_METADATA_CLIENT_NAME, GRPC_METADATA_CORRELATION_ID,
};

/// Client interceptor that injects correlation ID into outgoing gRPC requests.
///
/// Gets the correlation ID from the task-local context (if available) or
/// generates a new one. Injects into the `x-gitlab-correlation-id` metadata key.
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
    let correlation_id = current_or_generate();
    inject_correlation_id(&mut request, &correlation_id);
    Ok(request)
}

/// Inject a correlation ID into a gRPC request.
///
/// Use this when you need to explicitly inject a known correlation ID,
/// rather than getting it from the task-local context.
pub fn inject_correlation_id<T>(request: &mut Request<T>, correlation_id: &CorrelationId) {
    if let Ok(value) = correlation_id.as_str().parse::<MetadataValue<_>>() {
        request
            .metadata_mut()
            .insert(GRPC_METADATA_CORRELATION_ID, value);
    }
}

/// Configuration for the client interceptor.
#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    /// Client name to include in outgoing requests.
    ///
    /// This will be sent in the `x-gitlab-client-name` metadata key.
    pub client_name: Option<String>,
}

impl ClientConfig {
    /// Create a new client configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the client name.
    #[must_use]
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }
}

/// Create a configurable client interceptor.
///
/// Use this when you need to include a client name in outgoing requests.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::{ClientConfig, create_client_interceptor};
///
/// let config = ClientConfig::new().with_client_name("my-service");
/// let interceptor = create_client_interceptor(config);
/// let client = MyServiceClient::with_interceptor(channel, interceptor);
/// ```
pub fn create_client_interceptor(
    config: ClientConfig,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |mut request: Request<()>| {
        let correlation_id = current_or_generate();
        inject_correlation_id(&mut request, &correlation_id);

        // Inject client name if configured
        if let Some(ref name) = config.client_name
            && let Ok(value) = name.parse::<MetadataValue<_>>()
        {
            request
                .metadata_mut()
                .insert(GRPC_METADATA_CLIENT_NAME, value);
        }

        Ok(request)
    }
}

/// Inject correlation ID into a typed request.
///
/// Convenience function for injecting correlation ID into any request type.
/// Use this when building requests manually.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::inject_to_request;
/// use labkit_rs::CorrelationId;
///
/// let mut req = Request::new(MyMessage { ... });
/// let id = CorrelationId::generate();
/// inject_to_request(&mut req, &id);
/// ```
pub fn inject_to_request<T>(request: &mut Request<T>, correlation_id: &CorrelationId) {
    inject_correlation_id(request, correlation_id);
}

//! labkit-rs - Observability utilities for Rust
//!
//! This crate provides observability utilities for distributed systems,
//! including correlation ID propagation for HTTP and gRPC services using
//! OpenTelemetry context and baggage.
//!
//! # Features
//!
//! - `http` - Tower layers for HTTP middleware (Axum, etc.)
//! - `grpc` - Tonic interceptors for gRPC
//! - `full` - All features enabled
//!
//! # Correlation IDs
//!
//! Correlation IDs are ULID-based identifiers that trace requests across service
//! boundaries. This crate uses OpenTelemetry's context and baggage for propagation,
//! enabling unified context across HTTP and gRPC:
//!
//! - Generation of unique ULID-based correlation IDs
//! - Extraction from incoming HTTP headers (`X-Request-Id`) and gRPC metadata
//! - Injection into outgoing requests (cross-protocol: gRPC → HTTP and vice versa)
//! - OpenTelemetry context-based storage for async propagation
//!
//! # Example (HTTP with Axum)
//!
//! ```rust,ignore
//! use axum::Router;
//! use labkit_rs::correlation::http::{CorrelationIdLayer, PropagateCorrelationIdLayer};
//!
//! let app = Router::new()
//!     .route("/", get(handler))
//!     .layer(PropagateCorrelationIdLayer::new())
//!     .layer(CorrelationIdLayer::new());
//! ```
//!
//! # Example (gRPC with Tonic)
//!
//! ```rust,ignore
//! use labkit_rs::correlation::grpc::server_interceptor;
//!
//! let service = MyServiceServer::with_interceptor(my_service, server_interceptor);
//! ```

pub mod correlation;
pub mod logging;

// Re-export commonly used items
pub use correlation::context;
pub use correlation::id::{
    CorrelationId, GRPC_METADATA_CLIENT_NAME, GRPC_METADATA_CORRELATION_ID,
    HTTP_HEADER_CLIENT_NAME, HTTP_HEADER_CORRELATION_ID, LOG_FIELD_CORRELATION_ID,
};
pub use logging::{Format, LogConfig, init_logging};

// Re-export OpenTelemetry types for convenience
pub use opentelemetry::Context as OtelContext;

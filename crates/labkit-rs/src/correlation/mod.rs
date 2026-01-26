//! Correlation ID generation and propagation.
//!
//! This module provides correlation ID support for distributed tracing across
//! HTTP and gRPC services.

pub mod context;
pub mod id;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "grpc")]
pub mod grpc;

pub use id::CorrelationId;

//! Correlation ID generation and propagation.

pub mod context;
pub mod id;
mod propagator;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "grpc")]
pub mod grpc;

pub use id::CorrelationId;

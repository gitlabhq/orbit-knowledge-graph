//! HTTP middleware for correlation ID propagation.
//!
//! This module provides Tower layers for extracting and injecting correlation
//! IDs in HTTP requests and responses.
//!
//! # Inbound Requests
//!
//! Use [`CorrelationIdLayer`] to extract the correlation ID from incoming
//! requests or generate a new one:
//!
//! ```rust,ignore
//! use axum::Router;
//! use labkit_rs::correlation::http::CorrelationIdLayer;
//!
//! let app = Router::new()
//!     .route("/", get(handler))
//!     .layer(CorrelationIdLayer::new());
//! ```
//!
//! # Response Headers
//!
//! Use [`PropagateCorrelationIdLayer`] to copy the correlation ID to response
//! headers, enabling clients to correlate responses:
//!
//! ```rust,ignore
//! let app = Router::new()
//!     .layer(PropagateCorrelationIdLayer::new())
//!     .layer(CorrelationIdLayer::new());
//! ```
//!
//! # Outbound Requests
//!
//! Use [`InjectCorrelationIdLayer`] to inject correlation IDs into outgoing
//! HTTP client requests.

mod extract;
mod inject;

#[cfg(test)]
mod tests;

pub use extract::{CorrelationIdLayer, CorrelationIdService, extract_from_request};
pub use inject::{
    InjectCorrelationIdLayer, InjectCorrelationIdService, PropagateCorrelationIdLayer,
    PropagateCorrelationIdService,
};

//! OpenTelemetry metrics for HTTP and gRPC services.
//!
//! This module provides OTel-native metrics with OTLP export. Metrics are pushed
//! to an OpenTelemetry Collector, which can expose them for Prometheus scraping.
//!
//! # Features
//!
//! - `metrics` - Core metrics with OTLP export
//! - `metrics-http` - HTTP server metrics (Tower layer)
//! - `metrics-grpc` - gRPC server metrics wrapper
//!
//! # Initialization
//!
//! ```rust,ignore
//! use labkit_rs::metrics;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Returns a guard - metrics flush when guard is dropped
//!     let _metrics = metrics::init();
//!
//!     // ... run application ...
//!
//! } // metrics automatically flushed here
//! ```
//!
//! # HTTP Metrics (Tower/Axum)
//!
//! ```rust,ignore
//! use axum::Router;
//! use labkit_rs::metrics::http::HttpMetricsLayer;
//!
//! let app = Router::new()
//!     .route("/", get(handler))
//!     .layer(HttpMetricsLayer::new());
//! ```
//!
//! # gRPC Metrics (Tonic)
//!
//! ```rust,ignore
//! use labkit_rs::metrics::grpc::GrpcMetrics;
//! use std::sync::LazyLock;
//!
//! static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);
//!
//! async fn my_handler(req: Request<Msg>) -> Result<Response<Reply>, Status> {
//!     METRICS.record("MyService", "MyMethod", || async {
//!         Ok(Response::new(Reply { ... }))
//!     }).await
//! }
//! ```

mod config;
mod init;
mod instruments;

#[cfg(feature = "metrics-grpc")]
pub mod grpc;
#[cfg(feature = "metrics-http")]
pub mod http;

pub use config::MetricsConfig;
pub use init::{InitError, MetricsGuard, init, init_with_config, try_init, try_init_with_config};
pub use instruments::{
    DURATION_BUCKETS, GrpcServerInstruments, HttpServerInstruments, SIZE_BUCKETS,
};

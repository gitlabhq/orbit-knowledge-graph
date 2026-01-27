//! Structured logging with automatic correlation ID injection.
//!
//! This module provides a logging system that automatically includes correlation IDs
//! from task-local context in all log events. It supports both human-readable text
//! format for development and JSON format for production/Kubernetes environments.
//!
//! # Quick Start
//!
//! ```rust,ignore
//! use labkit_rs::logging;
//!
//! fn main() {
//!     // Initialize with defaults (text format, info level)
//!     logging::init();
//!
//!     // Or with custom configuration
//!     logging::init_with_config(
//!         logging::LogConfig::json().with_level("debug")
//!     );
//!
//!     tracing::info!("Application started");
//! }
//! ```
//!
//! # Automatic Correlation ID
//!
//! When code runs within a correlation context (via [`crate::correlation::context::scope`]),
//! all log messages automatically include the `correlation_id` field:
//!
//! ```rust,ignore
//! use labkit_rs::correlation::{CorrelationId, context};
//!
//! async fn handle_request() {
//!     let id = CorrelationId::generate();
//!     context::scope(id, async {
//!         // This log will include correlation_id automatically
//!         tracing::info!("Processing request");
//!     }).await;
//! }
//! ```
//!
//! # Output Formats
//!
//! **Text format** (development):
//! ```text
//! 2024-01-15T10:30:00Z INFO myapp::handler correlation_id=01HQ... Request received
//! ```
//!
//! **JSON format** (production):
//! ```json
//! {"timestamp":"2024-01-15T10:30:00Z","level":"INFO","target":"myapp::handler","correlation_id":"01HQ...","message":"Request received"}
//! ```
//!
//! # Environment Variables
//!
//! - `RUST_LOG` - Standard tracing-subscriber level filter (e.g., "debug", "info", "myapp=trace")
//! - `LOG_FORMAT` - Output format: "json" or "text" (default: text)

mod config;
mod init;
mod layer;

#[cfg(test)]
mod tests;

pub use config::{Format, LogConfig};
pub use init::{InitError, init, init_with_config, try_init, try_init_with_config};
pub use layer::{CorrelationIdJsonFormatter, CorrelationIdTextFormatter};

/// Alias for [`init`] for use with `labkit_rs::init_logging()`.
pub use init::init as init_logging;

//! Logging initialization functions.

use super::config::{Format, LogConfig};
use super::layer::{CorrelationIdJsonFormatter, CorrelationIdTextFormatter};
use std::io;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Error type for logging initialization failures.
#[derive(Debug, thiserror::Error)]
pub enum InitError {
    /// The global subscriber has already been set.
    #[error("global tracing subscriber already set")]
    AlreadySet,
    /// Failed to parse the log level filter.
    #[error("failed to parse log level filter: {0}")]
    InvalidFilter(#[from] tracing_subscriber::filter::ParseError),
}

/// Initialize logging with default configuration.
///
/// This is a convenience function that calls [`try_init`] and panics on failure.
/// Use this in application `main()` functions.
///
/// # Panics
///
/// Panics if the global subscriber has already been set.
///
/// # Example
///
/// ```rust,ignore
/// fn main() {
///     labkit_rs::logging::init();
///     tracing::info!("Application started");
/// }
/// ```
pub fn init() {
    try_init().expect("failed to initialize logging");
}

/// Initialize logging with custom configuration.
///
/// This is a convenience function that calls [`try_init_with_config`] and panics on failure.
///
/// # Panics
///
/// Panics if the global subscriber has already been set or if the log level is invalid.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::logging::{init_with_config, LogConfig};
///
/// fn main() {
///     init_with_config(LogConfig::json().with_level("debug"));
///     tracing::info!("Application started");
/// }
/// ```
pub fn init_with_config(config: LogConfig) {
    try_init_with_config(config).expect("failed to initialize logging");
}

/// Try to initialize logging with default configuration.
///
/// Returns an error if the global subscriber has already been set.
///
/// # Errors
///
/// Returns `InitError::AlreadySet` if a global subscriber is already registered.
pub fn try_init() -> Result<(), InitError> {
    try_init_with_config(LogConfig::default())
}

/// Try to initialize logging with custom configuration.
///
/// Returns an error if the global subscriber has already been set or if
/// the log level filter is invalid.
///
/// # Errors
///
/// - `InitError::AlreadySet` if a global subscriber is already registered.
/// - `InitError::InvalidFilter` if the log level string is invalid.
pub fn try_init_with_config(config: LogConfig) -> Result<(), InitError> {
    let filter = build_filter(&config)?;
    match config.format {
        Format::Json => init_json(filter),
        Format::Text => init_text(filter),
    }
}

fn build_filter(config: &LogConfig) -> Result<EnvFilter, InitError> {
    if let Some(ref level) = config.level {
        Ok(EnvFilter::try_new(level)?)
    } else {
        Ok(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
    }
}

fn init_json(filter: EnvFilter) -> Result<(), InitError> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .event_format(CorrelationIdJsonFormatter)
        .with_filter(filter);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .try_init()
        .map_err(|_| InitError::AlreadySet)
}

fn init_text(filter: EnvFilter) -> Result<(), InitError> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_writer(io::stderr)
        .event_format(CorrelationIdTextFormatter)
        .with_filter(filter);

    tracing_subscriber::registry()
        .with(fmt_layer)
        .try_init()
        .map_err(|_| InitError::AlreadySet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_filter_with_level() {
        let config = LogConfig::text().with_level("debug");
        let result = build_filter(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn build_filter_default() {
        let config = LogConfig::new();
        let result = build_filter(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn build_filter_invalid_syntax() {
        // EnvFilter rejects invalid directive syntax like mismatched brackets
        let config = LogConfig::new().with_level("target[");
        let result = build_filter(&config);
        assert!(result.is_err());
    }
}

//! Logging configuration types.

use std::env;
use std::str::FromStr;

/// Output format for log messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// Human-readable text format (default for development).
    #[default]
    Text,
    /// JSON format for production/k8s environments.
    Json,
}

impl FromStr for Format {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("json") {
            Ok(Self::Json)
        } else {
            Ok(Self::Text)
        }
    }
}

impl Format {
    #[must_use]
    pub fn from_env() -> Self {
        env::var("LOG_FORMAT")
            .map(|v| v.parse().unwrap_or_default())
            .unwrap_or_default()
    }
}

/// Configuration for the logging system.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Output format (text or JSON).
    pub format: Format,
    /// Log level filter (e.g., "info", "debug", "warn").
    /// If None, uses RUST_LOG env var or defaults to "info".
    pub level: Option<String>,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            format: Format::from_env(),
            level: None,
        }
    }
}

impl LogConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn json() -> Self {
        Self {
            format: Format::Json,
            level: None,
        }
    }

    #[must_use]
    pub fn text() -> Self {
        Self {
            format: Format::Text,
            level: None,
        }
    }

    #[must_use]
    pub fn with_level(mut self, level: impl Into<String>) -> Self {
        self.level = Some(level.into());
        self
    }

    #[must_use]
    pub fn with_format(mut self, format: Format) -> Self {
        self.format = format;
        self
    }
}

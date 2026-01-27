use std::env;
use std::time::Duration;

const DEFAULT_EXPORT_INTERVAL: Duration = Duration::from_secs(60);
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";

/// Configuration for the metrics system.
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub otlp_endpoint: String,
    pub service_name: String,
    pub export_interval: Duration,
    pub record_body_size: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| DEFAULT_OTLP_ENDPOINT.to_string()),
            service_name: env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| "unknown-service".to_string()),
            export_interval: DEFAULT_EXPORT_INTERVAL,
            record_body_size: false,
        }
    }
}

impl MetricsConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = endpoint.into();
        self
    }

    #[must_use]
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    #[must_use]
    pub fn with_export_interval(mut self, interval: Duration) -> Self {
        self.export_interval = interval;
        self
    }

    #[must_use]
    pub fn with_body_size_recording(mut self, enabled: bool) -> Self {
        self.record_body_size = enabled;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_builder() {
        let config = MetricsConfig::new()
            .with_endpoint("http://otel:4317")
            .with_service_name("test-service")
            .with_export_interval(Duration::from_secs(30))
            .with_body_size_recording(true);

        assert_eq!(config.otlp_endpoint, "http://otel:4317");
        assert_eq!(config.service_name, "test-service");
        assert_eq!(config.export_interval, Duration::from_secs(30));
        assert!(config.record_body_size);
    }
}

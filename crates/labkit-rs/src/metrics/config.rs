use std::time::Duration;

use serde::{Deserialize, Serialize};

const DEFAULT_EXPORT_INTERVAL_SECS: u64 = 60;
const DEFAULT_OTLP_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_SERVICE_NAME: &str = "unknown-service";

fn default_otlp_endpoint() -> String {
    DEFAULT_OTLP_ENDPOINT.to_string()
}

fn default_service_name() -> String {
    DEFAULT_SERVICE_NAME.to_string()
}

fn default_export_interval_secs() -> u64 {
    DEFAULT_EXPORT_INTERVAL_SECS
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_otlp_endpoint")]
    pub otlp_endpoint: String,
    #[serde(default = "default_service_name")]
    pub service_name: String,
    #[serde(default = "default_export_interval_secs")]
    pub export_interval_secs: u64,
    #[serde(default)]
    pub record_body_size: bool,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: DEFAULT_OTLP_ENDPOINT.to_string(),
            service_name: DEFAULT_SERVICE_NAME.to_string(),
            export_interval_secs: DEFAULT_EXPORT_INTERVAL_SECS,
            record_body_size: false,
        }
    }
}

impl MetricsConfig {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn export_interval(&self) -> Duration {
        Duration::from_secs(self.export_interval_secs)
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
        self.export_interval_secs = interval.as_secs();
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
        assert_eq!(config.export_interval_secs, 30);
        assert!(config.record_body_size);
    }
}

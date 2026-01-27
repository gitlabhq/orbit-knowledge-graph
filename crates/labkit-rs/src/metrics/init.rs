use opentelemetry::global;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::resource::{
    EnvResourceDetector, SdkProvidedResourceDetector, TelemetryResourceDetector,
};

use super::config::MetricsConfig;

#[derive(Debug, thiserror::Error)]
pub enum InitError {
    #[error("failed to create OTLP exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// Guard that shuts down the metrics provider when dropped.
///
/// Hold this guard for the lifetime of your application to ensure metrics
/// are properly flushed on shutdown.
///
/// # Example
///
/// ```rust,ignore
/// #[tokio::main]
/// async fn main() {
///     let _metrics = labkit_rs::metrics::init();
///
///     // ... run application ...
///
/// } // metrics automatically flushed here
/// ```
pub struct MetricsGuard {
    provider: SdkMeterProvider,
}

impl Drop for MetricsGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            tracing::warn!(error = %e, "failed to shutdown metrics provider");
        }
    }
}

/// Initialize metrics with default configuration.
///
/// Returns a guard that flushes metrics when dropped.
///
/// Uses `OTEL_EXPORTER_OTLP_ENDPOINT` and `OTEL_SERVICE_NAME` environment
/// variables for configuration.
///
/// # Panics
///
/// Panics if initialization fails.
#[must_use]
pub fn init() -> MetricsGuard {
    try_init().expect("failed to initialize metrics")
}

/// Initialize metrics with custom configuration.
///
/// Returns a guard that flushes metrics when dropped.
///
/// # Panics
///
/// Panics if initialization fails.
#[must_use]
pub fn init_with_config(config: MetricsConfig) -> MetricsGuard {
    try_init_with_config(config).expect("failed to initialize metrics")
}

/// Try to initialize metrics with default configuration.
///
/// Returns a guard that flushes metrics when dropped.
///
/// # Errors
///
/// Returns an error if the OTLP exporter cannot be created.
pub fn try_init() -> Result<MetricsGuard, InitError> {
    try_init_with_config(MetricsConfig::default())
}

/// Try to initialize metrics with custom configuration.
///
/// Returns a guard that flushes metrics when dropped.
///
/// # Errors
///
/// Returns an error if the OTLP exporter cannot be created.
pub fn try_init_with_config(config: MetricsConfig) -> Result<MetricsGuard, InitError> {
    let resource = Resource::builder()
        .with_detector(Box::new(SdkProvidedResourceDetector))
        .with_detector(Box::new(EnvResourceDetector::default()))
        .with_detector(Box::new(TelemetryResourceDetector))
        .with_service_name(config.service_name)
        .build();

    let exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint)
        .build()?;

    let reader = PeriodicReader::builder(exporter)
        .with_interval(config.export_interval)
        .build();

    let provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_reader(reader)
        .build();

    global::set_meter_provider(provider.clone());

    Ok(MetricsGuard { provider })
}

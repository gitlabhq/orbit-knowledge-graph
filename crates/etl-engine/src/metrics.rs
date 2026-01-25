//! Metrics collection for engine observability.
//!
//! Implement [`MetricCollector`] for your metrics backend (Prometheus, StatsD, etc.).
//!
//! # Example
//!
//! ```ignore
//! use etl_engine::metrics::{MetricCollector, NoopMetricCollector};
//! use std::sync::Arc;
//!
//! // Use NoopMetricCollector when metrics are disabled
//! let metrics: Arc<dyn MetricCollector> = Arc::new(NoopMetricCollector);
//!
//! // Or implement MetricCollector for your backend
//! struct MyMetrics { /* ... */ }
//!
//! impl MetricCollector for MyMetrics {
//!     fn increment(&self, name: &str, tags: &[(&str, &str)]) {
//!         // Send to your metrics backend
//!     }
//!
//!     fn gauge(&self, name: &str, value: f64, tags: &[(&str, &str)]) {
//!         // Send to your metrics backend
//!     }
//!
//!     fn histogram(&self, name: &str, value: f64, tags: &[(&str, &str)]) {
//!         // Send to your metrics backend
//!     }
//! }
//! ```

/// A trait for collecting metrics.
///
/// Implement this trait to integrate with your metrics backend (Prometheus,
/// StatsD, OpenTelemetry, etc.). Handlers receive the collector via
/// [`HandlerContext`](crate::module::HandlerContext) and can call these methods.
///
/// # Thread safety
///
/// Implementations must be `Send + Sync` because multiple handlers may record
/// metrics concurrently.
pub trait MetricCollector: Send + Sync {
    /// Increments a counter by 1.
    ///
    /// Use for counting events like messages received, errors, etc.
    fn increment(&self, name: &str, tags: &[(&str, &str)]);

    /// Sets a gauge to a specific value.
    ///
    /// Use for values that go up and down, like active handlers or queue depth.
    fn gauge(&self, name: &str, value: f64, tags: &[(&str, &str)]);

    /// Records a value in a histogram.
    ///
    /// Use for distributions like latencies, message sizes, etc.
    fn histogram(&self, name: &str, value: f64, tags: &[(&str, &str)]);
}

/// A no-op metric collector that discards all metrics.
///
/// Used as the default when no metrics backend is configured.
pub struct NoopMetricCollector;

impl MetricCollector for NoopMetricCollector {
    #[inline]
    fn increment(&self, _name: &str, _tags: &[(&str, &str)]) {}

    #[inline]
    fn gauge(&self, _name: &str, _value: f64, _tags: &[(&str, &str)]) {}

    #[inline]
    fn histogram(&self, _name: &str, _value: f64, _tags: &[(&str, &str)]) {}
}

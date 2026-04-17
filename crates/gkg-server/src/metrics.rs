//! Server-level OTel metrics.
//!
//! Counters and histograms that apply to the server as a whole, not to a
//! specific pipeline stage. Subsystem-specific metrics (query pipeline,
//! content resolution) stay in their own `metrics.rs` files.

use std::sync::LazyLock;

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::Counter;

static METRICS: LazyLock<ServerMetrics> = LazyLock::new(ServerMetrics::new);

struct ServerMetrics {
    rate_limit_rejected: Counter<u64>,
    stream_timed_out: Counter<u64>,
}

impl ServerMetrics {
    fn new() -> Self {
        let meter = global::meter("gkg_server");
        Self {
            rate_limit_rejected: meter
                .u64_counter("gkg.server.rate_limit.rejected")
                .with_description("Queries rejected by rate limiter before entering the pipeline")
                .build(),
            stream_timed_out: meter
                .u64_counter("gkg.server.stream_timed_out")
                .with_description("Query streams that exceeded the configured timeout")
                .build(),
        }
    }
}

/// Record a rate-limited query rejection.
pub fn record_rate_limit_rejected(reason: &'static str) {
    METRICS
        .rate_limit_rejected
        .add(1, &[KeyValue::new("reason", reason)]);
}

/// Record a stream timeout.
pub fn record_stream_timeout() {
    METRICS.stream_timed_out.add(1, &[]);
}

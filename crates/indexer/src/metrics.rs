//! OpenTelemetry metrics for engine observability.
//!
//! [`EngineMetrics`] holds pre-built OTel instruments for tracking throughput,
//! handler latency, worker pool utilization, and destination write performance.
//!
//! When no `MeterProvider` is configured (the default), all instruments are
//! no-ops — zero overhead in production until you opt in via
//! `opentelemetry::global::set_meter_provider()`.

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

/// OTel-recommended histogram buckets for duration in seconds.
const DURATION_BUCKETS: &[f64] = &[
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// Pre-built OpenTelemetry instruments for the ETL engine.
///
/// Created once and cloned where needed. All instruments are derived from
/// `global::meter("etl_engine")`, so they follow whatever `MeterProvider`
/// is installed at startup.
#[derive(Clone)]
pub struct EngineMetrics {
    pub(crate) messages_processed: Counter<u64>,
    pub(crate) message_duration: Histogram<f64>,
    pub(crate) handler_duration: Histogram<f64>,
    pub(crate) permit_wait_duration: Histogram<f64>,
    pub(crate) active_permits: UpDownCounter<i64>,
    pub(crate) nats_fetch_duration: Histogram<f64>,
    pub(crate) destination_write_duration: Histogram<f64>,
    pub(crate) destination_rows_written: Counter<u64>,
    pub(crate) destination_bytes_written: Counter<u64>,
}

impl EngineMetrics {
    pub fn new() -> Self {
        let meter = global::meter("etl_engine");
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        let messages_processed = meter
            .u64_counter("etl.messages.processed")
            .with_description("Total messages processed")
            .build();

        let message_duration = meter
            .f64_histogram("etl.message.duration")
            .with_unit("s")
            .with_description("End-to-end time per message through dispatch")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let handler_duration = meter
            .f64_histogram("etl.handler.duration")
            .with_unit("s")
            .with_description("Time inside each handler's handle() call")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let permit_wait_duration = meter
            .f64_histogram("etl.permit.wait.duration")
            .with_unit("s")
            .with_description("Time waiting for a worker pool permit")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let active_permits = meter
            .i64_up_down_counter("etl.permits.active")
            .with_description("Number of worker permits currently held")
            .build();

        let nats_fetch_duration = meter
            .f64_histogram("etl.nats.fetch.duration")
            .with_unit("s")
            .with_description("Time to fetch a batch of messages from NATS")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let destination_write_duration = meter
            .f64_histogram("etl.destination.write.duration")
            .with_unit("s")
            .with_description("Time to write a batch to ClickHouse")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let destination_rows_written = meter
            .u64_counter("etl.destination.rows.written")
            .with_description("Total rows written to ClickHouse")
            .build();

        let destination_bytes_written = meter
            .u64_counter("etl.destination.bytes.written")
            .with_unit("By")
            .with_description("Total bytes written to ClickHouse")
            .build();

        Self {
            messages_processed,
            message_duration,
            handler_duration,
            permit_wait_duration,
            active_permits,
            nats_fetch_duration,
            destination_write_duration,
            destination_rows_written,
            destination_bytes_written,
        }
    }
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

//! OpenTelemetry metrics for engine observability.
//!
//! [`EngineMetrics`] holds pre-built OTel instruments for tracking throughput,
//! handler latency, worker pool utilization, and destination write performance.
//!
//! When no `MeterProvider` is configured (the default), all instruments are
//! no-ops — zero overhead in production until you opt in via
//! `opentelemetry::global::set_meter_provider()`.

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

/// OTel-recommended histogram buckets for duration in seconds.
pub const DURATION_BUCKETS: &[f64] = &[
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
    pub(crate) destination_write_errors: Counter<u64>,
    pub(crate) handler_errors: Counter<u64>,
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

        let destination_write_errors = meter
            .u64_counter("etl.destination.write.errors")
            .with_description("Total failed writes to ClickHouse")
            .build();

        let handler_errors = meter
            .u64_counter("etl.handler.errors")
            .with_description("Total handler errors at the engine dispatch level")
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
            destination_write_errors,
            handler_errors,
        }
    }
}

impl EngineMetrics {
    pub(crate) fn record_message_outcome(&self, topic: &KeyValue, outcome: &'static str) {
        self.messages_processed
            .add(1, &[topic.clone(), KeyValue::new("outcome", outcome)]);
    }

    pub(crate) fn record_handler_error(&self, handler: &str, error_kind: &'static str) {
        self.handler_errors.add(
            1,
            &[
                KeyValue::new("handler", handler.to_owned()),
                KeyValue::new("error_kind", error_kind),
            ],
        );
    }

    pub(crate) fn record_handler_duration(&self, handler: &str, duration: f64) {
        self.handler_duration
            .record(duration, &[KeyValue::new("handler", handler.to_owned())]);
    }

    pub(crate) fn record_message_duration(&self, topic: &KeyValue, duration: f64) {
        self.message_duration
            .record(duration, std::slice::from_ref(topic));
    }

    pub(crate) fn record_nats_fetch_duration(&self, duration: f64, outcome: &'static str) {
        self.nats_fetch_duration
            .record(duration, &[KeyValue::new("outcome", outcome)]);
    }

    pub(crate) fn record_write_success(&self, table: &str, duration: f64, rows: u64, bytes: u64) {
        let label = KeyValue::new("table", table.to_owned());
        self.destination_write_duration
            .record(duration, std::slice::from_ref(&label));
        self.destination_rows_written
            .add(rows, std::slice::from_ref(&label));
        self.destination_bytes_written
            .add(bytes, std::slice::from_ref(&label));
    }

    pub(crate) fn record_write_error(&self, table: &str) {
        self.destination_write_errors
            .add(1, &[KeyValue::new("table", table.to_owned())]);
    }
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

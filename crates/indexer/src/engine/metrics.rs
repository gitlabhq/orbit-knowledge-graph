//! OTel instrument holders for the ETL engine, backed by the central
//! `gkg-observability` catalog.
//!
//! Names, descriptions, units, labels, and histogram buckets live in
//! `crates/gkg-observability/src/indexer/etl.rs`. This module only
//! builds instruments against the running `MeterProvider` and exposes
//! ergonomic `record_*` wrappers for the ETL engine.

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use gkg_observability::indexer::etl;

pub use gkg_observability::buckets::LATENCY as DURATION_BUCKETS;

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
        let meter = gkg_observability::meter();
        Self::with_meter(&meter)
    }

    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            messages_processed: etl::MESSAGES_PROCESSED.build_counter_u64(meter),
            message_duration: etl::MESSAGE_DURATION.build_histogram_f64(meter),
            handler_duration: etl::HANDLER_DURATION.build_histogram_f64(meter),
            permit_wait_duration: etl::PERMIT_WAIT_DURATION.build_histogram_f64(meter),
            active_permits: etl::ACTIVE_PERMITS.build_up_down_counter_i64(meter),
            nats_fetch_duration: etl::NATS_FETCH_DURATION.build_histogram_f64(meter),
            destination_write_duration: etl::DESTINATION_WRITE_DURATION.build_histogram_f64(meter),
            destination_rows_written: etl::DESTINATION_ROWS_WRITTEN.build_counter_u64(meter),
            destination_bytes_written: etl::DESTINATION_BYTES_WRITTEN.build_counter_u64(meter),
            destination_write_errors: etl::DESTINATION_WRITE_ERRORS.build_counter_u64(meter),
            handler_errors: etl::HANDLER_ERRORS.build_counter_u64(meter),
        }
    }
}

impl EngineMetrics {
    pub(crate) fn record_message_outcome(&self, topic: &KeyValue, outcome: &'static str) {
        self.messages_processed.add(
            1,
            &[topic.clone(), KeyValue::new(etl::labels::OUTCOME, outcome)],
        );
    }

    pub(crate) fn record_handler_error(&self, handler: &str, error_kind: &'static str) {
        self.handler_errors.add(
            1,
            &[
                KeyValue::new(etl::labels::HANDLER, handler.to_owned()),
                KeyValue::new(etl::labels::ERROR_KIND, error_kind),
            ],
        );
    }

    pub(crate) fn record_handler_duration(&self, handler: &str, duration: f64) {
        self.handler_duration.record(
            duration,
            &[KeyValue::new(etl::labels::HANDLER, handler.to_owned())],
        );
    }

    pub(crate) fn record_message_duration(&self, topic: &KeyValue, duration: f64) {
        self.message_duration
            .record(duration, std::slice::from_ref(topic));
    }

    pub(crate) fn record_nats_fetch_duration(&self, duration: f64, outcome: &'static str) {
        self.nats_fetch_duration
            .record(duration, &[KeyValue::new(etl::labels::OUTCOME, outcome)]);
    }

    pub(crate) fn record_write_success(&self, table: &str, duration: f64, rows: u64, bytes: u64) {
        let label = KeyValue::new(etl::labels::TABLE, table.to_owned());
        self.destination_write_duration
            .record(duration, std::slice::from_ref(&label));
        self.destination_rows_written
            .add(rows, std::slice::from_ref(&label));
        self.destination_bytes_written
            .add(bytes, std::slice::from_ref(&label));
    }

    pub(crate) fn record_write_error(&self, table: &str) {
        self.destination_write_errors
            .add(1, &[KeyValue::new(etl::labels::TABLE, table.to_owned())]);
    }
}

impl Default for EngineMetrics {
    fn default() -> Self {
        Self::new()
    }
}

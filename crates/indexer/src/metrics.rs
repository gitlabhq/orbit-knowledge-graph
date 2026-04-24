//! OTel instrument holders for the indexer, backed by the central
//! `gkg-observability` catalog.
//!
//! Names, descriptions, units, labels, and histogram buckets live in
//! `crates/gkg-observability/src/indexer/{etl,migration}.rs`. This module only
//! builds instruments against the running `MeterProvider` and exposes
//! ergonomic `record_*` wrappers for the ETL engine and migration path.
//!
//! When no `MeterProvider` is configured (the default), all instruments are
//! no-ops, so there is zero overhead in production until you opt in via
//! `opentelemetry::global::set_meter_provider()`.

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use gkg_observability::indexer::etl;
use gkg_observability::indexer::migration;

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

/// Schema-migration counter with labels `phase` and `result`.
///
/// Prometheus exposes this as `gkg_schema_migration_phase_total` after the
/// rename from the former `gkg_schema_migration_total_total` double suffix.
#[derive(Clone)]
pub struct MigrationMetrics {
    pub(crate) phase: Counter<u64>,
}

impl MigrationMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            phase: migration::PHASE.build_counter_u64(&meter),
        }
    }

    pub(crate) fn record(&self, phase: &'static str, result: &'static str) {
        self.phase.add(
            1,
            &[
                KeyValue::new(migration::labels::PHASE, phase),
                KeyValue::new(migration::labels::RESULT, result),
            ],
        );
    }
}

impl Default for MigrationMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration completion metrics: successful finishes and per-version cleanup.
///
/// The `version_band` label buckets the historical version integer into
/// `current`, `previous`, or `ancient` to cap cardinality as schema history
/// grows. Prometheus exposes these as `gkg_schema_migration_completed_total`
/// and `gkg_schema_cleanup_total` after the rename that dropped the former
/// `_total_total` double suffix.
#[derive(Clone)]
pub struct CompletionMetrics {
    pub(crate) migration_completed: Counter<u64>,
    pub(crate) cleanup: Counter<u64>,
}

impl CompletionMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            migration_completed: migration::COMPLETED.build_counter_u64(&meter),
            cleanup: migration::CLEANUP.build_counter_u64(&meter),
        }
    }

    pub(crate) fn record_migration_completed(&self) {
        self.migration_completed.add(1, &[]);
    }

    pub(crate) fn record_cleanup(&self, version: u32, current: u32, result: &'static str) {
        let band = version_band(version, current);
        self.cleanup.add(
            1,
            &[
                KeyValue::new(migration::labels::VERSION_BAND, band),
                KeyValue::new(migration::labels::RESULT, result),
            ],
        );
    }
}

impl Default for CompletionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Bucket a schema version into a bounded label value so cleanup cardinality
/// does not grow with migration history.
fn version_band(version: u32, current: u32) -> &'static str {
    match current.saturating_sub(version) {
        0 => "current",
        1 => "previous",
        _ => "ancient",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_band_buckets() {
        assert_eq!(version_band(10, 10), "current");
        assert_eq!(version_band(9, 10), "previous");
        assert_eq!(version_band(1, 10), "ancient");
        assert_eq!(version_band(11, 10), "current");
    }
}

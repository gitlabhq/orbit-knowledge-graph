use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram};

use gkg_observability::indexer::deletion;

#[derive(Clone)]
pub struct DeletionMetrics {
    pub(super) table_deletion_duration: Histogram<f64>,
    pub(super) table_deletion_errors: Counter<u64>,
}

impl DeletionMetrics {
    pub fn new() -> Self {
        let meter = gkg_observability::meter();
        Self {
            table_deletion_duration: deletion::TABLE_DELETION_DURATION.build_histogram_f64(&meter),
            table_deletion_errors: deletion::TABLE_DELETION_ERRORS.build_counter_u64(&meter),
        }
    }

    pub(super) fn record_table_deleted(&self, table: &str, duration: f64) {
        self.table_deletion_duration.record(
            duration,
            &[KeyValue::new(deletion::labels::TABLE, table.to_owned())],
        );
    }

    pub(super) fn record_table_error(&self, table: &str) {
        self.table_deletion_errors.add(
            1,
            &[KeyValue::new(deletion::labels::TABLE, table.to_owned())],
        );
    }
}

impl Default for DeletionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};

use crate::metrics::DURATION_BUCKETS;

#[derive(Clone)]
pub struct DeletionMetrics {
    pub(super) table_deletion_duration: Histogram<f64>,
    pub(super) table_deletion_errors: Counter<u64>,
}

impl DeletionMetrics {
    pub fn new() -> Self {
        let meter = global::meter("gkg_indexer_namespace_deletion");

        let table_deletion_duration = meter
            .f64_histogram("gkg.indexer.namespace_deletion.table.duration")
            .with_unit("s")
            .with_description("Duration of a single table's soft-delete INSERT-SELECT")
            .with_boundaries(DURATION_BUCKETS.to_vec())
            .build();

        let table_deletion_errors = meter
            .u64_counter("gkg.indexer.namespace_deletion.table.errors")
            .with_description("Total per-table deletion failures")
            .build();

        Self {
            table_deletion_duration,
            table_deletion_errors,
        }
    }

    pub(super) fn record_table_deleted(&self, table: &str, duration: f64) {
        self.table_deletion_duration
            .record(duration, &[KeyValue::new("table", table.to_owned())]);
    }

    pub(super) fn record_table_error(&self, table: &str) {
        self.table_deletion_errors
            .add(1, &[KeyValue::new("table", table.to_owned())]);
    }
}

impl Default for DeletionMetrics {
    fn default() -> Self {
        Self::new()
    }
}
